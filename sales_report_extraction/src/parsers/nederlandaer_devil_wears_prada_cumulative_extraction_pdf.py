import re
import os
import pdfplumber
from prefect import task, get_run_logger
from src.models import ValidationResult

def parse_currency(value_str):
    """
    Turn a money cell into a float, or raise ValueError if it can't be read.

    Raises rather than returning 0.0, which is what this used to do. The
    Dominion row AND the Grand Totals row are both read through this function,
    so returning 0 on an unreadable cell zeroed both sides and the Gross check
    passed on 0 == 0 - delivering wrong figures, with no alert.

    This also drops the old "more than one dot" fallback, which turned an
    ambiguous "1.234.56" into 1.23. Multiple dots now raise: pdfplumber
    producing them means two columns have been merged and every figure on that
    row is suspect.
    """
    if not value_str:
        return 0.0
    if isinstance(value_str, bool):
        # bool is a subclass of int, so it would otherwise become 1.0/0.0.
        raise ValueError(f"Expected a number in a money column, got a boolean: {value_str!r}")
    if isinstance(value_str, (int, float)):
        return float(value_str)

    # Strip currency symbols and every space (including the non-breaking kind),
    # leaving only digits and separators.
    clean = re.sub(r'\s+', '', str(value_str).replace('£', ''))
    if clean in ('', '-', '.', '-.'):
        return 0.0                                          # a dash means nothing

    # Work out what the separators mean before touching them. Guessing wrong is
    # a 100x error: "1,234.56" is comma-thousands, but "1234,56" is a European
    # decimal comma and blanket-stripping it turns 1234.56 into 123456.
    if ',' in clean and '.' in clean:
        if clean.rfind(',') > clean.rfind('.'):
            clean = clean.replace('.', '').replace(',', '.')  # 1.234,56
        else:
            clean = clean.replace(',', '')                    # 1,234.56
    elif re.search(r',\d{2}$', clean):
        clean = clean.replace(',', '.')                       # 1234,56
    else:
        clean = clean.replace(',', '')                        # 1,080

    try:
        return float(clean)
    except ValueError:
        raise ValueError(
            f"Could not read a number from {value_str!r}. The source format has "
            f"probably changed - check the file before trusting its figures."
        ) from None

def parse_int(value_str):
    """
    Ticket counts are whole numbers.

    Goes through parse_currency so '1,080' and '1080.0' both work, and so an
    unreadable cell raises here too rather than silently becoming 0.
    """
    return int(round(parse_currency(value_str)))

@task(name="Parse Nederlander Prada Cumulative PDF")
def nederlandaer_devil_wears_prada_cumulative_extraction_pdf(file_path: str) -> tuple:
    logger = get_run_logger()
    logger.info(f"📂 Opening PDF file: {os.path.basename(file_path)}")
    
    extracted_rows = []
    
    try:
        with pdfplumber.open(file_path) as pdf:
            page = pdf.pages[0]
            table_data = page.extract_table()
            
            if not table_data:
                error_msg = "No table structure detected in PDF."
                logger.error(f"❌ {error_msg}")
                raise ValueError(error_msg)

            for row in table_data:
                if row and row[0] == "Dominion Theatre":
                    data = {
                        "Venue": row[0],
                        "Event Template": row[1],
                        "Tickets": parse_int(row[2]),
                        "Comps": parse_int(row[3]),
                        "Gross": parse_currency(row[4]),
                        "VAT": parse_currency(row[5]),
                        "Net": parse_currency(row[9]),
                        "Partner Gross": parse_currency(row[11]),
                        "Performance/Event Code": "CUMULATIVE"
                    }
                    extracted_rows.append(data)
                    logger.info(f"📊 Found Venue Data: {data['Tickets']} Tickets | {data['Comps']} Comps | £{data['Gross']:,.2f} Gross")
                    break 

        if not extracted_rows:
            error_msg = "No valid data extracted from PDF. 'Dominion Theatre' row not found."
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)

        status = "PASSED"
        message = "Devil Wears Prada PDF parsed and math verified successfully."
        
        grand_total_row = next((r for r in table_data if r and r[0] == "Grand Totals"), None)
        
        if grand_total_row:
            rpt_gross = parse_currency(grand_total_row[4])
            if abs(extracted_rows[0]["Gross"] - rpt_gross) > 0.01:
                error_msg = f"Mismatch with Grand Totals (Venue: £{extracted_rows[0]['Gross']} vs Grand: £{rpt_gross})"
                logger.error(f"❌ VERIFICATION FAILED: {error_msg}")
                raise ValueError(error_msg)
            else:
                logger.info("✅ VERIFICATION PASSED: Venue totals match Grand Totals perfectly.")
        else:
            error_msg = "Grand Totals row not found. Cannot verify data integrity."
            logger.error(f"❌ VERIFICATION FAILED: {error_msg}")
            raise ValueError(error_msg)

    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {str(e)}")
        raise e

    metrics = {
        "Extracted Rows": len(extracted_rows),
        "Total Tickets": extracted_rows[0]["Tickets"],
        "Total Comps": extracted_rows[0]["Comps"],
        "Total Gross": extracted_rows[0]["Gross"]
    }

    logger.info(f"✅ {message}")
    validation_result = ValidationResult(
        status=status,
        message=message,
        metrics=metrics
    )

    return extracted_rows, validation_result
