import os
import pdfplumber
from prefect import task, get_run_logger
from src.models import ValidationResult

# Shared money/count parsing: raises on a cell it cannot read instead of
# returning 0, so a supplier format change can no longer zero both the
# extracted figures and the totals row they are checked against.
# See src/parsers/_common.py.
#
# Note this drops the old "more than one dot" fallback, which turned an
# ambiguous "1.234.56" into 1.23 - a silently wrong figure. Multiple dots with
# no comma now raise, because pdfplumber producing them means two columns have
# been merged and every figure on that row is suspect.
from src.parsers._common import to_float as parse_currency, to_int as parse_int

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
