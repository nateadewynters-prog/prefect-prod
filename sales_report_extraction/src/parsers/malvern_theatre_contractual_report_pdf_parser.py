import pdfplumber
import re
import os
from prefect import task, get_run_logger
from src.models import ValidationResult

def parse_currency(value_str):
    """
    Turn a money cell into a float, or raise ValueError if it can't be read.

    Raises rather than returning 0.0, which is what this used to do. The
    performance rows AND the '... Performances' summary line are both read
    through this function, so returning 0 on an unreadable cell zeroed both
    sides and the totals check passed on 0 == 0 - delivering wrong figures,
    with no alert.
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

# Strict Data Contract
EXPECTED_SCHEMA = {"Day", "Date", "Time", "Production", "Total Capacity", "Sold", "Reserved", "Remaining", "Reserved Value", "Total Gross"}

@task(name="Parse Malvern Contractual PDF")
def extract_contractual_report(pdf_path):
    logger = get_run_logger()
    extracted_rows = []
    
    logger.info(f"📂 Opening PDF file: {os.path.basename(pdf_path)}")
    
    calc_total_sold = 0
    calc_total_gross = 0.0
    report_total_sold = 0
    report_total_gross = 0.0
    verification_found = False

    row_pattern = re.compile(
        r"^\s*(?P<day>\w+)\s+"                 
        r"(?P<date>\d+\s+\w+\s+\d+)\s+"        
        r"(?P<time>\d{2}:\d{2})\s+"            
        r"(?P<prod>.*?)\s{2,}"                 
        r"(?P<cap>[\d,]+)\s+"                  
        r"(?P<sold>[\d,]+)\s+"                 
        r"(?P<rsrv>[\d,]+)\s+"                 
        r"(?P<rem>[\d,]+)\s+"                  
        r"(?P<rsrv_val>[\d\.,]+)\s+"           
        r"(?P<gross>[\d\.,]+)"                 
    )

    summary_pattern = re.compile(
        r"^\s*\d+\s+Performances\s+"
        r"(?P<cap>[\d,]+)\s+"
        r"(?P<sold>[\d,]+)\s+"
        r"(?P<rsrv>[\d,]+)\s+"
        r"(?P<rem>[\d,]+)\s+"
        r"(?P<rsrv_val>[£\d\.,]+)\s+"
        r"(?P<gross>[£\d\.,]+)"
    )

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text(layout=True)
                if not text: continue
                
                for line in text.split('\n'):
                    # 1. Check Data Row
                    match = row_pattern.search(line)
                    if match:
                        d = match.groupdict()
                        sold = parse_int(d['sold'])
                        gross = parse_currency(d['gross'])
                        
                        calc_total_sold += sold
                        calc_total_gross += gross

                        extracted_rows.append({
                            "Day": d['day'],
                            "Date": d['date'],
                            "Time": d['time'],
                            "Production": d['prod'].strip(),
                            "Total Capacity": parse_int(d['cap']),
                            "Sold": sold,
                            "Reserved": parse_int(d['rsrv']),
                            "Remaining": parse_int(d['rem']),
                            "Reserved Value": parse_currency(d['rsrv_val']),
                            "Total Gross": gross
                        })
                        continue
                    
                    # 2. Check Summary Line
                    match_sum = summary_pattern.search(line)
                    if match_sum:
                        logger.info(f"🏁 Found 'Summary Totals' line on Page {i+1}.")
                        s = match_sum.groupdict()
                        report_total_sold = parse_int(s['sold'])
                        report_total_gross = parse_currency(s['gross'])
                        verification_found = True

        # --- STRICT SCHEMA VALIDATION ---
        if extracted_rows:
            actual_schema = set(extracted_rows[0].keys())
            if actual_schema != EXPECTED_SCHEMA:
                error_msg = f"Data schema mismatch! Expected exact columns: {EXPECTED_SCHEMA}, but got: {actual_schema}"
                logger.error(f"❌ {error_msg}")
                raise ValueError(error_msg)
            else:
                logger.info(f"✅ Schema validation passed. Extracted {len(extracted_rows)} production rows.")

    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {str(e)}")
        raise e
        
    # --- DYNAMIC VALIDATION RESULT ---
    metrics = {
        "Calculated Tickets": calc_total_sold,
        "Calculated Gross": f"£{calc_total_gross:,.2f}"
    }

    if verification_found:
        metrics["Reported Tickets"] = report_total_sold
        metrics["Reported Gross"] = f"£{report_total_gross:,.2f}"

        tickets_match = (calc_total_sold == report_total_sold)
        gross_matches = (abs(calc_total_gross - report_total_gross) < 1.0)

        if tickets_match and gross_matches:
            status = "PASSED"
            message = "Calculated totals successfully match the report summary."
            logger.info(f"✅ {message}")
        else:
            status = "FAILED"
            message = f"Mismatch! Calculated (Tickets: {calc_total_sold}, Gross: £{calc_total_gross:,.2f}) vs Reported (Tickets: {report_total_sold}, Gross: £{report_total_gross:,.2f})"
            logger.error(f"❌ {message}")
    else:
        status = "UNVALIDATED"
        message = "No stated totals found in PDF, manual review required."
        logger.warning(f"⚠️ {message}")

    validation_result = ValidationResult(
        status=status,
        message=message,
        metrics=metrics
    )
        
    return extracted_rows, validation_result
