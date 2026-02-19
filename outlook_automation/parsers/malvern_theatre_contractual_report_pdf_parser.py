import pdfplumber
import re
import os
from prefect import task, get_run_logger
from shared_libs.utils import ValidationResult

def parse_currency(value_str):
    if not value_str: return 0.0
    clean = value_str.replace('£', '').replace(',', '').strip()
    try:
        return float(clean)
    except ValueError:
        return 0.0

def parse_int(value_str):
    if not value_str: return 0
    clean = value_str.replace(',', '').strip()
    try:
        return int(clean)
    except ValueError:
        return 0

# Strict Data Contract
EXPECTED_SCHEMA = {"Day", "Date", "Time", "Production", "Total Capacity", "Sold", "Reserved", "Remaining", "Reserved Value", "Total Gross"}

@task(name="Parse Malvern Contractual PDF")
def extract_contractual_report(pdf_path):
    logger = get_run_logger()
    extracted_rows = []
    
    logger.info(f"📂 Opening PDF file: {os.path.basename(pdf_path)}")
    
    calc_total_sold = 0
    calc_total_gross = 0.0

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

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text(layout=True)
                if not text: continue
                
                for line in text.split('\n'):
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
        
    validation_result = ValidationResult(
        status="UNVALIDATED",
        message="No stated totals found in PDF, manual review required.",
        metrics={
            "Calculated Tickets": calc_total_sold,
            "Calculated Gross": f"£{calc_total_gross:,.2f}"
        }
    )
        
    return extracted_rows, validation_result