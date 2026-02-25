import pandas as pd
import os
from prefect import task, get_run_logger
from shared_libs.utils import ValidationResult

def clean_currency(value):
    if pd.isna(value) or value == '': return 0.0
    if isinstance(value, (int, float)): return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace('$', '').replace('£', '').replace(',', '').replace(' ', ''))
        except: return 0.0
    return 0.0

def clean_int(value):
    if pd.isna(value) or value == '': return 0
    if isinstance(value, (int, float)): return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.replace(',', '').replace(' ', '')))
        except: return 0
    return 0

# Strict Data Contract
EXPECTED_SCHEMA = {"Performance/Event Code", "Comps", "Paid Tickets", "Total Tickets", "Total Gross"}

@task(name="Parse Ticketek Settlement Excel")
def extract_settlement_data(file_path):
    logger = get_run_logger()
    extracted_events = []
    
    logger.info(f"📂 Opening Excel file: {os.path.basename(file_path)}")
    
    try:
        engine = 'xlrd' if file_path.endswith('.xls') else 'openpyxl'
        logger.info(f"ℹ️  Using engine '{engine}' to read file...")
        
        df = pd.read_excel(file_path, header=None, engine=engine)
        logger.info(f"✅ File read successfully. It has {len(df)} rows.")
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR reading Excel: {str(e)}")
        raise ValueError(f"Failed to read Excel file: {e}")

    grid_data = df.values.tolist()
    
    current_event_code = None
    total_col_index = -1
    inside_event_block = False
    
    paid_tickets = 0
    comp_tickets = 0
    total_gross = 0.0
    
    grand_total_tickets = 0
    grand_total_gross = 0.0
    
    footer_span_tickets = None
    footer_span_gross = None
    
    logger.info("--- 🕵️ Starting Grid Scan ---")

    for i, row in enumerate(grid_data):
        cleaned_row = [str(x).strip() if not pd.isna(x) else None for x in row]
        first_cell = cleaned_row[0] if len(cleaned_row) > 0 else None

        # 1. Detect Start of Block: "Summary Totals"
        if first_cell == "Summary Totals":
            if inside_event_block and current_event_code and "Event Span Total" not in current_event_code:
                extracted_events.append({
                    "Performance/Event Code": current_event_code,
                    "Comps": comp_tickets,
                    "Paid Tickets": paid_tickets,
                    "Total Tickets": paid_tickets + comp_tickets,
                    "Total Gross": total_gross
                })
                grand_total_tickets += (paid_tickets + comp_tickets)
                grand_total_gross += total_gross

            inside_event_block = True
            paid_tickets = 0
            comp_tickets = 0
            total_gross = 0.0
            
            current_event_code = "UNKNOWN_EVENT"
            for offset in range(1, 6):
                if i - offset >= 0:
                    potential_code = grid_data[i-offset][0]
                    if not pd.isna(potential_code) and str(potential_code).strip() != "":
                        current_event_code = str(potential_code).strip()
                        break
            
            if "Event Span Total" in current_event_code:
                inside_event_block = False
                current_event_code = None
                continue

            try:
                total_col_index = cleaned_row.index("Total")
            except ValueError:
                total_col_index = -1
            continue

        # 2. Extract Data
        if inside_event_block and total_col_index != -1:
            if first_cell == "Value Total":
                if len(row) > total_col_index:
                    paid_tickets = clean_int(row[total_col_index])
                if len(row) > total_col_index + 1:
                    total_gross = clean_currency(row[total_col_index+1])

            elif first_cell == "Comp Total":
                if len(row) > total_col_index:
                    comp_tickets = clean_int(row[total_col_index])

            # 3. Detect End of Block
            elif first_cell and current_event_code and first_cell.startswith(current_event_code) and "Total" in first_cell:
                calc_total = paid_tickets + comp_tickets
                extracted_events.append({
                    "Performance/Event Code": current_event_code,
                    "Comps": comp_tickets,
                    "Paid Tickets": paid_tickets,
                    "Total Tickets": calc_total,
                    "Total Gross": total_gross
                })
                
                grand_total_tickets += calc_total
                grand_total_gross += total_gross
                
                inside_event_block = False
                current_event_code = None

        # 4. Global Verification (The Footer)
        if first_cell == "Event Span Total":
            logger.info("--- 🔍 Global Verification (Footer) ---")
            footer_span_tickets = clean_int(row[total_col_index]) if len(row) > total_col_index else 0
            footer_span_gross = clean_currency(row[total_col_index+1]) if len(row) > total_col_index+1 else 0.0

    # --- STRICT SCHEMA VALIDATION ---
    if extracted_events:
        actual_schema = set(extracted_events[0].keys())
        if actual_schema != EXPECTED_SCHEMA:
            error_msg = f"Data schema mismatch! Expected exact columns: {EXPECTED_SCHEMA}, but got: {actual_schema}"
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        else:
            logger.info(f"✅ Schema validation passed. Found {len(extracted_events)} event rows.")

    # --- DYNAMIC VALIDATION RESULT ---
    metrics = {
        "Extracted Tickets": grand_total_tickets,
        "Extracted Gross": f"${grand_total_gross:,.2f}"
    }

    if footer_span_tickets is not None and footer_span_gross is not None:
        metrics["Reported Tickets"] = footer_span_tickets
        metrics["Reported Gross"] = f"${footer_span_gross:,.2f}"
        
        tickets_match = (grand_total_tickets == footer_span_tickets)
        gross_matches = (abs(grand_total_gross - footer_span_gross) < 1.0)
        
        if tickets_match and gross_matches:
            status = "PASSED"
            message = "✅ Calculated totals successfully match the 'Event Span Total' footer."
            logger.info(message)
        else:
            status = "FAILED"
            message = f"Mismatch! Calculated (Tickets: {grand_total_tickets}, Gross: ${grand_total_gross:,.2f}) vs Reported (Tickets: {footer_span_tickets}, Gross: ${footer_span_gross:,.2f})"
            logger.error(f"❌ {message}")
    else:
        status = "UNVALIDATED"
        message = "⚠️ No 'Event Span Total' footer found in Excel, manual review required."
        logger.warning(message)

    validation_result = ValidationResult(
        status=status,
        message=message,
        metrics=metrics
    )

    return extracted_events, validation_result