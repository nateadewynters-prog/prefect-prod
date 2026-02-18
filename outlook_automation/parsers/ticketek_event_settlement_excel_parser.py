import pandas as pd
import os
from prefect import task

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

@task(name="Parse Ticketek Settlement Excel", log_prints=True)
def extract_settlement_data(file_path):
    logs = []
    extracted_events = []
    
    logs.append(f"📂 Opening Excel file: {os.path.basename(file_path)}")
    
    try:
        engine = 'xlrd' if file_path.endswith('.xls') else 'openpyxl'
        logs.append(f"ℹ️  Using engine '{engine}' to read file...")
        
        df = pd.read_excel(file_path, header=None, engine=engine)
        logs.append(f"✅ File read successfully. It has {len(df)} rows.")
    except Exception as e:
        logs.append(f"❌ CRITICAL ERROR reading Excel: {str(e)}")
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
    
    logs.append("--- 🕵️ Starting Grid Scan ---")

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

    # --- STRICT SCHEMA VALIDATION ---
    if extracted_events:
        actual_schema = set(extracted_events[0].keys())
        if actual_schema != EXPECTED_SCHEMA:
            error_msg = f"Data schema mismatch! Expected exact columns: {EXPECTED_SCHEMA}, but got: {actual_schema}"
            logs.append(f"❌ {error_msg}")
            raise ValueError(error_msg)

    return extracted_events, logs