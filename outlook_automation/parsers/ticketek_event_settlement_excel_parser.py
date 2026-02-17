import pandas as pd
import os

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

def extract_settlement_data(file_path):
    logs = []
    extracted_events = []
    
    logs.append(f"📂 Opening Excel file: {os.path.basename(file_path)}")
    
    try:
        engine = 'xlrd' if file_path.endswith('.xls') else 'openpyxl'
        logs.append(f"ℹ️  Using engine '{engine}' to read file...")
        
        # Read header=None so we see the raw grid
        df = pd.read_excel(file_path, header=None, engine=engine)
        logs.append(f"✅ File read successfully. It has {len(df)} rows.")
    except Exception as e:
        logs.append(f"❌ CRITICAL ERROR reading Excel: {str(e)}")
        return [], logs

    grid_data = df.values.tolist()
    
    # State variables
    current_event_code = None
    total_col_index = -1
    inside_event_block = False
    
    # Data holders
    paid_tickets = 0
    comp_tickets = 0
    total_gross = 0.0
    
    # Global verification trackers
    grand_total_tickets = 0
    grand_total_gross = 0.0
    
    logs.append("--- 🕵️ Starting Grid Scan ---")

    for i, row in enumerate(grid_data):
        # Helper: Get safe string of first cell
        cleaned_row = [str(x).strip() if not pd.isna(x) else None for x in row]
        first_cell = cleaned_row[0] if len(cleaned_row) > 0 else None

        # 1. Detect Start of Block: "Summary Totals"
        if first_cell == "Summary Totals":
            # If we were already inside a block, we need to save the previous one first
            if inside_event_block and current_event_code and "Event Span Total" not in current_event_code:
                logs.append(f"   💾 Saving previous event: {current_event_code} (Total: {paid_tickets + comp_tickets})")
                extracted_events.append({
                    "Performance/Event Code": current_event_code,
                    "Comps": comp_tickets,
                    "Paid Tickets": paid_tickets,
                    "Total Tickets": paid_tickets + comp_tickets,
                    "Total Gross": total_gross
                })
                grand_total_tickets += (paid_tickets + comp_tickets)
                grand_total_gross += total_gross

            # --- Start New Block ---
            inside_event_block = True
            paid_tickets = 0
            comp_tickets = 0
            total_gross = 0.0
            
            # Search Upwards for Code (Look back 5 rows)
            current_event_code = "UNKNOWN_EVENT"
            found_code = False
            for offset in range(1, 6):
                if i - offset >= 0:
                    potential_code = grid_data[i-offset][0]
                    if not pd.isna(potential_code) and str(potential_code).strip() != "":
                        current_event_code = str(potential_code).strip()
                        found_code = True
                        break
            
            if found_code:
                logs.append(f"📍 Row {i}: Found 'Summary Totals'. Linked to Event Code: '{current_event_code}'")
            else:
                logs.append(f"⚠️ Row {i}: Found 'Summary Totals' but could not find an Event Code above it.")

            # Stop if we hit the grand total footer
            if "Event Span Total" in current_event_code:
                inside_event_block = False
                current_event_code = None
                continue

            # Find 'Total' column
            try:
                total_col_index = cleaned_row.index("Total")
            except ValueError:
                total_col_index = -1
                logs.append(f"⚠️ Row {i}: Could not find a column named 'Total' in this row!")
            continue

        # 2. Extract Data (Inside a block)
        if inside_event_block and total_col_index != -1:
            if first_cell == "Value Total":
                if len(row) > total_col_index:
                    paid_tickets = clean_int(row[total_col_index])
                if len(row) > total_col_index + 1:
                    total_gross = clean_currency(row[total_col_index+1])
                # logs.append(f"      -> Found Value Total: {paid_tickets} tickets, ${total_gross}")

            elif first_cell == "Comp Total":
                if len(row) > total_col_index:
                    comp_tickets = clean_int(row[total_col_index])
                # logs.append(f"      -> Found Comp Total: {comp_tickets} tickets")

            # 3. Detect End of Block (Row starts with Code + "Total")
            elif first_cell and current_event_code and first_cell.startswith(current_event_code) and "Total" in first_cell:
                calc_total = paid_tickets + comp_tickets
                extracted_events.append({
                    "Performance/Event Code": current_event_code,
                    "Comps": comp_tickets,
                    "Paid Tickets": paid_tickets,
                    "Total Tickets": calc_total,
                    "Total Gross": total_gross
                })
                logs.append(f"   🏁 Block Finished. Extracted {calc_total} tickets for {current_event_code}")
                
                grand_total_tickets += calc_total
                grand_total_gross += total_gross
                
                inside_event_block = False
                current_event_code = None

        # 4. Global Verification (The Footer)
        if first_cell == "Event Span Total":
            logs.append("--- 🔍 Global Verification (Footer) ---")
            span_tickets = clean_int(row[total_col_index]) if len(row) > total_col_index else 0
            span_gross = clean_currency(row[total_col_index+1]) if len(row) > total_col_index+1 else 0.0
            
            logs.append(f"📊 My Calculation: {grand_total_tickets} Total Tickets found.")
            logs.append(f"📊 Report Footer:  {span_tickets} Total Tickets listed.")
            
            if span_tickets == grand_total_tickets:
                logs.append(f"✅ SUCCESS: Ticket Counts Match!")
            else:
                logs.append(f"❌ MISMATCH: Missing {span_tickets - grand_total_tickets} tickets somewhere.")
                
            if abs(span_gross - grand_total_gross) < 1.0: 
                logs.append(f"✅ SUCCESS: Gross Amounts Match! (${grand_total_gross:,.2f})")
            else:
                logs.append(f"❌ MISMATCH: Gross amount differs by ${span_gross - grand_total_gross:,.2f}")

    return extracted_events, logs