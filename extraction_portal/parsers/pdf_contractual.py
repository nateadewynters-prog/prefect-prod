import pdfplumber
import re
import os

def parse_currency(value_str):
    """Converts '£5,310.15' or '309.41' to float."""
    if not value_str: return 0.0
    clean = value_str.replace('£', '').replace(',', '').strip()
    try:
        return float(clean)
    except ValueError:
        return 0.0

def parse_int(value_str):
    """Converts '5,936' to int."""
    if not value_str: return 0
    clean = value_str.replace(',', '').strip()
    try:
        return int(clean)
    except ValueError:
        return 0

def extract_contractual_report(pdf_path):
    logs = []
    extracted_rows = []
    
    logs.append(f"📂 Opening PDF file: {os.path.basename(pdf_path)}")
    
    # --- TRACKERS ---
    calc_total_sold = 0
    calc_total_gross = 0.0
    report_total_sold = 0
    report_total_gross = 0.0
    verification_found = False

    # --- REGEX PATTERNS ---
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
            logs.append(f"ℹ️  PDF has {len(pdf.pages)} pages.")
            
            for i, page in enumerate(pdf.pages):
                page_num = i + 1
                logs.append(f"📄 Scanning Page {page_num}...")
                
                text = page.extract_text(layout=True)
                if not text:
                    logs.append(f"⚠️  Page {page_num} seems empty or unreadable.")
                    continue
                
                lines = text.split('\n')
                rows_on_page = 0
                
                for line in lines:
                    # 1. Check Data Row
                    match = row_pattern.search(line)
                    if match:
                        d = match.groupdict()
                        sold = parse_int(d['sold'])
                        gross = parse_currency(d['gross'])
                        prod_name = d['prod'].strip()
                        
                        calc_total_sold += sold
                        calc_total_gross += gross

                        extracted_rows.append({
                            "Day": d['day'],
                            "Date": d['date'],
                            "Time": d['time'],
                            "Production": prod_name,
                            "Total Capacity": parse_int(d['cap']),
                            "Sold": sold,
                            "Reserved": parse_int(d['rsrv']),
                            "Remaining": parse_int(d['rem']),
                            "Reserved Value": parse_currency(d['rsrv_val']),
                            "Total Gross": gross
                        })
                        rows_on_page += 1
                        # Log the first found row just to show it's working
                        if rows_on_page == 1:
                            logs.append(f"   🔹 Found first row: '{prod_name}' (Sold: {sold})")
                        continue

                    # 2. Check Summary Line
                    match_sum = summary_pattern.search(line)
                    if match_sum:
                        logs.append(f"🏁 Found 'Summary Totals' line on Page {page_num}.")
                        s = match_sum.groupdict()
                        report_total_sold = parse_int(s['sold'])
                        report_total_gross = parse_currency(s['gross'])
                        verification_found = True
                        logs.append(f"   The Report claims Total Sold: {report_total_sold}")
                        logs.append(f"   The Report claims Total Gross: £{report_total_gross:,.2f}")

                logs.append(f"   ✅ Finished Page {page_num}. Found {rows_on_page} rows.")

        # --- VERIFICATION ---
        logs.append("--- 🔍 Starting Verification ---")
        logs.append(f"📊 My Calculation: {len(extracted_rows)} total rows found.")
        logs.append(f"📊 My Calculation: Total Sold = {calc_total_sold}")
        
        if verification_found:
            if calc_total_sold == report_total_sold:
                logs.append(f"✅ SUCCESS: Ticket Counts Match! ({calc_total_sold})")
            else:
                logs.append(f"❌ MISMATCH: I found {calc_total_sold} tickets, but report says {report_total_sold}.")
            
            if abs(calc_total_gross - report_total_gross) < 1.0:
                logs.append(f"✅ SUCCESS: Gross Value Matches! (£{calc_total_gross:,.2f})")
            else:
                logs.append(f"❌ MISMATCH: I found £{calc_total_gross:,.2f}, but report says £{report_total_gross:,.2f}.")
        else:
            logs.append("⚠️ WARNING: I never found the 'Summary' line in the PDF, so I can't double-check the totals.")

    except Exception as e:
        logs.append(f"❌ CRITICAL ERROR: {str(e)}")
        
    return extracted_rows, logs