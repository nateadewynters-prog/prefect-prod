import os
import zipfile
import pandas as pd
from prefect import task, get_run_logger
from src.models import ValidationResult

def clean_to_float(value):
    """Helper to handle strings, commas, and currency symbols."""
    if pd.isna(value):
        return 0.0
    clean_str = str(value).replace(',', '').replace('$', '').strip()
    try:
        return float(clean_str)
    except ValueError:
        return 0.0

def clean_to_int(value):
    """Helper to handle strings, commas, and convert to whole integers."""
    if pd.isna(value):
        return 0
    clean_str = str(value).replace(',', '').replace('$', '').strip()
    try:
        # Cast to float first to safely handle strings like "100.0", then to int
        return int(float(clean_str))
    except ValueError:
        return 0

@task(name="Parse GMG HK JCS ZIP/XLSX")
def extract_gmg_jcs_data(file_path):
    logger = get_run_logger()
    logger.info(f"📂 Processing ZIP file: {os.path.basename(file_path)}")
    
    extracted_xlsx_path = None
    
    # --- 1. Unzip Logic ---
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            xlsx_names = [f for f in z.namelist() if f.endswith('.xlsx')]
            if not xlsx_names:
                raise ValueError("No .xlsx file found inside the ZIP archive.")
            
            # Extract to the same directory as the zip
            extract_dir = os.path.dirname(file_path)
            z.extract(xlsx_names[0], path=extract_dir)
            extracted_xlsx_path = os.path.join(extract_dir, xlsx_names[0])
            logger.info(f"✅ Extracted XLSX: {xlsx_names[0]}")
    except Exception as e:
        raise ValueError(f"Failed to extract ZIP: {e}")

    # --- 2. Parsing & Extraction Logic ---
    try:
        # Read first column to find where the table starts and ends
        temp_df = pd.read_excel(extracted_xlsx_path, sheet_name='Overview', usecols=[0], header=None)
        header_idx_matches = temp_df[temp_df[0] == 'Performance Date'].index
        
        if len(header_idx_matches) == 0:
            raise ValueError("Could not find 'Performance Date' header in 'Overview' sheet.")
            
        header_idx = header_idx_matches[0]
        df = pd.read_excel(extracted_xlsx_path, sheet_name='Overview', skiprows=header_idx)
        
        # Locate the "Grand Total" row
        grand_total_mask = df['Performance Date'].astype(str).str.strip() == 'Grand Total'
        
        if not grand_total_mask.any():
            logger.warning("⚠️ No 'Grand Total' row found. Cannot validate math.")
            status = "UNVALIDATED"
            message = "⚠️ Extracted data, but no 'Grand Total' row found for mathematical validation."
            data_df = df.dropna(subset=['Performance Date']).copy()
            metrics = {}
        else:
            grand_total_idx = df[grand_total_mask].index[0]
            data_df = df.iloc[:grand_total_idx].copy()
            totals_row = df[grand_total_mask].iloc[0]

            # --- 3. Clean Data & Dynamic Validation Logic ---
            
            # Categorize columns based on their expected data type
            int_cols = [
                'Seat Capacity', 'Ticket Sold', 'Block Seats', 
                'Wheelchair/ Consignment Reservation', 'Seat Available', 
                'Paper Ticket'
            ]
            float_cols = [
                'Gross Sales($)', 'Commission($)', 'Net Sales($)', 
                'SQR E-Wallet', 'SQR JPG', 'DQR'
            ]
            
            artifact_metrics_cols = ['Ticket Sold', 'Gross Sales($)'] 
            
            all_passed = True
            failed_cols = []
            metrics = {}

            # Pre-clean the dataframe so the final returned records are formatted correctly
            for col in int_cols:
                if col in data_df.columns:
                    data_df[col] = data_df[col].apply(clean_to_int)
                    
            for col in float_cols:
                if col in data_df.columns:
                    data_df[col] = data_df[col].apply(clean_to_float)
            
            # Perform mathematical validation
            for col in int_cols + float_cols:
                if col in data_df.columns:
                    calc_sum = data_df[col].sum() # Data is already cleaned above
                    
                    if col in int_cols:
                        rep_tot = clean_to_int(totals_row[col])
                        matches = (calc_sum == rep_tot)
                    else:
                        rep_tot = clean_to_float(totals_row[col])
                        matches = (round(calc_sum, 2) == round(rep_tot, 2))
                        
                    if not matches:
                        all_passed = False
                        failed_cols.append(f"{col} (Calc: {calc_sum}, Rep: {rep_tot})")
                        
                    if col in artifact_metrics_cols:
                        metrics[f"Calc {col}"] = calc_sum
                        metrics[f"Rep {col}"] = rep_tot

            if all_passed:
                status = "PASSED"
                message = "✅ All calculated column sums perfectly match the 'Grand Total' row."
            else:
                status = "FAILED"
                message = f"❌ Mathematical mismatch in columns: {', '.join(failed_cols)}"
                logger.error(message)

        # --- 4. Return Medallion Contract ---
        validation_result = ValidationResult(
            status=status,
            message=message,
            metrics=metrics
        )
        
        return data_df.to_dict(orient='records'), validation_result

    finally:
        # --- 5. Cleanup ---
        # Delete the unzipped .xlsx so it doesn't get left behind in the inbox
        if extracted_xlsx_path and os.path.exists(extracted_xlsx_path):
            os.remove(extracted_xlsx_path)
            logger.info("🧹 Cleaned up temporary XLSX file.")