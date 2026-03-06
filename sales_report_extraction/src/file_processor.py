import os
import json
import shutil
import importlib
import pandas as pd
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser
from prefect import get_run_logger

class ProcessingEngine:
    def __init__(self, global_config: dict, config_path: str):
        self.base_dir = global_config['base_dir'] 
        self.dirs = global_config['data_dirs'] 
        self.config_path = config_path 
        self._ensure_directories() 

    def _ensure_directories(self):
        for relative_path in self.dirs.values(): 
            os.makedirs(os.path.join(self.base_dir, relative_path), exist_ok=True) 

    def generate_filename(self, metadata: dict, date_str: str, ext: str) -> str:
        dt = date_parser.parse(date_str).astimezone(timezone.utc) - timedelta(days=1) 
        fmt_date = dt.strftime("%d_%m_%Y") 
        name = f"{metadata['show_name']}.{metadata['venue_name']}_{metadata['show_id']}_{metadata['venue_id']}_{metadata['document_id']}_{fmt_date}{ext}" 
        return name.replace(" ", "-").replace("/", "-") 

    def process_file(self, temp_path: str, rule: dict) -> tuple:
        """Invokes the parser, handles lookups, saves CSV, and moves to archive."""
        logger = get_run_logger()
        proc_config = rule['processing'] 
        
        # Dynamically load the parser
        logger.info(f"🔄 Dynamically loading parser: {proc_config['parser_module']}.{proc_config['parser_function']}")
        parser_module = importlib.import_module(proc_config['parser_module']) 
        parser_func = getattr(parser_module, proc_config['parser_function']) 
        
        parsed_data, validation_result = parser_func(temp_path) 
        
        # Log validation failures before raising
        if validation_result.status == "FAILED" or not parsed_data: 
            logger.error(f"❌ Parser validation failed for {temp_path}: {validation_result.message}")
            raise ValueError(f"Validation Failed: {validation_result.message}") 

        df = pd.DataFrame(parsed_data) 

        if proc_config.get('needs_lookup'): 
            meta = rule['metadata'] 
            lookup_file = os.path.join(self.base_dir, self.dirs['lookups'], f"{meta['show_id']}_{meta['venue_id']}_event_dates.csv") 
            
            # Log exact missing lookup path
            if not os.path.exists(lookup_file): 
                logger.error(f"❌ Missing required lookup file at absolute path: {os.path.abspath(lookup_file)}")
                raise ValueError(f"Missing lookup file: {lookup_file}") 
            
            lookup_df = pd.read_csv(lookup_file) 
            df['Performance/Event Code'] = df['Performance/Event Code'].astype(str).str.strip() 
            lookup_df['Show Code'] = lookup_df['Show Code'].astype(str).str.strip() 
            
            df = df.merge(lookup_df[['Show Code', 'Performance Date Time']], left_on='Performance/Event Code', right_on='Show Code', how='left') 
            unmapped = df[df['Performance Date Time'].isna()]['Performance/Event Code'].unique() 
            
            # Log the specific unmapped codes
            if len(unmapped) > 0: 
                unmapped_str = ', '.join(map(str, unmapped[:5]))
                logger.error(f"❌ Lookup Merge Failed. Unmapped codes: {{{unmapped_str}}}")
                raise ValueError(f"Lookup Merge Failed: Unmapped codes found {{{unmapped_str}}}") 

        # Save outputs with volume logging
        filename = os.path.basename(temp_path) 
        csv_path = os.path.join(self.base_dir, self.dirs['processed'], filename.replace(os.path.splitext(filename)[1], '.csv')) 
        logger.info(f"💾 Saving {len(df)} rows to processed CSV: {csv_path}")
        df.to_csv(csv_path, index=False) 
        
        # Log Medallion movement to archive
        archive_path = os.path.join(self.base_dir, self.dirs['archive'], filename) 
        logger.info(f"📦 Archiving raw file from {temp_path} -> {archive_path}")
        shutil.move(temp_path, archive_path) 
        
        return df, validation_result, csv_path 

    def handle_failure(self, temp_path: str):
        logger = get_run_logger()
        if os.path.exists(temp_path): 
            filename = os.path.basename(temp_path) 
            failed_path = os.path.join(self.base_dir, self.dirs['failed'], filename) 
            logger.warning(f"⚠️ Moving failed file to quarantine: {failed_path}")
            shutil.move(temp_path, failed_path) 

    def update_config_state(self, successful_runs: list):
        if not successful_runs: return 
        logger = get_run_logger()
        max_dates = {} 
        
        for r_name, date_str in successful_runs: 
            dt = date_parser.parse(date_str).astimezone(timezone.utc) 
            if r_name not in max_dates or dt > max_dates[r_name]: 
                max_dates[r_name] = dt 
                
        with open(self.config_path, 'r') as f: 
            current_config = json.load(f) 
            
        updated = False 
        for rule in current_config['rules']: 
            r_name = rule['rule_name'] 
            if r_name in max_dates: 
                new_date_str = max_dates[r_name].strftime('%Y-%m-%d') 
                current_date_str = rule.get('backfill_since', '1900-01-01') 
                
                if new_date_str > current_date_str: 
                    logger.info(f"📈 Advancing state for '{r_name}': {current_date_str} -> {new_date_str}")
                    rule['backfill_since'] = new_date_str 
                    updated = True 
                    
        if updated: 
            with open(self.config_path, 'w') as f: 
                json.dump(current_config, f, indent=4)