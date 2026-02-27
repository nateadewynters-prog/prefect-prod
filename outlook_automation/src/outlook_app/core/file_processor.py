# src/outlook_app/core/file_processor.py
import os
import json
import shutil
import importlib
import pandas as pd
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser

class ProcessingEngine:
    def __init__(self, global_config: dict, config_path: str):
        self.base_dir = global_config['base_dir']
        self.dirs = global_config['data_dirs']
        self.history_file = os.path.join(self.base_dir, global_config['history_file'])
        self.config_path = config_path
        self._ensure_directories()

    def _ensure_directories(self):
        for relative_path in self.dirs.values():
            os.makedirs(os.path.join(self.base_dir, relative_path), exist_ok=True)
        if not os.path.exists(self.history_file):
            with open(self.history_file, 'w') as f: pass

    def load_processed_ids(self) -> set:
        processed = set()
        with open(self.history_file, 'r') as f:
            for line in f:
                parts = line.strip().split(',')
                processed.add(parts[1] if len(parts) >= 2 else parts[0])
        return processed

    def save_processed_id(self, msg_id: str, rule_name: str):
        timestamp = datetime.now(timezone.utc).isoformat()
        with open(self.history_file, 'a') as f:
            f.write(f"{timestamp},{msg_id},{rule_name}\n")

    def generate_filename(self, metadata: dict, date_str: str, ext: str) -> str:
        dt = date_parser.parse(date_str).astimezone(timezone.utc) - timedelta(days=1)
        fmt_date = dt.strftime("%d_%m_%Y")
        name = f"{metadata['show_name']}.{metadata['venue_name']}.{metadata['show_id']}_{metadata['venue_id']}_{metadata['document_id']}_{fmt_date}{ext}"
        return name.replace(" ", "-").replace("/", "-")

    def process_file(self, temp_path: str, rule: dict) -> tuple:
        """Invokes the parser, handles lookups, saves CSV, and moves to archive."""
        proc_config = rule['processing']
        
        # Dynamically load the parser from the new src layout
        parser_module = importlib.import_module(proc_config['parser_module'])
        parser_func = getattr(parser_module, proc_config['parser_function'])
        
        parsed_data, validation_result = parser_func(temp_path)
        if validation_result.status == "FAILED" or not parsed_data:
            raise ValueError(f"Validation Failed: {validation_result.message}")

        df = pd.DataFrame(parsed_data)

        if proc_config.get('needs_lookup'):
            meta = rule['metadata']
            lookup_file = os.path.join(self.base_dir, self.dirs['lookups'], f"{meta['show_id']}_{meta['venue_id']}_event_dates.csv")
            if not os.path.exists(lookup_file):
                raise ValueError(f"Missing lookup file: {lookup_file}")
            
            lookup_df = pd.read_csv(lookup_file)
            df['Performance/Event Code'] = df['Performance/Event Code'].astype(str).str.strip()
            lookup_df['Show Code'] = lookup_df['Show Code'].astype(str).str.strip()
            
            df = df.merge(lookup_df[['Show Code', 'Performance Date Time']], left_on='Performance/Event Code', right_on='Show Code', how='left')
            unmapped = df[df['Performance Date Time'].isna()]['Performance/Event Code'].unique()
            if len(unmapped) > 0:
                raise ValueError(f"Lookup Merge Failed: Unmapped codes found {{{', '.join(map(str, unmapped[:5]))}}}")

        # Save outputs
        filename = os.path.basename(temp_path)
        csv_path = os.path.join(self.base_dir, self.dirs['processed'], filename.replace(os.path.splitext(filename)[1], '.csv'))
        df.to_csv(csv_path, index=False)
        
        archive_path = os.path.join(self.base_dir, self.dirs['archive'], filename)
        shutil.move(temp_path, archive_path)
        
        return df, validation_result

    def handle_failure(self, temp_path: str):
        if os.path.exists(temp_path):
            filename = os.path.basename(temp_path)
            shutil.move(temp_path, os.path.join(self.base_dir, self.dirs['failed'], filename))

    def update_config_state(self, successful_runs: list):
        if not successful_runs: return
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
                    rule['backfill_since'] = new_date_str
                    updated = True
                    
        if updated:
            with open(self.config_path, 'w') as f:
                json.dump(current_config, f, indent=4)
