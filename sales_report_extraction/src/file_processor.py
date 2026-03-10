import os
import shutil
import importlib
import pandas as pd
from prefect import get_run_logger
from src.models import ValidationResult
from src.naming import generate_standard_filename, get_medallion_folders
from src.mapping import apply_event_lookups

class ProcessingEngine:
    def __init__(self, global_config: dict, config_path: str):
        self.base_dir = global_config['base_dir'] 
        self.dirs = global_config['data_dirs'] 
        self.config_path = config_path 
        self._ensure_directories() 

    def _ensure_directories(self):
        """Creates top-level directories if they do not exist."""
        for relative_path in self.dirs.values(): 
            os.makedirs(os.path.join(self.base_dir, relative_path), exist_ok=True) 

    def generate_filename(self, metadata: dict, date_str: str, ext: str) -> str:
        """Wrapper for standard naming utility."""
        return generate_standard_filename(metadata, date_str, ext)

    def process_file(self, temp_path: str, rule: dict) -> tuple:
        """Main orchestrator: loads parsers, manages flow, and saves files."""
        logger = get_run_logger()
        proc_config = rule['processing'] 
        filename = os.path.basename(temp_path)
        
        # 1. Setup nested folders (Show/Venue)
        proc_dir, arch_dir = get_medallion_folders(self.base_dir, self.dirs, rule['metadata'])
        os.makedirs(proc_dir, exist_ok=True)
        os.makedirs(arch_dir, exist_ok=True)

        # 2. Handle Passthrough Files (No parsing needed)
        if proc_config.get('passthrough_only', False):
            logger.info(f"⏩ Passthrough mode: Archiving {filename} and sending directly to SFTP.")
            
            # 🚀 THE FIX: Use arch_dir instead of proc_dir
            final_path = os.path.join(arch_dir, filename) 
            shutil.move(temp_path, final_path)
            
            val_res = ValidationResult(
                status="PASSED", message="File passed through and archived.", 
                metrics={"action": "passthrough"}
            )
            
            # We still return final_path so main.py knows where to find it for the SFTP upload
            return None, val_res, final_path

        # 3. Dynamic Parsing
        logger.info(f"🔄 Loading parser: {proc_config['parser_module']}.{proc_config['parser_function']}")
        parser_module = importlib.import_module(proc_config['parser_module']) 
        parser_func = getattr(parser_module, proc_config['parser_function']) 
        
        parsed_data, validation_result = parser_func(temp_path) 
        if validation_result.status == "FAILED" or not parsed_data: 
            raise ValueError(f"Validation Failed: {validation_result.message}") 

        df = pd.DataFrame(parsed_data) 

        # 4. Data Mapping (Lookups)
        if proc_config.get('needs_lookup'): 
            lookups_dir = os.path.join(self.base_dir, self.dirs['lookups'])
            df = apply_event_lookups(df, rule, lookups_dir)

        # 5. Save Processed File & Archive Raw File
        csv_path = os.path.join(proc_dir, filename.replace(os.path.splitext(filename)[1], '.csv')) 
        logger.info(f"💾 Saving {len(df)} rows to processed CSV: {csv_path}")
        df.to_csv(csv_path, index=False) 
        
        archive_path = os.path.join(arch_dir, filename) 
        logger.info(f"📦 Archiving raw file -> {archive_path}")
        shutil.move(temp_path, archive_path) 
        
        return df, validation_result, csv_path 

    def handle_failure(self, temp_path: str):
        """Moves a failing file from the inbox to the quarantine/failed folder."""
        logger = get_run_logger()
        if os.path.exists(temp_path): 
            failed_path = os.path.join(self.base_dir, self.dirs['failed'], os.path.basename(temp_path)) 
            logger.warning(f"⚠️ Moving failed file to quarantine: {failed_path}")
            shutil.move(temp_path, failed_path)
