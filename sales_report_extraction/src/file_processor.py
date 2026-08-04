import os
import shutil
import importlib
import pandas as pd
from prefect import get_run_logger
from src.models import ValidationResult
from src.naming import (
    generate_standard_filename,
    get_medallion_folders,
    RAW_PREFIX,
    PROCESSED_PREFIX,
)
from src.mapping import apply_event_lookups

# Excel rejects these characters in a sheet name and caps the name at 31 chars.
INVALID_SHEET_CHARS = "[]:*?/\\"


def _safe_sheet_name(name: str) -> str:
    """Turn a show name into a legal Excel sheet name."""
    for char in INVALID_SHEET_CHARS:
        name = name.replace(char, "-")
    return name.strip().upper()[:31] or "SHEET1"


def _check_passthrough_is_deliverable(temp_path: str, filename: str) -> int:
    """
    Sanity-check a passthrough file, and return its size in bytes.

    Passthrough rules never parse the file, so this is the only point at which
    anything looks at it before the contractor receives it. A link-based report
    whose download link has expired comes back as a login or "report not ready"
    page with HTTP 200, and without this check it is archived, declared PASSED
    and delivered as the day's sales figures.

    Deliberately narrow, because some suppliers legitimately export an HTML
    table saved as .xls: a file is only rejected if it opens like a web page
    *and* contains no table at all, which is what a login/error page looks like.
    """
    size = os.path.getsize(temp_path)
    if size == 0:
        raise ValueError(f"Passthrough file {filename} is 0 bytes; refusing to deliver it.")

    with open(temp_path, 'rb') as fh:
        head = fh.read(512).lstrip().lower()

    if head.startswith((b'<!doctype', b'<html')):
        # Only markup gets this far, so reading it whole is cheap and avoids
        # missing a <table> that sits past a long <head> or <style> block.
        with open(temp_path, 'rb') as fh:
            if b'<table' not in fh.read().lower():
                raise ValueError(
                    f"Passthrough file {filename} is a web page with no data table - "
                    f"the download link has probably expired or returned a login "
                    f"page. Refusing to deliver it."
                )

    return size


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

    def generate_filename(self, metadata: dict, date_str: str, ext: str, test_mode: bool = False) -> str:
        """Wrapper for standard naming utility."""
        return generate_standard_filename(metadata, date_str, ext, test_mode)

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

            # Check before the move, so a rejected file stays in the inbox for
            # handle_failure to quarantine rather than landing in the archive
            # zone looking like it was delivered.
            file_size = _check_passthrough_is_deliverable(temp_path, filename)

            # 🚀 THE FIX: Use arch_dir instead of proc_dir
            final_path = os.path.join(arch_dir, filename)
            shutil.move(temp_path, final_path)

            val_res = ValidationResult(
                status="PASSED", message="File passed through and archived.",
                metrics={"action": "passthrough", "bytes": file_size}
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
        # A parser can declare OUTPUT_EXT (e.g. ".xlsx") when the contractor needs
        # a specific format. Parsers that don't declare one still get a .csv, so
        # this stays backwards compatible with every existing rule.
        output_ext = getattr(parser_module, 'OUTPUT_EXT', '.csv')
        # The parsed output leaves the raw zone, so the stage marker moves with it.
        # Count of 1 keeps any TEST. prefix in front: TEST.RAW.x -> TEST.PROCESSED.x
        output_name = filename.replace(RAW_PREFIX, PROCESSED_PREFIX, 1)
        output_path = os.path.join(proc_dir, f"{os.path.splitext(output_name)[0]}{output_ext}")

        logger.info(f"💾 Saving {len(df)} rows to processed file: {output_path}")
        if output_ext == '.xlsx':
            sheet_name = _safe_sheet_name(rule['metadata'].get('show_name', 'Sheet1'))
            df.to_excel(output_path, index=False, sheet_name=sheet_name)
        else:
            df.to_csv(output_path, index=False)
        
        archive_path = os.path.join(arch_dir, filename) 
        logger.info(f"📦 Archiving raw file -> {archive_path}")
        shutil.move(temp_path, archive_path) 
        
        return df, validation_result, output_path 

    def handle_failure(self, temp_path: str):
        """Moves a failing file from the inbox to the quarantine/failed folder."""
        logger = get_run_logger()
        if os.path.exists(temp_path): 
            failed_path = os.path.join(self.base_dir, self.dirs['failed'], os.path.basename(temp_path)) 
            logger.warning(f"⚠️ Moving failed file to quarantine: {failed_path}")
            shutil.move(temp_path, failed_path)
