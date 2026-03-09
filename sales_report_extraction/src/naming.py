import pytz
from datetime import timezone, timedelta
from dateutil import parser as date_parser

def generate_standard_filename(metadata: dict, date_str: str, ext: str) -> str:
    """Calculates the deterministic venue timezone and formats the standard filename."""
    # 1. Parse UTC and apply Venue Timezone
    utc_dt = date_parser.parse(date_str).astimezone(timezone.utc)
    venue_tz = pytz.timezone(metadata.get('timezone', 'UTC'))
    local_dt = utc_dt.astimezone(venue_tz)
    
    # 2. Subtract 1 day for end-of-day reporting
    report_dt = local_dt - timedelta(days=1)
    
    # 3. Format and sanitize
    fmt_date = report_dt.strftime("%d_%m_%Y") 
    name = f"{metadata['show_name']}.{metadata['venue_name']}_{metadata['show_id']}_{metadata['venue_id']}_{metadata['document_id']}_{fmt_date}{ext}" 
    
    return name.replace(" ", "-").replace("/", "-")

def get_medallion_folders(base_dir: str, dirs: dict, metadata: dict) -> tuple:
    """Generates the Show/Venue nested paths for processed and archive zones."""
    show_folder = metadata['show_name'].replace(" ", "-").replace("/", "-")
    venue_folder = metadata['venue_name'].replace(" ", "-").replace("/", "-")
    
    proc_dir = f"{base_dir}/{dirs['processed']}/{show_folder}/{venue_folder}"
    arch_dir = f"{base_dir}/{dirs['archive']}/{show_folder}/{venue_folder}"
    
    return proc_dir, arch_dir
