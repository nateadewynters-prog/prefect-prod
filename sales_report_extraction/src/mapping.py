import os
import pandas as pd
from prefect import get_run_logger

def apply_event_lookups(df: pd.DataFrame, rule: dict, lookups_dir: str) -> pd.DataFrame:
    """Merges parsed data with Medallion lookup files. Raises ValueError on failure."""
    logger = get_run_logger()
    meta = rule['metadata']
    
    lookup_file = os.path.join(lookups_dir, f"{meta['show_id']}_{meta['venue_id']}_event_dates.csv") 
    
    if not os.path.exists(lookup_file): 
        logger.error(f"❌ Missing required lookup file: {os.path.abspath(lookup_file)}")
        raise ValueError(f"Missing lookup file: {lookup_file}") 
    
    # Clean strings and merge
    lookup_df = pd.read_csv(lookup_file) 
    df['Performance/Event Code'] = df['Performance/Event Code'].astype(str).str.strip() 
    lookup_df['Show Code'] = lookup_df['Show Code'].astype(str).str.strip() 
    
    df = df.merge(
        lookup_df[['Show Code', 'Performance Date Time']], 
        left_on='Performance/Event Code', 
        right_on='Show Code', 
        how='left'
    ) 
    
    # Check for unmapped codes
    unmapped = df[df['Performance Date Time'].isna()]['Performance/Event Code'].unique() 
    if len(unmapped) > 0: 
        unmapped_str = ', '.join(map(str, unmapped[:5]))
        logger.error(f"❌ Lookup Merge Failed. Unmapped codes: {{{unmapped_str}}}")
        raise ValueError(f"Lookup Merge Failed: Unmapped codes found {{{unmapped_str}}}") 
        
    return df
