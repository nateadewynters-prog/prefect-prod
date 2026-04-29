import os
import requests
import urllib.parse
from bs4 import BeautifulSoup
from src.env_setup import get_universal_logger

def extract_direct_url(initial_url: str, logger) -> str:
    """
    Parses a URL to see if it contains a hidden redirect target 
    (like Ticketmaster's AWS S3 'targetUrl').
    """
    try:
        parsed = urllib.parse.urlparse(initial_url)
        params = urllib.parse.parse_qs(parsed.query)
        
        # Check for Ticketmaster/AWS Pre-Signed URLs
        aws_link = params.get('targetUrl', [None])[0]
        if aws_link:
            logger.info("🔓 Hijacked direct AWS targetUrl from link parameters.")
            return aws_link
    except Exception as e:
        logger.warning(f"⚠️ Could not parse URL parameters: {e}")
        
    return initial_url

def download_from_email_body(html_body: str, output_filepath: str) -> bool:
    """
    Scans HTML for a download link, resolves Sophos/Ticketmaster redirects, 
    and saves the binary file.
    """
    logger = get_universal_logger(__name__)
    
    if not html_body:
        logger.error("❌ Email body is empty. Cannot extract link.")
        return False

    # 1. Find the Link
    soup = BeautifulSoup(html_body, 'html.parser')
    link = next((a['href'] for a in soup.find_all('a', href=True) if "Download" in a.text), None)
    
    if not link:
        logger.error("❌ No link containing 'Download' found in the email body.")
        return False
        
    logger.info(f"🔗 Found Download Link in email. Commencing extraction...")

    # 2. Setup Session (Mimic a browser to prevent blocks)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    try:
        # 3. Follow the redirect chain to the Portal Page
        portal_resp = session.get(link, allow_redirects=True, timeout=30)
        
        # 4. Extract the hidden direct download link (Bypass the login wall)
        final_download_url = extract_direct_url(portal_resp.url, logger)
        
        # 5. Fetch the actual file
        logger.info("⬇️ Downloading binary stream...")
        file_resp = session.get(final_download_url, timeout=30)
        file_resp.raise_for_status()
        
        # 6. Validate Size (> 10KB means it's a real file, not an HTML error page)
        size_kb = len(file_resp.content) / 1024
        if size_kb > 10:
            with open(output_filepath, 'wb') as f:
                f.write(file_resp.content)
            logger.info(f"✅ Successfully downloaded via link to {os.path.basename(output_filepath)} ({size_kb:.2f} KB)")
            return True
        else:
            logger.error(f"❌ Downloaded file is too small ({size_kb:.2f} KB). Likely hit an authentication wall.")
            return False
            
    except Exception as e:
        logger.error(f"💥 Link extraction failed: {e}")
        return False