import base64
import urllib.parse
import re
import requests
from bs4 import BeautifulSoup
from prefect import get_run_logger

def extract_target_url_from_sophos(sophos_url: str) -> str | None:
    """Decodes Sophos Base64 string to reveal the true URL."""
    parsed = urllib.parse.urlparse(sophos_url)
    queries = urllib.parse.parse_qs(parsed.query)
    
    encoded_url = queries.get('u')
    if not encoded_url: return None
        
    base64_str = encoded_url[0].replace('-', '+').replace('_', '/')
    missing_padding = len(base64_str) % 4
    if missing_padding: base64_str += '=' * (4 - missing_padding)
        
    try:
        return base64.b64decode(base64_str).decode('utf-8')
    except Exception:
        return None

def download_from_html_link(html_body: str, output_path: str):
    """Parses email HTML, breaks Sophos encryption, and downloads the file."""
    logger = get_run_logger()
    soup = BeautifulSoup(html_body, 'html.parser')
    
    # Locate the button or Sophos protected link
    download_btn = soup.find('a', string=lambda text: text and 'Download Excel' in text)
    if not download_btn:
        download_btn = soup.find('a', href=re.compile(r'protection\.sophos\.com'))
        
    if not download_btn or not download_btn.get('href'):
        raise ValueError("Could not find a valid Sophos/Ticketmaster download link in email body.")
        
    real_tm_url = extract_target_url_from_sophos(download_btn['href'])
    if not real_tm_url:
        raise ValueError("Failed to decode the Sophos wrapper link.")
        
    # Navigate Ticketmaster redirects and download
    session = requests.Session()
    response = session.get(real_tm_url, allow_redirects=True)
    response.raise_for_status()

    tm_params = urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query)
    target_url_param = tm_params.get('targetUrl')
    download_link = urllib.parse.unquote(target_url_param[0]) if target_url_param else response.url

    file_response = session.get(download_link)
    file_response.raise_for_status()

    # An expired or unauthenticated link answers with a login or "report not
    # ready" page and answers HTTP 200, so raise_for_status() sees nothing
    # wrong. Left unchecked, that page is written under the report's real
    # filename and a passthrough rule delivers it to the contractor as the
    # day's sales figures.
    content_type = file_response.headers.get('Content-Type', '')
    if 'html' in content_type.lower():
        raise ValueError(
            f"Download returned a web page, not a report (Content-Type: "
            f"{content_type}). The link has probably expired."
        )

    # Save directly to the provided temp_path
    with open(output_path, 'wb') as f:
        f.write(file_response.content)
        
    logger.info(f"✅ Successfully downloaded link-based file to {output_path}")