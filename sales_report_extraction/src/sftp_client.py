import os
import paramiko
from prefect import get_run_logger

def upload_to_sftp(local_file_path: str, filename: str):
    """Uploads a local file to the root directory of the configured SFTP server."""
    logger = get_run_logger()
    
    # Fetch centralized env vars
    host = os.getenv("SFTP_SALES_DB_HOST")
    port = int(os.getenv("SFTP_SALES_DB_PORT", 22))
    username = os.getenv("SFTP_LEGACY_SALES_DB_USERNAME")
    password = os.getenv("SFTP_LEGACY_SALES_DB_PASSWORD")

    if not all([host, username, password]):
        raise ValueError("Missing SFTP credentials in environment variables.")

    logger.info(f"📤 Connecting to SFTP server: {host}:{port}")
    
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # The user requested the file to be placed in the root directory '/'
        remote_path = f"/{filename}"
        
        logger.info(f"⬆️ Uploading {filename} to SFTP {remote_path}...")
        sftp.put(local_file_path, remote_path)
        logger.info("✅ SFTP Upload successful.")
        
        sftp.close()
        transport.close()
    except Exception as e:
        logger.error(f"❌ SFTP Upload failed: {str(e)}")
        raise