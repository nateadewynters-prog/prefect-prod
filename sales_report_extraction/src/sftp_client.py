import os
import paramiko
from src.env_setup import get_universal_logger

def upload_to_sftp(local_file_path: str, filename: str):
    """Uploads a local file to the root directory of the configured SFTP server."""
    logger = get_universal_logger(__name__)
    
    host = os.getenv("SFTP_SALES_DB_HOST")
    port = int(os.getenv("SFTP_SALES_DB_PORT", 22))
    username = os.getenv("SFTP_LEGACY_SALES_DB_USERNAME")
    password = os.getenv("SFTP_LEGACY_SALES_DB_PASSWORD")

    if not all([host, username, password]):
        raise ValueError("Missing SFTP credentials in environment variables.")

    logger.info(f"📤 Connecting to SFTP server: {host}:{port}")
    
    try:
        file_size_kb = os.path.getsize(local_file_path) / 1024
        
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        remote_path = f"/{filename}"
        
        logger.info(f"⬆️ Uploading {filename} ({file_size_kb:.2f} KB) to SFTP {remote_path}...")
        sftp.put(local_file_path, remote_path)
        logger.info("✅ SFTP Upload successful.")
        
        sftp.close()
        transport.close()
    except Exception as e:
        logger.error(f"❌ SFTP Upload failed for {filename}: {str(e)}")
        raise