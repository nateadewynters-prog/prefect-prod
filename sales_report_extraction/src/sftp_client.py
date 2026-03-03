import os
import paramiko
from prefect import get_run_logger
from src.notifications import send_teams_notification # Added import

def upload_to_sftp(local_file_path: str, filename: str):
    """Uploads a local file to the root directory of the configured SFTP server."""
    logger = get_run_logger() #
    
    # Fetch centralized env vars
    host = os.getenv("SFTP_SALES_DB_HOST") #
    port = int(os.getenv("SFTP_SALES_DB_PORT", 22)) #
    username = os.getenv("SFTP_LEGACY_SALES_DB_USERNAME") #
    password = os.getenv("SFTP_LEGACY_SALES_DB_PASSWORD") #

    if not all([host, username, password]): #
        raise ValueError("Missing SFTP credentials in environment variables.") #

    logger.info(f"📤 Connecting to SFTP server: {host}:{port}") #
    
    try:
        # Calculate file size for observability
        file_size_kb = os.path.getsize(local_file_path) / 1024 # Added
        
        transport = paramiko.Transport((host, port)) #
        transport.connect(username=username, password=password) #
        sftp = paramiko.SFTPClient.from_transport(transport) #
        
        remote_path = f"/{filename}" #
        
        logger.info(f"⬆️ Uploading {filename} ({file_size_kb:.2f} KB) to SFTP {remote_path}...") # Enhanced logging
        sftp.put(local_file_path, remote_path) #
        logger.info("✅ SFTP Upload successful.") #
        
        sftp.close() #
        transport.close() #
    except Exception as e:
        error_msg = f"SFTP Upload failed for {filename}: {str(e)}"
        logger.error(f"❌ {error_msg}") #
        send_teams_notification(f"🚨 **SFTP Delivery Failed**\n\n{error_msg}", logger) # Added alert
        raise #