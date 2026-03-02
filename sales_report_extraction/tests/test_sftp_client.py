# tests/test_sftp_client.py
import pytest
from unittest.mock import patch, MagicMock
from src.sftp_client import upload_to_sftp

# 👇 ADDED THE LOGGER MOCK HERE 👇
@patch('src.sftp_client.get_run_logger') 
@patch('src.sftp_client.paramiko.Transport')
@patch('src.sftp_client.paramiko.SFTPClient')
@patch('src.sftp_client.os.getenv')
# 👇 ADDED mock_get_run_logger TO THE ARGUMENTS 👇
def test_upload_to_sftp_success(mock_getenv, mock_sftp_client, mock_transport, mock_get_run_logger):
    
    # --- 1. Arrange: Setup the fake environment ---
    
    # Create a fake dictionary of environment variables
    def fake_env_vars(key, default=None):
        env_map = {
            "SFTP_SALES_DB_HOST": "fake-server.internal",
            "SFTP_SALES_DB_PORT": "22",
            "SFTP_LEGACY_SALES_DB_USERNAME": "test_user",
            "SFTP_LEGACY_SALES_DB_PASSWORD": "super_secret_fake_password"
        }
        return env_map.get(key, default)
    
    # Tell our mocked os.getenv to use our fake dictionary
    mock_getenv.side_effect = fake_env_vars

    # Setup a fake SFTP session object
    mock_sftp_session = MagicMock()
    mock_sftp_client.from_transport.return_value = mock_sftp_session

    # --- 2. Act: Run our function ---
    local_test_path = "/fake/local/processed/venue_show_123.csv"
    test_filename = "venue_show_123.csv"
    
    upload_to_sftp(local_file_path=local_test_path, filename=test_filename)

    # --- 3. Assert: Verify the logic did exactly what we wanted ---
    
    # Did it try to connect to the right host and port?
    mock_transport.assert_called_once_with(("fake-server.internal", 22))
    
    # Did it attempt to put the file in the root directory '/' as requested?
    mock_sftp_session.put.assert_called_once_with(
        "/fake/local/processed/venue_show_123.csv", 
        "/venue_show_123.csv"
    )
    
    # Did it clean up the connections properly?
    mock_sftp_session.close.assert_called_once()