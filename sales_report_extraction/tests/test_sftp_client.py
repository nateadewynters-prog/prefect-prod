import pytest
from unittest.mock import patch, MagicMock
from src.sftp_client import upload_to_sftp

# 🚀 FIX: Changed to mock get_universal_logger instead of get_run_logger
@patch('src.sftp_client.get_universal_logger')
@patch('src.sftp_client.paramiko.Transport')
@patch('src.sftp_client.paramiko.SFTPClient')
@patch('src.sftp_client.os.path.getsize')
@patch('src.sftp_client.os.getenv')
def test_upload_to_sftp_success(
    mock_getenv, 
    mock_getsize, 
    mock_sftp_client, 
    mock_transport, 
    mock_universal_logger
):
    """Test that upload_to_sftp connects and puts the file correctly."""
    
    def fake_env_vars(key, default=None):
        env_map = {
            "SFTP_SALES_DB_HOST": "fake-server.internal",
            "SFTP_SALES_DB_PORT": "22",
            "SFTP_LEGACY_SALES_DB_USERNAME": "test_user",
            "SFTP_LEGACY_SALES_DB_PASSWORD": "super_secret_fake_password"
        }
        return env_map.get(key, default)

    mock_getenv.side_effect = fake_env_vars
    mock_getsize.return_value = 10240

    mock_sftp_session = MagicMock()
    mock_sftp_client.from_transport.return_value = mock_sftp_session

    local_test_path = "/fake/local/processed/venue_show_123.csv"
    test_filename = "venue_show_123.csv"

    upload_to_sftp(local_file_path=local_test_path, filename=test_filename)

    mock_getsize.assert_called_once_with(local_test_path)
    mock_transport.assert_called_once_with(("fake-server.internal", 22))
    mock_sftp_session.put.assert_called_once_with(
        local_test_path,
        f"/{test_filename}"
    )
    mock_sftp_session.close.assert_called_once()
