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
        f"/{test_filename}.tmp"  # <-- Added .tmp extension
    )
    
    # Assert the atomic rename happens after the upload
    mock_sftp_session.rename.assert_called_once_with(
        f"/{test_filename}.tmp",
        f"/{test_filename}"
    )
    
    mock_sftp_session.close.assert_called_once()


# ---------------------------------------------------------------------------
# Credential selection: live vs test mode
# ---------------------------------------------------------------------------
# The full set of SFTP vars, so a lookup of the *wrong* key still returns
# something plausible and the test fails on the assertion, not a None.
FAKE_SFTP_ENV = {
    "SFTP_SALES_DB_HOST": "fake-server.internal",
    "SFTP_SALES_DB_PORT": "22",
    "SFTP_LEGACY_SALES_DB_USERNAME": "live_user",
    "SFTP_LEGACY_SALES_DB_PASSWORD": "live_password",
    "SFTP_TEST_LEGACY_SALES_DB_USERNAME": "test_user",
    "SFTP_TEST_LEGACY_SALES_DB_PASSWORD": "test_password",
}


@pytest.mark.parametrize("kwargs,expected_user,expected_password", [
    ({}, "live_user", "live_password"),                      # default: live
    ({"test_mode": False}, "live_user", "live_password"),
    ({"test_mode": True}, "test_user", "test_password"),
])
@patch('src.sftp_client.get_universal_logger')
@patch('src.sftp_client.paramiko.Transport')
@patch('src.sftp_client.paramiko.SFTPClient')
@patch('src.sftp_client.os.path.getsize')
@patch('src.sftp_client.os.getenv')
def test_upload_to_sftp_selects_credentials(
    mock_getenv,
    mock_getsize,
    mock_sftp_client,
    mock_transport,
    mock_universal_logger,
    kwargs,
    expected_user,
    expected_password
):
    """Test mode swaps the username/password pair but keeps host and port."""
    mock_getenv.side_effect = lambda key, default=None: FAKE_SFTP_ENV.get(key, default)
    mock_getsize.return_value = 10240
    mock_sftp_client.from_transport.return_value = MagicMock()

    upload_to_sftp(
        local_file_path="/fake/local/processed/report.csv",
        filename="report.csv",
        **kwargs
    )

    # Host and port are shared by both modes.
    mock_transport.assert_called_once_with(("fake-server.internal", 22))
    mock_transport.return_value.connect.assert_called_once_with(
        username=expected_user, password=expected_password
    )