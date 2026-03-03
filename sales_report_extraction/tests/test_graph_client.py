import pytest
from unittest.mock import patch, MagicMock
from src.graph_client import GraphClient

# 1. Setup a "Fixture" - This gives us a fresh client before every test
@pytest.fixture
def mock_client():
    return GraphClient(
        tenant_id="fake_tenant",
        client_id="fake_client",
        client_secret="fake_secret",
        target_user="fake_user@domain.com"
    )

# 2. Test the Authentication Logic
@patch('src.graph_client.get_run_logger') # Added to bypass Prefect context error
@patch('src.graph_client.msal.ConfidentialClientApplication')
def test_get_token_success(mock_msal_class, mock_get_run_logger, mock_client):
    """Test that the client successfully extracts the token from MSAL."""

    # Arrange: Create a fake MSAL app that returns a fake token
    mock_app_instance = MagicMock()
    mock_app_instance.acquire_token_for_client.return_value = {"access_token": "super_secret_token_123"}
    mock_msal_class.return_value = mock_app_instance

    # Act: Call the method we want to test
    token = mock_client._get_token()

    # Assert: Verify the method did what it was supposed to do
    assert token == "super_secret_token_123"
    mock_msal_class.assert_called_once_with(
        "fake_client",
        authority="https://login.microsoftonline.com/fake_tenant",
        client_credential="fake_secret"
    )
    mock_app_instance.acquire_token_for_client.assert_called_once_with(
        scopes=["https://graph.microsoft.com/.default"]
    )

# 3. Test Failure Handling
@patch('src.graph_client.get_run_logger') # Added to bypass Prefect context error
@patch('src.graph_client.msal.ConfidentialClientApplication')
def test_get_token_failure(mock_msal_class, mock_get_run_logger, mock_client):
    """Test that the client raises an exception if authentication fails."""

    # Arrange: Create a fake MSAL app that returns an error instead of a token
    mock_app_instance = MagicMock()
    mock_app_instance.acquire_token_for_client.return_value = {
        "error": "invalid_client",
        "error_description": "AADSTS7000215: Invalid client secret provided."
    }
    mock_msal_class.return_value = mock_app_instance

    # Act & Assert: Verify that our code raises a Python Exception
    with pytest.raises(Exception) as exc_info:
        mock_client._get_token()

    # Assert that our custom error message is present
    assert "Failed to acquire Graph Token" in str(exc_info.value)
    assert "Invalid client secret" in str(exc_info.value)
