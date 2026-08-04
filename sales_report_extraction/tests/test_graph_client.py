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

# ---------------------------------------------------------------------------
# 4. tag_email must ADD a tag, never replace the existing list
# ---------------------------------------------------------------------------
# Two rules can legitimately claim the same email (that is what FileNameKeyword
# is for). If tagging one rule's outcome wiped the other's, that report would be
# lost silently, so these tests pin the read-modify-write behaviour.
@patch('src.graph_client.get_run_logger')
@patch('src.graph_client.requests.patch')
@patch('src.graph_client.requests.get')
def test_tag_email_preserves_existing_categories(
    mock_get, mock_patch, mock_logger, mock_client
):
    """An existing tag must survive; the new one is appended to it."""
    mock_client._token = "fake-token"          # skip MSAL entirely

    mock_get.return_value = MagicMock(
        **{"json.return_value": {"categories": ["sales_report_failed"]},
           "raise_for_status.return_value": None}
    )
    mock_patch.return_value = MagicMock(status_code=200)

    assert mock_client.tag_email("MSG_1", "sales_report_extracted") is True

    sent = mock_patch.call_args.kwargs["json"]
    assert sent == {"categories": ["sales_report_failed", "sales_report_extracted"]}


@patch('src.graph_client.get_run_logger')
@patch('src.graph_client.requests.patch')
@patch('src.graph_client.requests.get')
def test_tag_email_on_untagged_email_sends_only_the_new_tag(
    mock_get, mock_patch, mock_logger, mock_client
):
    """The common case: no categories yet, so the payload is just the new tag."""
    mock_client._token = "fake-token"

    mock_get.return_value = MagicMock(
        **{"json.return_value": {}, "raise_for_status.return_value": None}
    )
    mock_patch.return_value = MagicMock(status_code=200)

    assert mock_client.tag_email("MSG_2", "sales_report_extracted") is True
    assert mock_patch.call_args.kwargs["json"] == {"categories": ["sales_report_extracted"]}


@patch('src.graph_client.get_run_logger')
@patch('src.graph_client.requests.patch')
@patch('src.graph_client.requests.get')
def test_tag_email_is_idempotent(mock_get, mock_patch, mock_logger, mock_client):
    """Re-tagging an already-tagged email is a no-op, not a duplicate entry."""
    mock_client._token = "fake-token"

    mock_get.return_value = MagicMock(
        **{"json.return_value": {"categories": ["sales_report_extracted"]},
           "raise_for_status.return_value": None}
    )

    assert mock_client.tag_email("MSG_3", "sales_report_extracted") is True
    mock_patch.assert_not_called()
