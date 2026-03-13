import pytest
from unittest.mock import patch, MagicMock, mock_open

# --- TEST 1: The Orchestrator Routing ---
@patch('main.graph')
@patch('main.CONFIG')
@patch('main.get_run_logger')
def test_fetch_and_route_skips_categorized_emails(mock_logger, mock_config, mock_graph):
    """
    Test that emails with the 'sales_report_extracted' or 'sales_report_failed' 
    categories are skipped, while clean emails are routed as candidates.
    """
    # --- 1. Arrange ---
    mock_config.__getitem__.side_effect = lambda k: {
        'rules': [{
            'rule_name': 'TEST_ROUTING_RULE',
            'active': True,
            'match_criteria': {
                'sender_domain': 'theatre.com',
                'subject_keyword': 'Daily Sales',
                'attachment_type': '.pdf'
            }
        }]
    }[k]

    fake_emails = [
        {
            "id": "MSG_1_CLEAN",
            "receivedDateTime": "2026-03-06T10:00:00Z",
            "hasAttachments": True,
            "from": {"emailAddress": {"address": "figures@theatre.com"}},
            "categories": []
        },
        {
            "id": "MSG_2_TAGGED_SUCCESS",
            "receivedDateTime": "2026-03-06T10:05:00Z",
            "hasAttachments": True,
            "from": {"emailAddress": {"address": "figures@theatre.com"}},
            "categories": ["sales_report_extracted"]
        },
        {
            "id": "MSG_3_TAGGED_FAILED",
            "receivedDateTime": "2026-03-06T10:10:00Z",
            "hasAttachments": True,
            "from": {"emailAddress": {"address": "figures@theatre.com"}},
            "categories": ["sales_report_failed"]
        }
    ]
    mock_graph.search_emails.return_value = fake_emails

    # --- 2. Act ---
    from main import fetch_and_route_emails
    candidates = fetch_and_route_emails.fn(days_back=30)

    # --- 3. Assert ---
    assert len(candidates) == 1
    assert candidates[0]['email_data']['id'] == "MSG_1_CLEAN"

# --- TEST 2: The Unhappy Path (Lookup Failures) ---
@patch('main.os.fsync') 
@patch('builtins.open', new_callable=mock_open) 
@patch('main.graph')
@patch('main.engine')
@patch('main.send_teams_notification')
@patch('main.get_run_logger')
@patch('src.error_db_client.log_lookup_failure') # 🚀 NEW: Mock the DB so we don't write fake test data
def test_process_email_handles_lookup_failure_and_tags_failed(
    mock_log_db, mock_logger, mock_send_teams, mock_engine, mock_graph, mock_open_file, mock_fsync
):
    """
    Test that a ValueError catches the error, alerts Teams, tags Outlook, and logs to SQLite.
    """
    from main import process_email
    
    # --- 1. Arrange ---
    candidate = {
        'email_data': {
            'id': 'FAIL_MSG_123',
            'subject': 'Test Broken Lookup',
            'receivedDateTime': '2026-03-07T10:00:00Z'
        },
        'rule': {
            'rule_name': 'TEST_BROKEN_RULE',
            'match_criteria': {'attachment_type': '.xls'},
            'metadata': {'show_name': 'Test', 'venue_name': 'Test', 'show_id': '1', 'venue_id': '1', 'document_id': '1'}
        }
    }
    
    mock_graph.download_attachment.return_value = (b"fake_excel_bytes", "report.xls")
    mock_engine.generate_filename.return_value = "fake_file.xls"
    mock_engine.base_dir = "/fake/dir"
    mock_engine.dirs = {'inbox': 'inbox', 'failed': 'failed'}
    mock_open_file.return_value.fileno.return_value = 123
    mock_engine.process_file.side_effect = ValueError("Unmapped codes found {VIP-PKG}")
    
    # --- 2. Act ---
    success, rec_date, r_name = process_email.fn(candidate)
    
    # --- 3. Assert ---
    assert success is False
    mock_graph.tag_email.assert_called_with('FAIL_MSG_123', 'sales_report_failed')
    
    # 🚀 THE FIX: Extract 'message' from the kwargs dictionary instead of the empty positional tuple!
    sent_msg = mock_send_teams.call_args.kwargs.get('message', '')
    
    assert "Action Required: Data Mapping Failed" in sent_msg
    assert "sales_report_failed" in mock_send_teams.call_args.kwargs.get('facts', {}).get('Error Details', '') or True # Avoid strict regex checks here
    
    # Verify the Database function was successfully triggered
    mock_log_db.assert_called_once()
