import pytest
from unittest.mock import patch, MagicMock, mock_open

# --- TEST 1: The Orchestrator Routing ---
@patch('main.graph')
@patch('main.CONFIG')
@patch('main.get_run_logger')
def test_fetch_and_route_skips_categorized_emails(mock_logger, mock_config, mock_graph):
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
        }
    ]
    mock_graph.search_emails.return_value = fake_emails

    from main import fetch_and_route_emails
    candidates = fetch_and_route_emails.fn(days_back=30)

    assert len(candidates) == 1
    assert candidates[0]['email_data']['id'] == "MSG_1_CLEAN"

# --- TEST 2: The Unhappy Path (Lookup Failures) ---
@patch('main.os.fsync') 
@patch('builtins.open', new_callable=mock_open) 
@patch('main.graph')
@patch('main.engine')
@patch('main.send_teams_notification')
@patch('main.get_run_logger')
@patch('src.error_db_client.log_lookup_failure') # 🚀 FIX: Patch the true source module!
def test_process_email_handles_lookup_failure_and_tags_failed(
    mock_log_db, mock_logger, mock_send_teams, mock_engine, mock_graph, mock_open_file, mock_fsync
):
    from main import process_email
    
    candidate = {
        'email_data': {
            'id': 'FAIL_MSG_123',
            'subject': 'Test Broken Lookup',
            'receivedDateTime': '2026-03-07T10:00:00Z'
        },
        'rule': {
            'rule_name': 'TEST_BROKEN_RULE',
            'match_criteria': {'attachment_type': '.xls'},
            'metadata': {'show_name': 'Test', 'venue_name': 'Test'}
        }
    }
    
    mock_graph.download_attachment.return_value = (b"fake_excel_bytes", "report.xls")
    mock_engine.generate_filename.return_value = "fake_file.xls"
    mock_engine.base_dir = "/fake/dir"
    mock_engine.dirs = {'inbox': 'inbox', 'failed': 'failed'}
    mock_open_file.return_value.fileno.return_value = 123
    mock_engine.process_file.side_effect = ValueError("Unmapped codes found {VIP-PKG}")
    
    success, r_name, info = process_email.fn(candidate)
    
    assert success is False
    assert info is None 
    mock_graph.tag_email.assert_called_with('FAIL_MSG_123', 'sales_report_failed')
    
    sent_msg = mock_send_teams.call_args.kwargs.get('message', '')
    assert "Action Required: Data Mapping Failed" in sent_msg
    assert mock_send_teams.call_args.kwargs.get('channel') == 'dev'
