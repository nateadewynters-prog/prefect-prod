import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, mock_open

# --- TEST 1: The Orchestrator Routing ---
# CHANGED: rules now come from `rule_loader.load_rules()`, not `CONFIG['rules']`,
# so we mock the loader instead of CONFIG.
@patch('main.graph')
@patch('main.rule_loader')
@patch('main.get_run_logger')
def test_fetch_and_route_skips_categorized_emails(mock_logger, mock_rule_loader, mock_graph):
    mock_rule_loader.load_rules.return_value = [{
        'rule_name': 'TEST_ROUTING_RULE',
        'active': True,
        'match_criteria': {
            'sender_domain': 'theatre.com',
            'subject_keyword': 'Daily Sales',
            'attachment_type': '.pdf'
        }
    }]

    recent = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    fake_emails = [
        {
            "id": "MSG_1_CLEAN",
            "receivedDateTime": recent,
            "hasAttachments": True,
            "from": {"emailAddress": {"address": "figures@theatre.com"}},
            "categories": []
        },
        {
            "id": "MSG_2_TAGGED_SUCCESS",
            "receivedDateTime": recent,
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
    # The routing task should have pulled its rules from SharePoint, once.
    mock_rule_loader.load_rules.assert_called_once()


# --- TEST 2: The Unhappy Path (Lookup Failures) ---
# CHANGED: added @patch('main.SharePointUploader') so the test no longer builds a
# real uploader (which reads env / MSAL) on the first line of process_email.
@patch('main.SharePointUploader')
@patch('main.os.fsync')
@patch('builtins.open', new_callable=mock_open)
@patch('main.graph')
@patch('main.engine')
@patch('main.send_teams_notification')
@patch('main.get_run_logger')
@patch('src.error_db_client.log_lookup_failure')  # Patch the true source module!
def test_process_email_handles_lookup_failure_and_tags_failed(
    mock_log_db, mock_logger, mock_send_teams, mock_engine, mock_graph,
    mock_open_file, mock_fsync, mock_sp_uploader
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