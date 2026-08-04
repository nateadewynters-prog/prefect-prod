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

# --- TEST 3: Delivery is only reported as successful once the email is tagged ---
# The email category is the only durable record that a report has been sent, so
# an untagged-but-delivered email is re-delivered on the next run. tag_email can
# return False without raising, so process_email must not report success then.
@patch('main.SharePointUploader')
@patch('main.upload_to_sftp')
@patch('main.os.path.exists', return_value=True)
@patch('main.create_markdown_artifact')
@patch('main.os.fsync')
@patch('builtins.open', new_callable=mock_open)
@patch('main.graph')
@patch('main.engine')
@patch('main.rule_loader')
@patch('main.send_teams_notification')
@patch('main.get_run_logger')
def _run_process_email(mock_logger, mock_send_teams, mock_rule_loader, mock_engine,
                       mock_graph, mock_open_file, mock_fsync, mock_artifact,
                       mock_exists, mock_sftp, mock_sp_uploader, *, tag_result):
    """Drive process_email down its success path with tag_email returning tag_result."""
    from main import process_email

    candidate = {
        'email_data': {
            'id': 'OK_MSG_123',
            'subject': 'Daily Sales',
            'receivedDateTime': '2026-03-07T10:00:00Z'
        },
        'rule': {
            'rule_name': 'TEST_OK_RULE',
            'match_criteria': {'attachment_type': '.xls'},
            'metadata': {'show_name': 'Test', 'venue_name': 'Test'},
            'processing': {'passthrough_only': True},
        }
    }

    mock_graph.download_attachment.return_value = (b"bytes", "report.xls")
    mock_graph.tag_email.return_value = tag_result
    mock_engine.generate_filename.return_value = "RAW.fake.xls"
    mock_engine.base_dir = "/fake/dir"
    mock_engine.dirs = {'inbox': 'inbox', 'failed': 'failed'}
    mock_open_file.return_value.fileno.return_value = 123

    val = MagicMock(status="PASSED", message="ok", metrics={})
    mock_engine.process_file.return_value = (None, val, "/fake/dir/archive/RAW.fake.xls")

    return process_email.fn(candidate), mock_sftp, mock_graph


def test_process_email_succeeds_when_tagging_works():
    (success, r_name, info), mock_sftp, mock_graph = _run_process_email(tag_result=True)

    assert success is True
    mock_sftp.assert_called_once()
    mock_graph.tag_email.assert_any_call('OK_MSG_123', 'sales_report_extracted')


def test_process_email_fails_if_the_email_cannot_be_tagged_after_delivery():
    """Delivered but untagged must be reported as a failure, not a success."""
    (success, r_name, info), mock_sftp, mock_graph = _run_process_email(tag_result=False)

    # The file did go out, so this is a genuine problem an operator must see.
    mock_sftp.assert_called_once()
    assert success is False
    assert info is None
