import pytest
from unittest.mock import patch

# We patch the global objects instantiated in main.py before they run
@patch('main.graph')
@patch('main.CONFIG')
@patch('main.get_run_logger')
def test_fetch_and_route_skips_categorized_emails(mock_logger, mock_config, mock_graph):
    """
    Test that emails with the 'sales_report_extracted' category are 
    skipped, while clean emails are routed as candidates.
    """
    
    # --- 1. Arrange ---
    # Mock the global CONFIG dictionary loaded in main.py
    mock_config.__getitem__.side_effect = lambda k: {
        'rules': [{
            'rule_name': 'TEST_ROUTING_RULE',
            'active': True,
            'backfill_since': '2026-01-01',
            'match_criteria': {
                'sender_domain': 'theatre.com',
                'subject_keyword': 'Daily Sales',
                'attachment_type': '.pdf'
            }
        }]
    }[k]

    # Create fake emails to simulate the Graph API response
    fake_emails = [
        {
            "id": "MSG_1_CLEAN",
            "receivedDateTime": "2026-03-06T10:00:00Z",
            "hasAttachments": True,
            "from": {"emailAddress": {"address": "figures@theatre.com"}},
            "categories": []  # No tags: Should be processed
        },
        {
            "id": "MSG_2_TAGGED",
            "receivedDateTime": "2026-03-06T10:05:00Z",
            "hasAttachments": True,
            "from": {"emailAddress": {"address": "figures@theatre.com"}},
            # Tagged with our success category: Should be skipped!
            "categories": ["sales_report_extracted", "Blue category"] 
        },
        {
            "id": "MSG_3_NO_ATTACHMENT",
            "receivedDateTime": "2026-03-06T10:10:00Z",
            "hasAttachments": False, # No attachment: Should be skipped
            "from": {"emailAddress": {"address": "figures@theatre.com"}},
            "categories": [] 
        }
    ]
    
    # Force our mocked graph client to return these fake emails
    mock_graph.search_emails.return_value = fake_emails

    # --- 2. Act ---
    from main import fetch_and_route_emails
    
    # Because fetch_and_route_emails is a Prefect @task, we call .fn() 
    # to run the raw, underlying Python function without triggering Prefect's engine.
    candidates = fetch_and_route_emails.fn()

    # --- 3. Assert ---
    # Only MSG_1_CLEAN should have made it through the filters
    assert len(candidates) == 1
    assert candidates[0]['email_data']['id'] == "MSG_1_CLEAN"