import os
import pytest
from datetime import timezone
from src.file_processor import ProcessingEngine

# 1. Setup our "Fixture"
@pytest.fixture
def mock_engine(tmp_path):
    """
    Creates an isolated ProcessingEngine using pytest's temporary directory.
    This guarantees we never touch the real /opt/prefect/prod/code/data folders.
    """
    global_config = {
        "base_dir": str(tmp_path), # <-- The magic happens here!
        "data_dirs": {
            "inbox": "data/inbox",
            "processed": "data/processed",
            "archive": "data/archive",
            "failed": "data/failed",
            "lookups": "data/lookups"
        }
    }
    config_path = str(tmp_path / "fake_config.json")
    
    return ProcessingEngine(global_config=global_config, config_path=config_path)

# 2. Test Initialization
def test_ensure_directories(mock_engine, tmp_path):
    """Test that initialization successfully creates all required directories."""
    assert os.path.exists(tmp_path / "data" / "inbox")
    assert os.path.exists(tmp_path / "data" / "processed")
    assert os.path.exists(tmp_path / "data" / "archive")

# 3. Test Business Logic (Date Math & String Formatting)
def test_generate_filename(mock_engine):
    """Test that T-1 date subtraction and standard string formatting work correctly."""
    metadata = {
        "show_name": "Phantom",
        "venue_name": "West End",
        "show_id": "100",
        "venue_id": "200",
        "document_id": "300"
    }
    
    # Simulate receiving an email on Feb 27, 2026 at 14:00 UTC
    received_date = "2026-02-27T14:00:00Z"
    
    filename = mock_engine.generate_filename(metadata, received_date, ".pdf")
    
    # Expected: The date should shift back 1 day to the 26th, and spaces become hyphens.
    expected_filename = "Phantom.West-End_100_200_300_26_02_2026.pdf"
    assert filename == expected_filename

def test_generate_filename_with_deterministic_timezones(mock_engine):
    """
    Test that the deterministic timezone logic correctly shifts the date 
    based on the venue's physical location before applying the T-1 rule.
    """
    
    # ⏱️ SCENARIO: An email arrives at 5:00 PM UTC on March 8th, 2026.
    received_date_utc = "2026-03-08T17:00:00Z"
    
    # --- TEST 1: Singapore (UTC+8) ---
    # 5:00 PM UTC on the 8th is actually 1:00 AM on the 9th in Singapore!
    # T-1 Rule: Minus 1 day for the end-of-day report = March 8th.
    metadata_sg = {
        "show_name": "Jesus Christ Superstar", 
        "venue_name": "Singapore",
        "show_id": "287", "venue_id": "125", "document_id": "501",
        "timezone": "Asia/Singapore" # 🚀 The new config field
    }
    filename_sg = mock_engine.generate_filename(metadata_sg, received_date_utc, ".xls")
    
    # Assert the date in the filename is the 8th
    assert "08_03_2026" in filename_sg
    assert filename_sg == "Jesus-Christ-Superstar.Singapore_287_125_501_08_03_2026.xls"

    # --- TEST 2: Los Angeles (UTC-8) ---
    # 5:00 PM UTC on the 8th is 9:00 AM on the 8th in Los Angeles.
    # T-1 Rule: Minus 1 day for the end-of-day report = March 7th.
    metadata_la = {
        "show_name": "Hamilton", 
        "venue_name": "Pantages",
        "show_id": "100", "venue_id": "200", "document_id": "300",
        "timezone": "America/Los_Angeles"
    }
    filename_la = mock_engine.generate_filename(metadata_la, received_date_utc, ".xls")
    
    # Assert the date in the filename is the 7th
    assert "07_03_2026" in filename_la

    # --- TEST 3: Default Fallback (Missing Timezone) ---
    # If a rule forgets the timezone, it defaults to UTC.
    # 5:00 PM UTC on the 8th. T-1 Rule = March 7th.
    metadata_default = {
        "show_name": "Unknown", 
        "venue_name": "Default",
        "show_id": "0", "venue_id": "0", "document_id": "0"
        # No timezone provided!
    }
    filename_default = mock_engine.generate_filename(metadata_default, received_date_utc, ".xls")
    
    assert "07_03_2026" in filename_default
