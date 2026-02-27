import os
import pytest
from datetime import timezone
from outlook_app.core.file_processor import ProcessingEngine

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
        },
        "history_file": "data/processed_ids.txt"
    }
    config_path = str(tmp_path / "fake_config.json")
    
    return ProcessingEngine(global_config=global_config, config_path=config_path)

# 2. Test Initialization
def test_ensure_directories(mock_engine, tmp_path):
    """Test that initialization successfully creates all required directories."""
    assert os.path.exists(tmp_path / "data" / "inbox")
    assert os.path.exists(tmp_path / "data" / "processed")
    assert os.path.exists(tmp_path / "data" / "processed_ids.txt")

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
    expected_filename = "Phantom.West-End.100_200_300_26_02_2026.pdf"
    assert filename == expected_filename

# 4. Test File I/O (State Management)
def test_save_and_load_processed_ids(mock_engine):
    """Test the history log read/write operations in the temporary directory."""
    
    # Save a couple of fake message IDs
    mock_engine.save_processed_id("MSG_123", "RULE_A")
    mock_engine.save_processed_id("MSG_456", "RULE_B")
    
    # Load them back into memory
    processed_set = mock_engine.load_processed_ids()
    
    # Assert they exist
    assert "MSG_123" in processed_set
    assert "MSG_456" in processed_set
    assert "MSG_999" not in processed_set
