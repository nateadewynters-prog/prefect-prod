import os
import pytest
from datetime import timezone
from src.file_processor import ProcessingEngine

@pytest.fixture
def mock_engine(tmp_path):
    global_config = {
        "base_dir": str(tmp_path),
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

def test_ensure_directories(mock_engine, tmp_path):
    assert os.path.exists(tmp_path / "data" / "inbox")
    assert os.path.exists(tmp_path / "data" / "processed")
    assert os.path.exists(tmp_path / "data" / "archive")

def test_generate_filename(mock_engine):
    metadata = {
        "show_name": "Phantom",
        "venue_name": "West End",
        "show_id": "100",
        "venue_id": "200",
        "document_id": "300",
        "report_type": "Daily" # 🚀 Injected required key
    }
    
    received_date = "2026-02-27T14:00:00Z"
    filename = mock_engine.generate_filename(metadata, received_date, ".pdf")
    
    expected_filename = "Phantom.West-End.Daily_100_200_300_26_02_2026.pdf"
    assert filename == expected_filename

def test_generate_filename_with_deterministic_timezones(mock_engine):
    received_date_utc = "2026-03-08T17:00:00Z"
    
    # --- TEST 1: Singapore (UTC+8) ---
    metadata_sg = {
        "show_name": "Jesus Christ Superstar", 
        "venue_name": "Singapore",
        "show_id": "287", "venue_id": "125", "document_id": "501",
        "timezone": "Asia/Singapore",
        "report_type": "SalesSummary" # 🚀 Injected required key
    }
    filename_sg = mock_engine.generate_filename(metadata_sg, received_date_utc, ".xls")
    
    assert "08_03_2026" in filename_sg
    assert filename_sg == "Jesus-Christ-Superstar.Singapore.SalesSummary_287_125_501_08_03_2026.xls"

    # --- TEST 2: Los Angeles (UTC-8) ---
    metadata_la = {
        "show_name": "Hamilton", 
        "venue_name": "Pantages",
        "show_id": "100", "venue_id": "200", "document_id": "300",
        "timezone": "America/Los_Angeles",
        "report_type": "Cumulative" # 🚀 Injected required key
    }
    filename_la = mock_engine.generate_filename(metadata_la, received_date_utc, ".xls")
    
    assert "07_03_2026" in filename_la
    assert "Hamilton.Pantages.Cumulative_" in filename_la

    # --- TEST 3: Default Fallback ---
    metadata_default = {
        "show_name": "Unknown", 
        "venue_name": "Default",
        "show_id": "0", "venue_id": "0", "document_id": "0",
        "report_type": "Report" # 🚀 Injected required key
    }
    filename_default = mock_engine.generate_filename(metadata_default, received_date_utc, ".xls")
    
    assert "07_03_2026" in filename_default
    assert "Unknown.Default.Report_" in filename_default
