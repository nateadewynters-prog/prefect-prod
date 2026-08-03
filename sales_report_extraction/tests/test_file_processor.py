import os
import sys
import types
import pytest
from unittest.mock import patch
from datetime import timezone
from src.file_processor import ProcessingEngine
from src.models import ValidationResult

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
    
    expected_filename = "RAW.Phantom.West-End.Daily_100_200_300_26_02_2026.pdf"
    assert filename == expected_filename

def test_generate_filename_test_mode_prepends_prefix(mock_engine):
    """A test rule gains a TEST. prefix; nothing else about the name changes."""
    metadata = {
        "show_name": "Mamma Mia!",
        "venue_name": "Zurich",
        "show_id": "234",
        "venue_id": "100",
        "document_id": "486",
        "report_type": "Advance",
        "timezone": "Europe/Zurich"
    }

    received_date = "2026-07-16T08:00:00Z"

    live_name = mock_engine.generate_filename(metadata, received_date, ".xlsx")
    test_name = mock_engine.generate_filename(metadata, received_date, ".xlsx", True)

    assert live_name == "RAW.Mamma-Mia!.Zurich.Advance_234_100_486_15_07_2026.xlsx"
    assert test_name == "TEST.RAW.Mamma-Mia!.Zurich.Advance_234_100_486_15_07_2026.xlsx"
    # The prefix is the only difference between the two.
    assert test_name == f"TEST.{live_name}"


def test_generate_filename_defaults_to_live_name(mock_engine):
    """Omitting test_mode must produce the identical name it does today."""
    metadata = {
        "show_name": "Phantom",
        "venue_name": "West End",
        "show_id": "100",
        "venue_id": "200",
        "document_id": "300",
        "report_type": "Daily"
    }

    received_date = "2026-02-27T14:00:00Z"

    implicit = mock_engine.generate_filename(metadata, received_date, ".pdf")
    explicit = mock_engine.generate_filename(metadata, received_date, ".pdf", False)

    assert implicit == explicit == "RAW.Phantom.West-End.Daily_100_200_300_26_02_2026.pdf"
    assert not implicit.startswith("TEST.")


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
    assert filename_sg == "RAW.Jesus-Christ-Superstar.Singapore.SalesSummary_287_125_501_08_03_2026.xls"

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
    assert "RAW.Hamilton.Pantages.Cumulative_" in filename_la

    # --- TEST 3: Default Fallback ---
    metadata_default = {
        "show_name": "Unknown", 
        "venue_name": "Default",
        "show_id": "0", "venue_id": "0", "document_id": "0",
        "report_type": "Report" # 🚀 Injected required key
    }
    filename_default = mock_engine.generate_filename(metadata_default, received_date_utc, ".xls")
    
    assert "07_03_2026" in filename_default
    assert "RAW.Unknown.Default.Report_" in filename_default


# ---------------------------------------------------------------------------
# Stage markers: RAW. on the way in, PROCESSED. on the parsed output
# ---------------------------------------------------------------------------
@pytest.fixture
def stub_parser():
    """Register a throwaway parser module so importlib can resolve it."""
    module = types.ModuleType("stub_parser")
    module.extract = lambda path: (
        [{"seats": 10}],
        ValidationResult(status="PASSED", message="ok", metrics={}),
    )
    sys.modules["stub_parser"] = module
    yield module
    del sys.modules["stub_parser"]


def _write_raw(engine, name):
    """Drop a dummy raw file in the inbox, as process_email would have done."""
    path = os.path.join(engine.base_dir, engine.dirs['inbox'], name)
    with open(path, 'w') as f:
        f.write("dummy")
    return path


@pytest.mark.parametrize("raw_name,expected_output", [
    ("RAW.Phantom.West-End.Daily_100_200_300_26_02_2026.xlsx",
     "PROCESSED.Phantom.West-End.Daily_100_200_300_26_02_2026.csv"),
    ("TEST.RAW.Phantom.West-End.Daily_100_200_300_26_02_2026.xlsx",
     "TEST.PROCESSED.Phantom.West-End.Daily_100_200_300_26_02_2026.csv"),
])
def test_parsed_output_swaps_raw_for_processed(mock_engine, stub_parser, raw_name, expected_output):
    """A parser's output is PROCESSED.; any TEST. prefix stays out in front."""
    temp_path = _write_raw(mock_engine, raw_name)
    rule = {
        "processing": {"parser_module": "stub_parser", "parser_function": "extract"},
        "metadata": {"show_name": "Phantom", "venue_name": "West End"},
    }

    with patch('src.file_processor.get_run_logger'):
        _, _, output_path = mock_engine.process_file(temp_path, rule)

    assert os.path.basename(output_path) == expected_output
    # The raw file is archived under its own unchanged RAW. name.
    archive_path = os.path.join(
        mock_engine.base_dir, mock_engine.dirs['archive'], "Phantom", "West-End", raw_name
    )
    assert os.path.exists(archive_path)


def test_passthrough_keeps_the_raw_marker(mock_engine):
    """Passthrough files are never parsed, so they stay RAW. all the way to SFTP."""
    raw_name = "RAW.Mamma-Mia!.Zurich.Advance_234_100_486_15_07_2026.xlsx"
    temp_path = _write_raw(mock_engine, raw_name)
    rule = {
        "processing": {"passthrough_only": True},
        "metadata": {"show_name": "Mamma Mia!", "venue_name": "Zurich"},
    }

    with patch('src.file_processor.get_run_logger'):
        _, _, final_path = mock_engine.process_file(temp_path, rule)

    assert os.path.basename(final_path) == raw_name
