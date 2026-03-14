# 🧠 Sales Report Parsers

**Domain:** Custom Extraction Logic  
**Architecture:** Observable ETL / Pluggable Parser Modules  

---

## 1. Overview

This directory contains specialized extraction modules. The orchestrator dynamically invokes these scripts based on rules defined in `config/show_reporting_rules.json`.

---

## 2. Active Parsers

The following parsers are currently implemented and active:

- **`malvern_theatre_contractual_report_pdf_parser.py`**: Extracts contractual data from Malvern Theatre PDF reports.
- **`nederlandaer_devil_wears_prada_cumulative_extraction_pdf.py`**: Specialized cumulative extractor for "The Devil Wears Prada" reports from Nederlandaer.
- **`ticketek_event_settlement_excel_parser.py`**: Robust Excel parser for Ticketek settlement reports, supporting complex lookups.

---

## 3. The Validation Data Contract (Observable ETL)

To maintain high observability across the medallion pipeline, every parser **MUST** return a specific tuple:
```python
return extracted_rows, validation_result
```

- `extracted_rows`: A List of Dictionaries or a Pandas DataFrame.
- `validation_result`: A `ValidationResult` object (defined in `src.models`).

### Contract Status Levels
- `PASSED`: Data is verified and ready for delivery.
- `FAILED`: Hard schema mismatch (triggers ❌ Teams alert via the orchestrator).
- `UNVALIDATED`: Extraction completed but couldn't be mathematically verified (triggers ⚠️ Teams alert via the orchestrator).

This contract ensures that even if a parser succeeds in extracting data, it can proactively flag data quality issues before they reach the Sales Database.
