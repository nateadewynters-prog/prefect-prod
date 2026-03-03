# 🧠 Sales Report Parsers

**Domain:** Custom Extraction Logic  
**Architecture:** Pluggable Parser Modules  

> **System Map Reference:** For orchestrator instructions and data flow details, refer to the **Sales Extraction README** at `/opt/prefect/prod/code/sales_report_extraction/readme.md`.

---

## 1. Overview

This directory contains specialized extraction modules used by the Medallion Email Extraction pipeline. The orchestrator dynamically invokes these scripts using Python's `importlib` based on rules defined in the `show_reporting_rules.json` configuration file.

---

## 2. Extraction Modalities

### A. PDF Extraction (`pdfplumber`)
Used for unpacking human-readable contractual reports. It leverages spatial mapping and regex-based row matching to extract tabular data from unstructured PDFs.

### B. Excel Extraction (`pandas` grid-scan)
Used for vendor settlement grids. It employs grid-scanning algorithms to identify header coordinates and data blocks, accounting for variable spacing and merged cells.

---

## 3. The Validation Data Contract

Every parser in this directory **MUST** return a specific tuple to the orchestrator:

```python
return extracted_rows, validation_result
```

- `extracted_rows`: A List of Dictionaries or a Pandas DataFrame representing the cleaned data.
- `validation_result`: A `ValidationResult` object enforcing the strict observability contract.

### Import Path
The validation contract must be imported as:
```python
from src.models import ValidationResult
```

### Contract Status Levels
- `PASSED`: Internal sums match report totals.
- `FAILED`: Hard schema mismatch or unrecoverable error (triggers Red ❌ Teams alert).
- `UNVALIDATED`: Extraction completed but couldn't be verified (triggers Warning ⚠️ Teams alert).

---

## 4. Development Standards

1. **Isolation:** Keep parser logic entirely self-contained.
2. **Schema Enforcement:** Use an `EXPECTED_SCHEMA` to violently reject format changes.
3. **Resiliency:** Wrap extraction logic in strict `try/except` blocks to accurately propagate `FAILED` statuses.

---

## 5. Cheat Sheet: Adding a New Vendor Parser

Follow these steps to integrate a new vendor into the automation pipeline:

* **Step 1:** Create `new_vendor_parser.py` inside `src/parsers/`. Wrap the main function in a Prefect `@task` and ensure it returns a tuple: `(extracted_rows, ValidationResult)`.
* **Step 2:** Open `config/show_reporting_rules.json`.
* **Step 3:** Add a new JSON object to the `"rules"` array. 
* **Step 4:** Define the `"match_criteria"` (sender domain, subject keyword, extension).
* **Step 5:** Define the Medallion `"metadata"` (show_id, venue_id, etc.).
* **Step 6:** Set the `"processing"` block. Point `"parser_module"` to `"src.parsers.new_vendor_parser"` and set `"parser_function"` to the exact name of your Python function.
