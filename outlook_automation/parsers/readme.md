# 🧠 Outlook Automation Parsers

**Domain:** Custom Extraction Logic  
**Architecture:** Pluggable Parser Modules  

> **System Map Reference:** For orchestrator instructions and data flow details, refer to the **Outlook README** at `/opt/prefect/prod/code/outlook_automation/readme.md`.

---

## 1. Overview

This directory contains the highly specialized extraction modules used by the Medallion Email Extraction pipeline. Instead of relying on rigid hardcoded structures, the `email_extraction_flow.py` orchestrator dynamically invokes these scripts using Python's `importlib` based on rules defined in the `show_reporting_rules.json` configuration file.

Each parser serves a singular purpose: accurately unpacking data from complex, unstructured files provided by external vendors.

---

## 2. Extraction Modalities

Our parsers primarily handle two types of attachments:

### A. PDF Extraction (`pdfplumber`)
Used for unpacking human-readable contractual reports and summaries.
- Leverages the robust `pdfplumber` library to perform spatial mapping and regex-based row matching.
- Extracts tabular data that lacks traditional delimiters.
- Evaluates built-in summary blocks to verify calculations (e.g. comparing the sum of extracted rows against the stated total gross).
- Example: `malvern_theatre_contractual_report_pdf_parser.py`.

### B. Excel Extraction (`pandas` grid-scan)
Used for vendor settlement grids which do not follow standard DataFrame geometries.
- Utilizes `pandas` to open unstructured XLS/XLSX workbooks.
- Employs grid-scanning algorithms to identify header coordinates and data blocks.
- Accounts for variable empty columns, unexpected spacing, and merged cells inherent to human-edited sheets.
- Example: `ticketek_event_settlement_excel_parser.py`.

---

## 3. The Validation Data Contract

Every parser in this directory **MUST** return a specific tuple to the orchestrator:

```python
return extracted_rows, validation_result
```

`extracted_rows`: A List of Dictionaries or a Pandas DataFrame representing the cleaned data.
`validation_result`: A `ValidationResult` object (from `utils.py`) enforcing the strict observability contract.

### Contract Status Levels

When developing or modifying a parser, you must assign an explicit validation state:

- `PASSED`: Internal sums perfectly match the report's stated summary totals. (Proceeds silently).
- `FAILED`: Hard schema mismatch or unrecoverable error during extraction. (Halts processing, moves file to `failed/`, triggers Teams ❌).
- `UNVALIDATED`: Extraction completed, but the parser could not locate a summary total to verify against, or specific constraints were breached. (Creates output but triggers Teams ⚠️ warning for manual review).

---

## 4. Development Standards

When contributing new parsers:

1. **Isolation:** Keep parser logic entirely self-contained. Do not rely on external API calls within the parser.
2. **Schema Enforcement:** Use an `EXPECTED_SCHEMA` set at the top of the file to violently reject format changes by the vendor.
3. **Currency / Int Parsing:** Implement specialized helper functions (e.g. `parse_currency`) to handle anomalies like `£`, `,`, and trailing spaces before inserting values into the final dictionary.
4. **Resiliency:** Wrap primary extraction logic in strict `try/except` blocks, ensuring that `ValidationResult(status="FAILED")` is accurately propagated up.