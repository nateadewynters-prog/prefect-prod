# 🧠 Sales Report Parsers

**Domain:** Custom Extraction Logic  
**Architecture:** Observable ETL / Pluggable Parser Modules  

---

## 1. Overview

This directory contains specialized extraction modules. The orchestrator dynamically invokes these scripts based on rules defined in `config/show_reporting_rules.json`.

---

## 2. The Validation Data Contract (Observable ETL)

To maintain high observability across the medallion pipeline, every parser **MUST** return a specific tuple:
```python
return extracted_rows, validation_result
```

- `extracted_rows`: A List of Dictionaries or a Pandas DataFrame.
- `validation_result`: A `ValidationResult` object (defined in `src.models`).

### Contract Status Levels
- `PASSED`: Data is verified and ready for delivery.
- `FAILED`: Hard schema mismatch (triggers ❌ Teams alert).
- `UNVALIDATED`: Extraction completed but couldn't be mathematically verified (triggers ⚠️ Teams alert).

This contract ensures that even if a parser succeeds in extracting data, it can proactively flag data quality issues before they reach the Sales Database.
