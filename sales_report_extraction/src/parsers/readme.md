# 🧠 Sales Report Parsers

**Domain:** Custom Extraction Logic  
**Architecture:** Pluggable Parser Modules  

---

## 1. Overview

This directory contains specialized extraction modules. The orchestrator dynamically invokes these scripts based on rules defined in `config/show_reporting_rules.json`.

---

## 2. The Validation Data Contract

Every parser **MUST** return a specific tuple:
```python
return extracted_rows, validation_result
```

- `extracted_rows`: A List of Dictionaries or a Pandas DataFrame.
- `validation_result`: A `ValidationResult` object (defined in `src.models`).

### Contract Status Levels
- `PASSED`: Data is verified and ready for delivery.
- `FAILED`: Hard schema mismatch (triggers ❌ Teams alert).
- `UNVALIDATED`: Extraction completed but couldn't be mathematically verified (triggers ⚠️ Teams alert).

---

## 3. Adding a New Parser

1. **Create the script:** e.g., `new_vendor_parser.py` in this directory.
2. **Implement logic:** Ensure it accepts a file path and returns the required tuple.
3. **Update Config:** Add a rule to `show_reporting_rules.json` pointing `parser_module` to `src.parsers.new_vendor_parser`.
