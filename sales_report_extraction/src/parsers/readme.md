# 🧠 Sales Report Parsers

**Domain:** Custom Extraction Logic  
**Architecture:** Observable ETL / Pluggable Parser Modules  

---

## 1. Overview

This directory contains specialized extraction modules. The orchestrator dynamically invokes these scripts based on rules defined in the SharePoint rules list and loaded via `src/config_loader.py`.

---

## 2. Active Parsers

The following parsers are currently implemented and active:

- **`chicago_tour_lugano_parser.py`**: Parser for Chicago Tour 2025 (LAC — Sala Teatro, Lugano) XLSX reports in Italian locale, terminated by a `Totale` row. Locates the header row by name and reads every column through a name-to-index map, so the table can move down the sheet or have its columns reordered without breaking. Maps `Tickets Sold` from Paid Tickets rather than Total Tickets, because the source defines Total as Paid + Comps and comps are emitted separately.
- **`gmg_hk_jesus_christ_superstar_xlsx_parser.py`**: Specialized parser for GMG Hong Kong reports (XLSX inside ZIP).
- **`malvern_theatre_contractual_report_pdf_parser.py`**: Extracts contractual data from Malvern Theatre PDF reports.
- **`nederlandaer_devil_wears_prada_cumulative_extraction_pdf.py`**: Specialized cumulative extractor for "The Devil Wears Prada" reports from Nederlandaer.
- **`taiwan_jesus_christ_superstar_xlsx_parser.py`**: Specialized bilingual (English/Mandarin) parser for Taiwan's Jesus Christ Superstar XLSX reports, validated against the 合計 grand-total row.
- **`the_bodyguard_bulgaria_parser.py`**: Parser for The Bodyguard (Bulgaria) `play_details` exports, which arrive as `.xls` but are really HTML tables (read via `read_html`). Declares `OUTPUT_EXT = ".xlsx"`.
- **`the_bodyguard_tour_bulgaria_sofia.py`**: Parser for The Bodyguard 2025 (Sofia) reports. Also an HTML table saved with an `.xls` extension, but parsed directly with BeautifulSoup (`html.parser`, matching `link_extractor.py`) because `openpyxl` and `xlrd` both reject the file. Flattens the two-tier header into combined names (`Sales tickets`, `Reservations Price`, …) and reads every value by name; totals are marked `Σύνολα`.
- **`ticketek_event_settlement_excel_parser.py`**: Robust Excel parser for Ticketek settlement reports, supporting complex lookups.

---

## 3. The Validation Data Contract (Observable ETL)

To maintain high observability across the medallion pipeline, every parser **MUST** return a specific tuple:
```python
return extracted_rows, validation_result
```

- `extracted_rows`: A List of Dictionaries or a Pandas DataFrame.
- `validation_result`: A `ValidationResult` object (defined in `src.models`).

A parser module may also declare an optional module-level `OUTPUT_EXT` constant (e.g. `".xlsx"`) when the contractor needs a specific processed-file format. Parsers that omit it still get a `.csv`.

### Contract Status Levels
- `PASSED`: Data is verified and ready for delivery.
- `FAILED`: Hard schema mismatch (triggers ❌ Teams alert via the orchestrator).
- `UNVALIDATED`: Extraction completed but couldn't be mathematically verified (triggers ⚠️ Teams alert via the orchestrator).

This contract ensures that even if a parser succeeds in extracting data, it can proactively flag data quality issues before they reach the Sales Database.
