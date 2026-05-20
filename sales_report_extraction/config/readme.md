# ⚙️ Configuration & Routing Rules

**Domain:** Orchestration & Data Mapping  
**Format:** JSON-Driven Medallion Pattern  

---

## 1. Overview

This directory contains the central configuration layer. All email routing, medallion mapping, and parser assignments are handled via declarative JSON rules.

---

## 2. Rule Structure (`show_reporting_rules.json`)

Each object in the `"rules"` array controls one unique data flow:

### 📊 Active Reporting Rules

| Show Name | Venue Name | Show ID | Venue ID | Doc ID | Timezone | Subject Keyword |
|---|---|---|---|---|---|---|
| Beauty & The Beast | Hull | 298 | 42 | 62 | Europe/London | Hull New Theatre - Beauty & The Beast |
| Devil Wears Prada | Dominion Theatre | 180 | 1 | 505 | Europe/London | TDWP Cumulative Sales |
| Goldilocks and the Three Bears | Aberdeen | 290 | 29 | 355 | Europe/London | Goldilocks and the 3 Bears Sales Report - Aberdeen |
| Jesus Christ Superstar | Singapore | 287 | 125 | 501 | Asia/Singapore | Jesus Christ Superstar - Sales Summary Report |
| Jesus Christ Superstar | Christchurch | 287 | 224 | 506 | Pacific/Auckland | JCSSalesReports |
| Jesus Christ Superstar | Manila | 287 | 220 | 504 | Asia/Singapore | Jesus Christ Superstar (Sales Summary) |
| Jesus Christ Superstar | Hong Kong | 287 | 221 | 509 | Asia/Singapore | Daily Ticket Report for GMG PRODUCTIONS |
| Silence of the Lambs Tour 2026 | Bournemouth | 285 | 51 | 341 | Europe/London | SotL 2026 Tour BOURNEMOUTH |
| Silence of the Lambs Tour 2026 | Malvern Theatres | 285 | 174 | 503 | Europe/London | Malvern Theatres Figures The Silence of the Lambs |
| The Little Mermaid | Grand Opera House Belfast | 291 | 30 | 81 | Europe/London | The Little Mermaid - Grand Opera House Belfast |
| Waitress Tour 2026 | Birmingham Hippodrome | 280 | 31 | 58 | Europe/London | Waitress 2026 Tour BIRMINGHAM |
| The Bodyguard 2025 | Dublin Bord Gais Energy Theatre | 233 | 41 | 71 | Europe/London | Daily Report: Sales Comparison - The Bodyguard |
| Jesus Christ Superstar | Auckland Civic Theatre | 287 | 222 | 71 | Pacific/Auckland | Daily Report: Sales Comparison - AKL JCS |
| Jesus Christ Superstar | Wellington St. James Theatre | 287 | 223 | 71 | Pacific/Auckland | Daily Report: Sales Comparison - WLG JCS |
| Waitress Tour 2026 | Dublin Bord Gais Energy Theatre | 280 | 41 | 71 | Europe/London | Daily Report: Sales Comparison - Waitress |
| Annie Tour 2026 | Dublin Bord Gais Energy Theatre | 279 | 41 | 71 | Europe/London | Daily Report: Sales Comparison - Annie |
| Mamma Mia! | Belfast SSE Arena | 234 | 218 | 71 | Europe/London | Daily Report: Sales Comparison - MM! Belfast |
| Silence of the Lambs Tour 2026 | Dublin Bord Gais Energy Theatre | 285 | 41 | 71 | Europe/London | Daily Report: Sales Comparison - SotL 2026 Tour |
| High Society Tour 2026 | Dublin Bord Gais Energy Theatre | 288 | 41 | 71 | Europe/London | Daily Report: Sales Comparison - High Society Tour 2026 |

### 📡 Match Criteria
- **`sender_domain`**: The verified source domain of the email.
- **`subject_keyword`**: String used for simplified, robust subject-only keyword search in the Graph API.
- **`attachment_type`**: Strict extension enforcement (e.g., `.pdf`, `.xls`).

### 🏷️ Metadata Mapping (Medallion)
- **`show_name`**, **`venue_name`**: Used for standard filename generation.
- **`show_id`**, **`venue_id`**, **`document_id`**: Identifiers for downstream systems.
- **`timezone`**: The exact IANA Time Zone (e.g., `Asia/Singapore`, `Europe/London`).

### 🌐 Deterministic Timezone Logic & Sales Day Offset
The engine uses the `timezone` and optional `sales_day_offset_hours` fields to perfectly align the UTC email receipt time with the venue's local reporting date:
1. **Sales Day Offset:** For venues with late-night performances, `sales_day_offset_hours` (e.g., `2`) is added to the arrival time. This pushes emails received in the early morning hours (e.g., 1 AM) into the "effective" next day before the date calculation.
2. **Local Conversion:** The (offset) UTC time is converted to the venue's local time (e.g., `Asia/Singapore`) using `pytz`.
3. **Standardization:** The system subtracts **1 day** from the local time because reports received today reflect yesterday's business.
4. **Consistency:** This ensures that reports from Singapore, London, and New York are all dated accurately relative to their own business days, regardless of the UTC offset or late-night arrival.

### 🧠 Processing Logic
- **`parser_module`**: The path to the Python parser script (e.g., `src.parsers.malvern_theatre_contractual_report_pdf_parser`).
- **`passthrough_only`**: If `true`, the raw attachment is moved directly to `processed/` for SFTP delivery without modification.
- **`needs_lookup`**: Indicates if the parser requires a local lookup CSV (e.g., for mapping seat categories).

---

## 3. Dynamic Backfilling & Idempotency

The orchestrator leverages server-side tags and client-side fingerprinting for state management:
1. **Rolling Window:** By default, the system scans for untagged emails received within the last **30 days**.
2. **Fingerprint Deduplication:** The unique `internetMessageId` is used to detect duplicate emails (twins) within the window. Duplicates are tagged as `"sales_report_duplicate"` and skipped.
3. **Stateless Logic:** Successful runs apply the `"sales_report_extracted"` tag.
4. **Failure Isolation:** Errors apply the `"sales_report_failed"` tag.
5. **Custom Runs:** Use `days_back`, `target_rule_name`, and `retry_failed` parameters in the Prefect UI for historical corrections.
