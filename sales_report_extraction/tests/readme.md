# 🧪 Test Suite

**Domain:** Quality Assurance & Validation  
**Framework:** `pytest`  

---

## 1. Overview

This suite verifies client authentication, file processing logic, server-side tagging, and **dynamic backfill parameters** without touching production data or external services (via extensive mocking).

---

## 2. Key Test Areas

- **Rule Configuration:** `test_config_loader.py` verifies `SharePointRuleLoader`'s rule-name generation, parser-vs-passthrough classification, half-configured rule skipping, inactive-rule handling, ID coercion, pagination, and list-not-found errors, fully mocked against `requests.get`.
- **Graph API Tagging:** `test_categorization.py` verifies the ability to search for, apply, and remove category tags (`sales_report_extracted`, `sales_report_failed`, `sales_report_duplicate`), ensuring idempotency and retry capability.
- **File Processing:** `test_file_processor.py` tests directory setup and deterministic filename generation, including timezone and Sales Day Offset logic.
- **Dynamic Orchestration:** `test_main.py` validates that routing pulls its rules from the SharePoint loader (not the JSON config) and that already-categorized emails are skipped during fetch.
- **Failure Resilience:** Specifically, `test_process_email_handles_lookup_failure_and_tags_failed` verifies that mapping errors (e.g., missing lookups) result in the `"sales_report_failed"` tag. Teams alerts are now managed by the orchestrator.
- **SFTP Integration:** `test_sftp_client.py` ensures that files are correctly handled and uploaded. Internal Teams notifications have been removed in favor of bubbling exceptions.

---

## 3. What to Patch (Mocking)

When writing tests, you must mock external dependencies to ensure isolation and avoid errors like `MissingContextError` from Prefect.

### Common Mocks
- **Prefect Logger:** Always patch `get_run_logger` to avoid context errors.
  ```python
  @patch('src.graph_client.get_run_logger')
  def test_something(mock_logger): ...
  ```
- **`os.fsync`**: Mocked to prevent `"Bad file descriptor"` errors when testing file-writing logic with in-memory `mock_open`.
- **MSAL (Auth):** Mock `msal.ConfidentialClientApplication` to avoid real login attempts.
- **SFTP (Paramiko):** Mock `paramiko.SSHClient` and `SFTPClient` in `test_sftp_client.py`.
- **Global Config:** For rule data, patch `src.config_loader.requests.get` and stub `SharePointRuleLoader._get_token` (see `test_config_loader.py`), or patch `main.rule_loader` in flow-level tests. `show_reporting_rules.json` now only supplies `global_settings` (paths).

---

## 4. How to Run Tests via Docker

To ensure the tests run in the exact environment used by production, execute them inside the running container.

### Run the full suite:
```bash
sudo docker exec -it prefect-sales-extraction pytest tests/
```

### Run a specific test file:
```bash
sudo docker exec -it prefect-sales-extraction pytest tests/test_graph_client.py
```

### Run with verbose output:
```bash
sudo docker exec -it prefect-sales-extraction pytest tests/ -v
```

---

## 5. Mocking Strategy Example
The `test_graph_client.py` uses `unittest.mock.patch` to simulate API responses:
1. It mocks the MSAL token acquisition.
2. It mocks `requests.get` to return a JSON payload simulating a list of emails.
3. This allows us to verify that the **subject-only search** and **Python-side sender validation** work correctly without an internet connection.
