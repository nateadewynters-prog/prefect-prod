# 🧪 Test Suite

**Domain:** Quality Assurance & Validation  
**Framework:** `pytest`  

---

## 1. Overview

This suite verifies client authentication, file processing logic, server-side tagging, and **dynamic backfill parameters**. Most tests use extensive mocking and touch nothing external — but a handful are **live integration tests**, see the warning in section 4 before running the suite.

---

## 2. Key Test Areas

- **Rule Configuration:** `test_config_loader.py` verifies `SharePointRuleLoader`'s rule-name generation, parser-vs-passthrough classification, half-configured rule skipping, inactive-rule handling, ID coercion, pagination, and list-not-found errors, fully mocked against `requests.get`.
- **Graph API Tagging:** `test_categorization.py` is a **live** manual script (run it directly with `python`) that searches the real mailbox, applies `sales_report_extracted` to the emails it finds, and reads the categories back to confirm they stuck. Its entry point is `run_test()`, not a `test_*` function, so `pytest` does not collect it.
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

> ⚠️ **WARNING — a bare `pytest tests/` fires live integration tests.** Three collected tests carry no pytest markers and use the production `.env` credentials: `test_alert.py` (posts a real card to the Teams Dev channel), `test_sales_reporting_sharepoint_connection.py` (authenticates against real Microsoft Graph) and `test_sharepoint_upload.py` (uploads a real dummy file to SharePoint). To stay fully offline, deselect them:
> ```bash
> pytest tests/ --ignore=tests/test_alert.py --ignore=tests/test_sales_reporting_sharepoint_connection.py --ignore=tests/test_sharepoint_upload.py
> ```
> `test_sales_reporting_sharepoint_tree.py` is also live but is not collected (its entry point is `get_sharepoint_tree()`).

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
