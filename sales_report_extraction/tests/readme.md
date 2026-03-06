# 🧪 Test Suite

**Domain:** Quality Assurance & Validation  
**Framework:** `pytest`  

---

## 1. Overview

This suite verifies client authentication, file processing logic, and date calculations without touching production data or external services.

---

## 2. How to Create a Test File

### Naming Convention
All test files must be named `test_*.py` and located in this directory to be automatically discovered by `pytest`.

### Basic Structure
```python
import pytest
from unittest.mock import patch, MagicMock

def test_my_feature():
    # Arrange
    expected = 10
    # Act
    actual = 5 + 5
    # Assert
    assert actual == expected
```

---

## 3. What to Patch (Mocking)

When writing tests, you must mock external dependencies to ensure isolation and avoid errors like `MissingContextError` from Prefect.

### Common Mocks
- **Prefect Logger:** Always patch `get_run_logger` to avoid context errors.
  ```python
  @patch('src.graph_client.get_run_logger')
  def test_something(mock_logger): ...
  ```
- **MSAL (Auth):** Mock `msal.ConfidentialClientApplication` to avoid real login attempts.
- **SFTP (Paramiko):** Mock `paramiko.SSHClient` and `SFTPClient` in `test_sftp_client.py`.
- **Global Config:** If a test needs specific config values, patch the `open` call or the `json.load` that reads `show_reporting_rules.json`.

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
3. This allows us to verify that the search filters and categorization logic work correctly without an internet connection.
