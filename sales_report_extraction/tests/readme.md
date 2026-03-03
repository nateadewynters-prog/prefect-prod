# 🧪 Test Suite

**Domain:** Quality Assurance & Validation  
**Framework:** `pytest`  

> **System Map Reference:** For orchestrator instructions and data flow details, refer to the **Sales Extraction README** at `/opt/prefect/prod/code/sales_report_extraction/readme.md`.

---

## 1. Overview

This project uses an isolated test suite to verify client authentication, file processing logic, and business calculations (like T-1 date math) without touching production data or external services.

---

## 2. Test Structure

### Isolated Fixtures
The suite uses **`pytest.fixture`** with `tmp_path` to create entirely fresh environments for each run. This ensures that:
- Files are written to temporary folders, never to `/opt/prefect/prod/code/data`.
- History logs are generated on-the-fly.

### Mocking External APIs
We use `unittest.mock` to simulate external environments:
- **`test_graph_client.py`**: Mocks MSAL authentication and Graph API responses to verify token handling and pagination.
- **`test_sftp_client.py`**: Mocks `paramiko` to ensure correct connection parameters and upload paths.

### Business Logic Validation
- **`test_file_processor.py`**: Verifies the "T-1" date subtraction logic and standardized filename formatting.

---

## 3. How to Run Tests

To execute the tests with absolute import support:

```bash
# Navigate to the project root
cd /opt/prefect/prod/code/sales_report_extraction

# Run pytest with the current directory in the PYTHONPATH
export PYTHONPATH=$(pwd) && pytest tests/ -v
```

All new features or parser updates should include a corresponding test case within this directory.
