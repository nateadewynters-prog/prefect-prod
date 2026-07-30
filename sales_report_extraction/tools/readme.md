# 🔧 Manual Tools

**Domain:** Diagnostics & one-off operations

---

## 1. Overview

Scripts you run by hand against the live environment. Nothing in here asserts anything, so
nothing in here is a test — that is why these files live outside `tests/`. Both used to sit in
the test suite with entry points named `run_test()` and `get_sharepoint_tree()`, which `pytest`
never collects, so they read as coverage while never actually running.

Run them from the service root so that `src` is importable:

```bash
cd /opt/prefect/prod/code/sales_report_extraction
python tools/print_sharepoint_tree.py
```

Inside the container `PYTHONPATH` is already set:

```bash
sudo docker exec -it prefect-sales-extraction python tools/print_sharepoint_tree.py
```

---

## 2. The Tools

- **`print_sharepoint_tree.py`** — prints the folder tree of the Sales Reporting document library,
  three levels deep. Read-only, but authenticates with the production credentials in `.env`.

- **`tag_emails_manually.py`** — searches the real mailbox for the latest Malvern emails, applies
  `sales_report_extracted`, and reads the categories back to confirm they stuck.

> ⚠️ **`tag_emails_manually.py` can silently lose a sales report.** `GraphClient.tag_email`
> overwrites the entire `categories` list with a single entry, so any category already on the
> email is destroyed. `main.py` then treats `sales_report_extracted` as a **permanent** skip — an
> email tagged by this tool is never picked up again. If you point it at a report the pipeline has
> not ingested yet, that report is never collected, with no error and no Teams alert.
>
> Capture each message's `categories` with a read-only `GET` before running it, and PATCH them
> back afterwards.
