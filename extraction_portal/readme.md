# 📂 Extraction Portal (Streamlit)

**Host:** `DEW-DBSYNC01`
**Status:** 🟢 Active
**Tech Stack:** Python 3.13, Streamlit, Pandas, PDFPlumber, XLRD
**Context:** Internal tool to automate manual data entry from "messy" PDF and Excel reports.

---

## 🚀 Quick Start

### 1. Activate Environment
Open PowerShell as Administrator (to ensure write permissions) and run:
```powershell
cd C:\Prefect\extraction_portal
..\venv\Scripts\Activate.ps1

```

### 2. Run the Portal

```powershell
streamlit run app.py

```

*Access the UI at:* `http://localhost:8501`

---

## 🧠 How It Works (Plain English)

This tool is designed to "read" reports just like a human analyst would, rather than treating them like standard database files.

### 1. The "Contractual Report" (PDF)

Imagine highlighting rows on a printed piece of paper with a marker.

* **Scanning:** The tool scans down the page looking for a specific pattern: *Day -> Date -> Time -> Show Name -> Numbers*.
* **Smart Reading:** It knows that "Show Names" can be short (*"Cats"*) or long (*"The Curious Incident..."*). It reads until it sees a large gap of white space (2+ spaces), then knows the next part is the "Capacity" number.
* **The "Sanity Check":** At the end, it adds up all the tickets it found. It compares this number to the "Total Sold" written at the bottom of the PDF. If they don't match exactly, it warns you.

### 2. The "Event Settlement" (Excel)

These files are messy grids, not simple tables.

* **Block Detection:** The tool looks for the phrase **"Summary Totals"**. When it finds it, it knows it has hit a chunk of data.
* **Looking Up:** From "Summary Totals", it looks *upwards* (backwards) to find the **Event Code** (e.g., `EPTT_2MAY...`).
* **Looking Down:** It then looks *downwards* inside that block to find the "Value Total" (Paid Tickets) and "Comp Total" (Free Tickets).
* **The "Sanity Check":** Just like the PDF, it compares the sum of all events it found against the "Event Span Total" at the very bottom of the sheet to ensure nothing was missed.

---

## 📂 Directory Structure

```text
C:\Prefect\extraction_portal\
├── app.py                  # Main UI & Router (Disk-first processing)
├── parsers\                # Logic modules
│   ├── __init__.py
│   ├── pdf_contractual.py  # Logic for "Contractual Report" PDFs
│   └── xls_settlement.py   # Logic for "Event Settlement" Grids
├── temp\                   # Temporary storage (Auto-cleared)
└── README.md               # This file

```

---

## 🛠️ Technical Deep Dive

This section details the specific algorithms used to parse the unstructured data.

### 1. PDF Contractual Reports (`pdf_contractual.py`)

This parser does **not** use coordinate-based extraction (which breaks if the layout shifts). Instead, it uses **Regex Pattern Matching** on the text stream.

#### The Regex Logic

The script iterates through every line of text extracted by `pdfplumber` and applies this pattern:

```regex
^\s*(?P<day>\w+)\s+                 # 1. Day Name (e.g., "Monday")
(?P<date>\d+\s+\w+\s+\d+)\s+        # 2. Date (e.g., "26 January 27")
(?P<time>\d{2}:\d{2})\s+            # 3. Time (e.g., "19:30")
(?P<prod>.*?)\s{2,}                 # 4. Production Name (Lazy match until...)
(?P<cap>[\d,]+)\s+                  # 5. Capacity (Gap of 2+ spaces required)

```

* **Key Mechanism:** The `\s{2,}` (two or more spaces) check is critical. It differentiates between spaces *inside* a show title (e.g., "The Lion King") and the gap *after* the title before the numbers start.

#### Verification

* **Internal Sum:** `calc_total_sold += row['Sold']`
* **External Reference:** Scans footer for `Summary Pattern: ^\s*\d+\s+Performances...`
* **Logic:** `if calc_total_sold != report_total_sold: raise Warning`

---

### 2. Event Settlement Grids (`xls_settlement.py`)

This parser handles Excel files that are formatted as **visual reports** (grids) rather than database tables. It reads the file with `header=None` to treat it as a matrix of raw cells.

#### The "Anchor & Scan" Algorithm

1. **Iterate:** Loop through every row in the spreadsheet.
2. **Anchor:** Stop when `row[0] == "Summary Totals"`. This indicates the start of a data block.
3. **Scan Up (Backwards Search):**
* From the "Summary Totals" row, the script looks at `row[i-1]`, `row[i-2]`, etc., (up to 5 rows).
* It captures the first non-empty cell as the **Event Code** (e.g., `EPTT_2MAY2026`).
* *Why?* Because inconsistent spacing often exists between the header and the data block.


4. **Scan Down (Data Extraction):**
* Locate the column index of the word "Total" in the current row.
* Find "Value Total" row -> grab cell at `[Total_Column_Index]`.
* Find "Comp Total" row -> grab cell at `[Total_Column_Index]`.


5. **Close Block:** The block is considered "finished" when the script encounters a row starting with the current *Event Code* again (the footer of that specific event).

#### Visualizing the "Grid Scan"

| Row | Col A | ... | Col H (Total) | Action Taken |
| --- | --- | --- | --- | --- |
| **10** | `EPTT_2MAY...` | ... | ... | *Found Event Code (Scan Up)* |
| **11** | (Empty) | ... | ... |  |
| **12** | **Summary Totals** | ... | **Total** | *ANCHOR FOUND* |
| **13** | Value Total | ... | **150** | *Extract Paid Tickets* |
| **14** | Comp Total | ... | **10** | *Extract Comps* |
| **15** | `EPTT_2MAY... Total` | ... | 160 | *Close Block* |

---

## ⚠️ Infrastructure & Constraints

### Memory Management (760MB RAM)

The VM has very limited RAM. We use specific strategies to prevent OOM (Out of Memory) crashes:

1. **Disk-First Processing:** Uploaded files are immediately saved to `C:\Prefect\extraction_portal\temp\` and read from disk, rather than keeping the file buffer in RAM.
2. **Aggressive Garbage Collection:** `gc.collect()` is forced after every extraction run.
3. **Engine Selection:**
* `.xls` uses `xlrd` (lighter weight).
* `.xlsx` uses `openpyxl`.



### "Access Denied" / Permission Errors

If you see `[WinError 5] Access is denied` when installing packages:

1. Close all running Python/Streamlit processes.
2. Run PowerShell as **Administrator**.
3. Ensure `batchuser` (or the service user) has **Full Control** over `C:\Prefect\venv`.

```

```