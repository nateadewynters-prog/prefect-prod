"""
services/sharepoint.py — archiving the exported PDF to SharePoint.

A dispatch already exports a PDF from Power BI to render the email preview
(pipeline.py step 4) and then throws it away. This module keeps a copy, filed
one folder per show:

    PDF Reports / Media PDF Reports / <Show Name> / <YYYY-MM-DD> - <Show Name>.pdf

It writes to the same SharePoint site the sales extraction uses, and reuses
the Graph token the dispatch already holds for sendMail — so there is no
second MSAL client and no new credential to manage.

Two Graph quirks drive the shape of this file:

  * Graph will NOT create missing parent folders for you, so we walk the path
    and create each level in turn. A 409 back just means a previous run (or
    someone in the browser) already made it, which is success for us.
  * The simple `PUT .../:/content` upload is capped at 4 MB, and a multi-page
    Power BI PDF can exceed that. We always use an upload session instead:
    one code path, no size cliff to get caught out by later.
"""

from urllib.parse import quote

import requests

from config import SHAREPOINT_SITE_ID, SHAREPOINT_PDF_PATH

GRAPH = "https://graph.microsoft.com/v1.0"

# Characters SharePoint refuses in a file or folder name.
_ILLEGAL = '"*:<>?/\\|'

# Upload session chunk size. Graph requires a multiple of 320 KiB; 5 MB is 16
# of them and comfortably covers a whole report in one go.
_CHUNK = 5 * 1024 * 1024

# (connect, read) timeouts. The read side is generous on chunk PUTs because a
# slow link plus a big report shouldn't abandon a half-finished upload.
_TIMEOUT = (10, 60)
_UPLOAD_TIMEOUT = (10, 300)


def sanitize(name: str) -> str:
    """Swap the characters SharePoint won't accept in a name for "-".

    Mirrors _sanitize_name in sales_report_extraction/src/sharepoint_uploader.py
    — replacing rather than deleting — so a show like "Frozen: The Musical"
    lands under the same folder name in both tools. Mapping "/" and "\\" here
    also means a show name can never add extra path depth.
    """
    for char in _ILLEGAL:
        name = name.replace(char, "-")
    return name.strip()


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _drive(path: str = "") -> str:
    """Graph URL for a path inside the site's default document library.

    An empty path means the library root, which Graph addresses differently
    from a named folder (`/root/` rather than `/root:/<path>:/`).
    """
    base = f"{GRAPH}/sites/{SHAREPOINT_SITE_ID}/drive/root"
    return base if not path else f"{base}:/{quote(path)}:"


def _ensure_folder(token: str, parent: str, name: str) -> str:
    """Create `name` inside `parent`, and return the new folder's path.

    Idempotent: "already there" is the normal case after the first run.
    """
    resp = requests.post(
        f"{_drive(parent)}/children",
        headers=_headers(token),
        json={"name": name, "folder": {},
              # "fail" rather than "rename", so a clash tells us the folder is
              # already there instead of quietly making "Show Name 1".
              "@microsoft.graph.conflictBehavior": "fail"},
        timeout=_TIMEOUT,
    )
    if resp.status_code != 409:  # 409 == it exists already, which is fine
        resp.raise_for_status()
    return f"{parent}/{name}" if parent else name


def _upload(token: str, path: str, data: bytes, log=None) -> str:
    """Upload `data` to `path` via an upload session; return its webUrl."""
    resp = requests.post(
        f"{_drive(path)}/createUploadSession",
        headers=_headers(token),
        # "replace" so re-running the same show on the same day overwrites its
        # own backup rather than leaving a trail of near-identical copies.
        json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    upload_url = resp.json()["uploadUrl"]

    total = len(data)
    resp = None
    for start in range(0, total, _CHUNK):
        chunk = data[start:start + _CHUNK]
        end = start + len(chunk) - 1
        # The session URL carries its own short-lived credential in the query
        # string, so it must NOT get the Authorization header.
        resp = requests.put(
            upload_url, data=chunk, timeout=_UPLOAD_TIMEOUT,
            headers={"Content-Length": str(len(chunk)),
                     "Content-Range": f"bytes {start}-{end}/{total}"},
        )
        resp.raise_for_status()
        if log:
            log(f"⏳ Uploaded {min(end + 1, total):,} / {total:,} bytes...")

    # Intermediate chunks come back 202 with no body; the last one returns the
    # finished item. A single-chunk upload takes the same path.
    return resp.json().get("webUrl", "")


def backup_pdf(token: str, show_name: str, show_code: str, pdf_bytes: bytes,
               run_date, date_range: str, frequency: str = "weekly",
               log=None) -> str:
    """File one report PDF under its show's folder. Returns the SharePoint URL.

    Raises on any failure — the caller decides whether that's fatal. For a
    dispatch it isn't: the archive copy must never stop the report going out.
    """
    if not SHAREPOINT_SITE_ID:
        raise RuntimeError("No SharePoint site id set (SHAREPOINT_SALES_REPORTING_SITE_ID).")
    if not pdf_bytes:
        raise ValueError("Refusing to back up an empty PDF.")

    show_folder = sanitize(show_name)
    if not show_folder:
        raise ValueError(f"Show name {show_name!r} leaves nothing usable as a folder name.")

    path = ""
    for level in (*SHAREPOINT_PDF_PATH, show_folder):
        path = _ensure_folder(token, path, level)

    if frequency == "monthly":
        file_date_tag = date_range.replace(" ", "")
    else:
        from datetime import timedelta
        last_monday = run_date - timedelta(days=run_date.weekday() + 7)
        last_sunday = last_monday + timedelta(days=6)
        file_date_tag = f"{last_monday:%d%m%y}-{last_sunday:%d%m%y}"

    filename = f"{show_code}_Digital_Media_Report_{file_date_tag}.pptx"
    return _upload(token, f"{path}/{filename}", pdf_bytes, log=log)
