import os
import requests
import msal
import pytz
from datetime import datetime
from src.env_setup import get_universal_logger


def _to_id_str(value) -> str:
    """
    Coerce an ID column to a clean string.

    ShowID and DocumentID were created as Number columns, so Graph hands them
    back as floats (e.g. 180.0). We must strip the trailing ".0" or the ID
    won't match downstream. Text IDs (like VenueID) pass straight through.
    """
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    return str(value).strip()


def _to_bool(value) -> bool:
    """
    Safely coerce a SharePoint value to a real bool.

    Graph returns Yes/No columns as proper JSON true/false, but a *blank*
    cell usually comes back as None (or the key is missing entirely).
    We must never use bool(value) directly: bool("false") is True in
    Python, which would silently flip an inactive rule to active.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("true", "yes", "1")


class SharePointRuleLoader:
    """
    Loads ALL sales-report rules from a SharePoint List and reshapes each list
    item into the same nested rule dict the rest of the pipeline already
    understands (match_criteria / metadata / processing).

    A rule is treated as a parser rule when BOTH ParserModule and
    ParserFunction are filled in; otherwise it is passthrough. The parser
    columns are dev-only and are left blank by ops users.

    Reuses the existing Azure app registration + MSAL client-credentials flow,
    exactly like GraphClient and SharePointUploader.
    """

    def __init__(self, list_name: str):
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.site_id = os.getenv("SHAREPOINT_SALES_REPORTING_SITE_ID")
        self.list_name = list_name

        if not all([self.tenant_id, self.client_id, self.client_secret, self.site_id]):
            raise ValueError(
                "Missing Azure credentials or SHAREPOINT_SALES_REPORTING_SITE_ID in .env"
            )

        self.base_url = "https://graph.microsoft.com/v1.0"
        # Build the MSAL app lazily (see _get_token). Doing it here would put a
        # thread lock on the object, which breaks cloudpickle when Prefect runs
        # a scheduled flow in a subprocess. GraphClient defers it for the same reason.
        self.app = None
        # Cache the resolved list GUID so we don't look it up on every call.
        self._list_id = None

    def _get_token(self) -> str:
        if self.app is None:
            self.app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}",
                client_credential=self.client_secret,
            )
        result = self.app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        if "access_token" in result:
            return result["access_token"]
        raise Exception(f"Failed to authenticate: {result.get('error_description')}")

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    def _resolve_list_id(self) -> str:
        """
        Find the list's GUID by its display name. Graph can't filter lists by
        displayName server-side reliably, so we pull the (short) list of lists
        and match client-side.
        """
        url = f"{self.base_url}/sites/{self.site_id}/lists"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()

        for lst in resp.json().get("value", []):
            if lst.get("displayName") == self.list_name:
                return lst["id"]

        raise ValueError(
            f"SharePoint list '{self.list_name}' not found on the configured site."
        )

    def _get_list_id(self) -> str:
        """Resolve the list GUID once and reuse it."""
        if self._list_id is None:
            self._list_id = self._resolve_list_id()
        return self._list_id

    def _fetch_items(self, list_id: str) -> list:
        """Fetch every list item with its column values, following pagination."""
        url = (
            f"{self.base_url}/sites/{self.site_id}/lists/{list_id}/items"
            "?$expand=fields&$top=200"
        )
        items = []
        headers = self._headers()

        while url:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")  # None when there are no more pages

        return items

    def load_rules(self) -> list:
        """
        Return a list of rule dicts shaped exactly like the old JSON rules,
        covering both passthrough and parser rules.
        """
        logger = get_universal_logger(__name__)

        list_id = self._get_list_id()
        items = self._fetch_items(list_id)

        rules = []
        for item in items:
            f = item.get("fields", {})

            # NOTE: the SharePoint default "Title" column is renamed to
            # "Show Name" in the UI, but its *internal* name stays "Title".
            show_name = (f.get("Title") or "").strip()
            venue_name = (f.get("VenueName") or "").strip()

            # Skip half-finished rows rather than letting them crash the run.
            if not show_name or not venue_name:
                logger.warning(
                    f"Skipping SharePoint rule with missing show/venue "
                    f"(item id {item.get('id')})."
                )
                continue

            report_type = (f.get("ReportType") or "").strip()

            # Auto-generated rule name: SHOW_VENUE_REPORTTYPE, uppercase, spaces
            # -> underscores. Report type is included so the same show + venue
            # can carry more than one rule (e.g. an Advance and a Cumulative
            # report) without the names colliding. Empty parts are dropped.
            rule_name = "_".join(
                p for p in (show_name, venue_name, report_type) if p
            ).upper().replace(" ", "_")

            match_criteria = {
                "sender_domain": (f.get("SenderDomain") or "").strip(),
                "subject_keyword": (f.get("SubjectKeyword") or "").strip(),
                "attachment_type": (f.get("AttachmentType") or "").strip(),
            }

            # Only include attachment_source when set; blank -> pipeline default "physical".
            source = (f.get("AttachmentSource") or "").strip()
            if source:
                match_criteria["attachment_source"] = source

            # Optional: when one email carries attachments for several venues,
            # this picks the right one by a fragment of its file name
            # (e.g. "MRSH" vs "MRGZ"). Blank -> take the first matching
            # attachment, which is the original behaviour.
            filename_keyword = (f.get("FileNameKeyword") or "").strip()
            if filename_keyword:
                match_criteria["filename_keyword"] = filename_keyword

            metadata = {
                "show_name": show_name,
                "venue_name": venue_name,
                "report_type": report_type,
                # Internal names use capital "ID"; ShowID/DocumentID are Number
                # columns, so _to_id_str strips the trailing ".0" they return.
                "show_id": _to_id_str(f.get("ShowID")),
                "venue_id": _to_id_str(f.get("VenueID")),
                "document_id": _to_id_str(f.get("DocumentID")),
                "timezone": (f.get("Timezone") or "UTC").strip(),
                "sales_day_offset_hours": int(f.get("SalesDayOffsetHours") or 0),
            }

            # Decide processing mode from the (optional, dev-only) parser columns.
            parser_module = (f.get("ParserModule") or "").strip()
            parser_function = (f.get("ParserFunction") or "").strip()

            if parser_module and parser_function:
                processing = {
                    "parser_module": parser_module,
                    "parser_function": parser_function,
                    "needs_lookup": _to_bool(f.get("NeedsLookup")),
                }
            elif parser_module or parser_function:
                # One filled without the other = a half-configured parser rule.
                # Skip it loudly rather than silently passing a raw file through.
                logger.warning(
                    f"Skipping rule '{rule_name}': ParserModule/ParserFunction "
                    f"must both be set, or both left blank."
                )
                continue
            else:
                processing = {"passthrough_only": True}

            rules.append(
                {
                    "rule_name": rule_name,
                    "active": _to_bool(f.get("Active")),
                    "match_criteria": match_criteria,
                    "metadata": metadata,
                    "processing": processing,
                    # Internal: the SharePoint row id, used to stamp LastRun.
                    # Not part of the logical rule; downstream code ignores it.
                    "_sp_item_id": item.get("id"),
                }
            )

        logger.info(
            f"Loaded {len(rules)} rule(s) from SharePoint list "
            f"'{self.list_name}'."
        )
        return rules

    def update_last_run(self, item_id, when=None) -> None:
        """
        Stamp the LastRun column on a single rule's row with the current UK
        time (or a supplied datetime). Best-effort: callers should treat a
        failure here as non-fatal, since the report itself already succeeded.
        """
        if not item_id:
            return

        logger = get_universal_logger(__name__)

        if when is None:
            when = datetime.now(pytz.timezone("Europe/London"))
        when_iso = when.isoformat()

        list_id = self._get_list_id()
        url = (
            f"{self.base_url}/sites/{self.site_id}/lists/{list_id}"
            f"/items/{item_id}/fields"
        )
        resp = requests.patch(
            url, headers=self._headers(), json={"LastRun": when_iso}
        )
        resp.raise_for_status()
        logger.info(f"🕒 Stamped LastRun on row {item_id}: {when_iso}")