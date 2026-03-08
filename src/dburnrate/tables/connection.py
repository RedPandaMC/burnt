"""Databricks REST API client for system table queries."""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any

import requests

from ..core.exceptions import DatabricksConnectionError, DatabricksQueryError

if TYPE_CHECKING:
    from ..core.config import Settings

_RETRY_STATUSES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_POLL_INTERVAL_S = 1.0
_STATEMENT_TIMEOUT = "30s"
_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]{1,256}$")


def _sanitize_id(value: str, field: str = "id") -> str:
    """Raise ValueError if value contains chars unsafe for SQL interpolation."""
    if not _SAFE_ID_RE.match(value):
        raise ValueError(
            f"Invalid {field}: {value!r} (alphanumeric, hyphens, underscores only)"
        )
    return value


class DatabricksClient:
    """Thin REST client for Databricks SQL Statement Execution API."""

    def __init__(self, settings: Settings) -> None:
        """Initialise client from settings; raises DatabricksConnectionError if credentials missing."""
        if not settings.workspace_url:
            raise DatabricksConnectionError("DBURNRATE_WORKSPACE_URL is not set")
        if not settings.token:
            raise DatabricksConnectionError("DBURNRATE_TOKEN is not set")

        self._base_url = settings.workspace_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {settings.token}",
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_sql(self, sql: str, warehouse_id: str) -> list[dict[str, Any]]:
        """Execute a SQL statement and return rows as list of dicts.

        Blocks until the statement completes (SUCCEEDED or FAILED).
        If the API returns inline results immediately, no polling is needed.
        Retries on transient errors (429, 5xx) up to 3 times.
        """
        statement_id, inline_result = self._submit(sql, warehouse_id)
        if inline_result is not None:
            return inline_result
        return self._wait_and_fetch(statement_id)

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> DatabricksClient:
        """Support use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close session on context manager exit."""
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _submit(
        self, sql: str, warehouse_id: str
    ) -> tuple[str, list[dict[str, Any]] | None]:
        """Submit a SQL statement; return (statement_id, inline_rows_or_None).

        If the API returns results inline (SUCCEEDED immediately), rows are
        included in the tuple so polling can be skipped.
        """
        url = f"{self._base_url}/api/2.0/sql/statements"
        payload = {
            "statement": sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": _STATEMENT_TIMEOUT,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        resp = self._post_with_retry(url, payload)
        data = resp.json()
        statement_id: str = data["statement_id"]
        state = data.get("status", {}).get("state", "PENDING")
        if state == "FAILED":
            error = data.get("status", {}).get("error", {})
            raise DatabricksQueryError(
                f"SQL statement failed immediately: {error.get('message', 'unknown error')}"
            )
        if state == "SUCCEEDED":
            return statement_id, self._extract_rows(data)
        return statement_id, None

    def _wait_and_fetch(self, statement_id: str) -> list[dict[str, Any]]:
        """Poll until statement completes, then return rows."""
        url = f"{self._base_url}/api/2.0/sql/statements/{statement_id}"
        while True:
            resp = self._get_with_retry(url)
            data = resp.json()
            state = data.get("status", {}).get("state", "PENDING")

            if state == "SUCCEEDED":
                return self._extract_rows(data)
            if state == "FAILED":
                error = data.get("status", {}).get("error", {})
                raise DatabricksQueryError(
                    f"SQL statement failed: {error.get('message', 'unknown error')}"
                )
            if state in ("CANCELED", "CLOSED"):
                raise DatabricksQueryError(f"SQL statement ended with state: {state}")

            time.sleep(_POLL_INTERVAL_S)

    def _extract_rows(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert API response into list of column-name → value dicts."""
        schema = data.get("manifest", {}).get("schema", {}).get("columns", [])
        col_names = [col["name"] for col in schema]

        rows: list[dict[str, Any]] = []
        result = data.get("result", {})
        for row_values in result.get("data_array", []):
            rows.append(dict(zip(col_names, row_values, strict=False)))
        return rows

    def _post_with_retry(self, url: str, payload: dict[str, Any]) -> requests.Response:
        """POST with exponential backoff retry on transient errors."""
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = self._session.post(url, json=payload, timeout=30)
                if resp.status_code in _RETRY_STATUSES:
                    wait = self._retry_wait(resp, attempt)
                    time.sleep(wait)
                    last_exc = DatabricksConnectionError(
                        f"HTTP {resp.status_code} from {url}"
                    )
                    continue
                if resp.status_code == 401:
                    raise DatabricksConnectionError(
                        "Authentication failed - check DBURNRATE_TOKEN"
                    )
                resp.raise_for_status()
                return resp
            except DatabricksConnectionError:
                raise
            except requests.RequestException as exc:
                last_exc = DatabricksConnectionError(f"Request failed: {exc}")
                time.sleep(2**attempt)
        raise last_exc or DatabricksConnectionError("All retries exhausted")

    def _get_with_retry(self, url: str) -> requests.Response:
        """GET with exponential backoff retry on transient errors."""
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = self._session.get(url, timeout=30)
                if resp.status_code in _RETRY_STATUSES:
                    wait = self._retry_wait(resp, attempt)
                    time.sleep(wait)
                    last_exc = DatabricksConnectionError(
                        f"HTTP {resp.status_code} from {url}"
                    )
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_exc = DatabricksConnectionError(f"Request failed: {exc}")
                time.sleep(2**attempt)
        raise last_exc or DatabricksConnectionError("All retries exhausted")

    @staticmethod
    def _retry_wait(resp: requests.Response, attempt: int) -> float:
        """Return seconds to wait before retry, respecting Retry-After header."""
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return float(2**attempt)
