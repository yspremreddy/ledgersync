"""Small standard-library client for the LedgerSync read-only API."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


class ApiError(RuntimeError):
    """A user-displayable API communication error."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ApiClient:
    """Read-only HTTP client used by the Streamlit application."""

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        query = urlencode(
            {key: value for key, value in params.items() if value is not None}
        )
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        request = Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except HTTPError as exc:
            detail = _http_error_detail(exc)
            raise ApiError(detail, status_code=exc.code) from exc
        except URLError as exc:
            reason = getattr(exc, "reason", exc)
            raise ApiError(f"API unavailable: {reason}") from exc
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ApiError("API returned an invalid JSON response") from exc

    def health(self) -> dict[str, Any]:
        return self.get("/health")

    def metrics(self) -> dict[str, Any]:
        return self.get("/metrics")

    def reconciliation_runs(self, limit: int = 20) -> dict[str, Any]:
        return self.get("/reconciliation/runs", limit=limit)

    def reconciliation_results(
        self, result_type: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        return self.get(
            "/reconciliation/results", result_type=result_type, limit=limit
        )

    def quality_checks(self, limit: int = 100) -> dict[str, Any]:
        return self.get("/quality/checks", limit=limit)

    def transaction(self, transaction_id: str) -> dict[str, Any]:
        encoded = quote(transaction_id, safe="")
        return self.get(f"/transactions/{encoded}")


def _http_error_detail(error: HTTPError) -> str:
    if error.code == 404:
        return "No matching record was found"
    try:
        body = json.loads(error.read().decode("utf-8"))
        detail = body.get("detail")
        if detail:
            return f"API request failed ({error.code}): {detail}"
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    return f"API request failed with status {error.code}"
