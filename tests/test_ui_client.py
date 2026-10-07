import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from ui.api_client import ApiClient, ApiError


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return io.BytesIO(json.dumps(self.payload).encode("utf-8"))

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class ApiClientTests(unittest.TestCase):
    def setUp(self):
        self.client = ApiClient("http://api:18000/", timeout=2)

    @patch("ui.api_client.urlopen")
    def test_results_request_is_read_only_and_parameterized(self, urlopen):
        urlopen.return_value = FakeResponse({"items": [], "total": 0})

        response = self.client.reconciliation_results("AMOUNT_MISMATCH", limit=25)

        self.assertEqual(response["total"], 0)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(
            request.full_url,
            "http://api:18000/reconciliation/results?"
            "result_type=AMOUNT_MISMATCH&limit=25",
        )
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 2)

    @patch("ui.api_client.urlopen")
    def test_transaction_id_is_url_encoded(self, urlopen):
        urlopen.return_value = FakeResponse(
            {"transaction_id": "tx/one", "ledger_records": []}
        )

        self.client.transaction("tx/one")

        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "http://api:18000/transactions/tx%2Fone")

    @patch("ui.api_client.urlopen")
    def test_not_found_has_clear_error(self, urlopen):
        urlopen.side_effect = HTTPError(
            "http://api:18000/transactions/missing",
            404,
            "Not Found",
            {},
            io.BytesIO(b'{"detail":"not found"}'),
        )

        with self.assertRaisesRegex(ApiError, "No matching record was found") as raised:
            self.client.transaction("missing")

        self.assertEqual(raised.exception.status_code, 404)

    @patch("ui.api_client.urlopen")
    def test_unavailable_api_has_clear_error(self, urlopen):
        urlopen.side_effect = URLError("connection refused")

        with self.assertRaisesRegex(ApiError, "API unavailable"):
            self.client.metrics()


if __name__ == "__main__":
    unittest.main()
