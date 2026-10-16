import os
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from hubspot_dlt.mock_api import MockHubSpotHandler
from hubspot_dlt.source import RESOURCE_SPECS, _resource_iterator
from hubspot_dlt.state import ControlStore


class MockHubSpotApiTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), MockHubSpotHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.temporary_directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temporary_directory.cleanup()

    def _fetch(self, resource_name, last_value=None):
        spec = next(spec for spec in RESOURCE_SPECS if spec.name == resource_name)
        control = ControlStore(
            os.path.join(self.temporary_directory.name, "control.sqlite")
        )
        with patch.dict(
            os.environ,
            {
                "HUBSPOT_API_BASE": self.base_url,
                "HUBSPOT_MOCK_PAGE_SIZE": "2",
                "HUBSPOT_MOCK_RECORDS": "5",
                "HUBSPOT_MOCK_DELAY_SECONDS": "0",
            },
        ):
            return list(
                _resource_iterator(
                    spec, "mock-token", control, last_value=last_value
                )
            )

    def test_mock_api_paginates_initial_crm_sync(self):
        records = self._fetch("contacts")

        self.assertEqual([record["id"] for record in records], ["1", "2", "3", "4", "5"])

    def test_incremental_search_fetches_only_newer_records(self):
        records = self._fetch("contacts", "2025-01-01T00:02:00+00:00")

        self.assertEqual([record["id"] for record in records], ["3", "4", "5"])
        self.assertTrue(all("@" in record["email"] for record in records))

    def test_incremental_deal_search_keeps_company_associations(self):
        records = self._fetch("deals", "2025-01-01T00:03:00+00:00")

        self.assertEqual([record["id"] for record in records], ["4", "5"])
        self.assertEqual(records[0]["company_id"], "4")


if __name__ == "__main__":
    unittest.main()
