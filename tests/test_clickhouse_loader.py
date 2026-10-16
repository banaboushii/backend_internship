import os
import unittest
from unittest.mock import patch

from hubspot_dlt.clickhouse_loader import _config


class ClickHouseConfigTests(unittest.TestCase):
    def test_uses_clickhouse_reachable_minio_endpoint_when_configured(self):
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "http://localhost:9000",
                "CLICKHOUSE_S3_ENDPOINT": "http://minio:9000",
            },
            clear=True,
        ):
            self.assertEqual(_config()["endpoint"], "http://minio:9000")

    def test_defaults_clickhouse_endpoint_to_minio_endpoint(self):
        with patch.dict(
            os.environ, {"MINIO_ENDPOINT": "http://localhost:9000"}, clear=True
        ):
            self.assertEqual(_config()["endpoint"], "http://localhost:9000")


if __name__ == "__main__":
    unittest.main()
