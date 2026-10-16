import os
import re
import unittest
from unittest.mock import patch

from hubspot_dlt.clickhouse_loader import _config, load_into_clickhouse


class SimulatedClickHouse:
    def __init__(self):
        self.commands = []
        self.loaded_files = set()
        self.crash_after_first_insert = False

    def command(self, sql):
        self.commands.append(sql)
        if not sql.startswith("INSERT INTO"):
            return
        key = re.search(r"SELECT .*?, '([^']+)' FROM s3", sql, re.DOTALL).group(1)
        if key in self.loaded_files:
            return
        self.loaded_files.add(key)
        if self.crash_after_first_insert:
            self.crash_after_first_insert = False
            raise RuntimeError("simulated crash after committed insert")

    def query(self, sql):
        return type(
            "QueryResult",
            (),
            {
                "result_rows": [
                    ("id", "String"),
                    ("_version", "DateTime64(6, 'UTC')"),
                    ("dealname", "String"),
                    ("updated_at", "String"),
                ]
            },
        )()


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

    @patch("hubspot_dlt.clickhouse_loader._list_parquet_keys")
    def test_loads_each_object_file_once_across_restart(self, list_files):
        list_files.side_effect = lambda _config, resource: (
            ["hubspot/deals/year=2026/month=10/load.file.parquet"]
            if resource == "deals"
            else []
        )
        client = SimulatedClickHouse()
        client.crash_after_first_insert = True

        with self.assertRaisesRegex(RuntimeError, "simulated crash"):
            load_into_clickhouse(client)
        load_into_clickhouse(client)

        self.assertEqual(len(client.loaded_files), 1)
        inserts = [command for command in client.commands if command.startswith("INSERT")]
        self.assertTrue(inserts)
        self.assertTrue(all("NOT IN (SELECT `_source_file`" in sql for sql in inserts))
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS `default`.`hubspot_tickets`",
            "\n".join(client.commands),
        )

    @patch("hubspot_dlt.clickhouse_loader._list_parquet_keys")
    def test_adds_new_parquet_fields_without_rebuilding_table(self, list_files):
        list_files.side_effect = lambda _config, resource: (
            [
                "hubspot/deals/year=2026/month=10/first.parquet",
                "hubspot/deals/year=2026/month=10/second.parquet",
            ]
            if resource == "deals"
            else []
        )

        class EvolvingSchemaClient(SimulatedClickHouse):
            def query(self, sql):
                rows = [
                    ("id", "String"),
                    ("_version", "DateTime64(6, 'UTC')"),
                    ("dealname", "String"),
                ]
                if "second.parquet" in sql:
                    rows.append(("future_field", "String"))
                return type("QueryResult", (), {"result_rows": rows})()

        client = EvolvingSchemaClient()
        load_into_clickhouse(client)

        self.assertTrue(
            any("ADD COLUMN IF NOT EXISTS `future_field`" in sql for sql in client.commands)
        )


if __name__ == "__main__":
    unittest.main()
