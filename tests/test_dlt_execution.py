import multiprocessing
import os
import signal
import tempfile
import threading
import unittest
from pathlib import Path

import dlt
import pyarrow.parquet as parquet
from dlt.destinations import filesystem

import hubspot_dlt.source as source_module


LAYOUT = "{table_name}/year={YYYY}/month={MM}/{load_id}.{file_id}.{ext}"
ROWS = (
    {"id": "1", "updatedAt": "2026-10-10T12:00:00Z", "name": "first"},
    {"id": "2", "updatedAt": "2026-10-10T12:01:00Z", "name": "second"},
)


def _crash_during_extraction(root: str, ready, release) -> None:
    destination = filesystem(
        bucket_url=f"file://{root}/landed", layout=LAYOUT
    )
    pipeline = dlt.pipeline(
        pipeline_name="crash_recovery_test",
        destination=destination,
        dataset_name="hubspot",
        pipelines_dir=f"{root}/.dlt",
    )

    @dlt.resource(
        name="deals",
        write_disposition="append",
        parallelized=True,
    )
    def deals(cursor=dlt.sources.incremental("updatedAt")):
        yield ROWS[0]
        ready.set()
        release.wait()
        yield ROWS[1]

    pipeline.run(deals, loader_file_format="parquet")


def _synthetic_item(spec):
    if spec.name == "engagements":
        return {
            "engagement": {
                "id": 7,
                "createdAt": 1791630000000,
                "lastUpdated": 1791633600000,
            },
            "metadata": {},
        }
    if spec.name == "pipelines":
        return {"id": "sales", "label": "Sales", "stages": []}
    return {
        "id": "1",
        "createdAt": "2026-10-10T11:00:00Z",
        "updatedAt": "2026-10-10T12:00:00Z",
        "properties": {"name": "Synthetic record"},
    }


class DltExecutionTests(unittest.TestCase):
    def test_force_kill_restarts_from_last_committed_package(self):
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as temporary_directory:
            ready = context.Event()
            release = context.Event()
            worker = context.Process(
                target=_crash_during_extraction,
                args=(temporary_directory, ready, release),
            )
            worker.start()
            self.assertTrue(ready.wait(timeout=30), "worker did not reach extraction")
            os.kill(worker.pid, signal.SIGKILL)
            worker.join(timeout=10)
            self.assertEqual(worker.exitcode, -signal.SIGKILL)

            destination = filesystem(
                bucket_url=f"file://{temporary_directory}/landed", layout=LAYOUT
            )
            pipeline = dlt.pipeline(
                pipeline_name="crash_recovery_test",
                destination=destination,
                dataset_name="hubspot",
                pipelines_dir=f"{temporary_directory}/.dlt",
            )

            @dlt.resource(
                name="deals",
                write_disposition="append",
                parallelized=True,
            )
            def complete_deals(cursor=dlt.sources.incremental("updatedAt")):
                yield from ROWS

            pipeline.run(complete_deals, loader_file_format="parquet")
            files = list(Path(temporary_directory, "landed").rglob("*.parquet"))
            ids = [
                record
                for path in files
                for record in parquet.read_table(path, columns=["id"])["id"].to_pylist()
            ]
            self.assertCountEqual(ids, ["1", "2"])

    def test_independent_hubspot_resources_extract_concurrently(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            barrier = threading.Barrier(len(source_module.RESOURCE_SPECS), timeout=20)
            thread_names = set()
            lock = threading.Lock()
            original_iterator = source_module._resource_iterator

            def concurrent_iterator(spec, token, control):
                barrier.wait()
                with lock:
                    thread_names.add(threading.current_thread().name)
                yield source_module._normalize(_synthetic_item(spec), spec)

            source_module._resource_iterator = concurrent_iterator
            try:
                source = source_module.hubspot_source(
                    token="test-token",
                    control_db_path=f"{temporary_directory}/control.sqlite",
                )
                pipeline = dlt.pipeline(
                    pipeline_name="parallel_resources_test",
                    destination=filesystem(
                        bucket_url=f"file://{temporary_directory}/landed",
                        layout=LAYOUT,
                    ),
                    dataset_name="hubspot",
                    pipelines_dir=f"{temporary_directory}/.dlt",
                )
                dlt.config["extract.workers"] = len(source_module.RESOURCE_SPECS)
                pipeline.run(source, loader_file_format="parquet")
                deal_file = next(
                    (Path(temporary_directory) / "landed" / "hubspot" / "deals").rglob(
                        "*.parquet"
                    )
                )
                pipeline_file = next(
                    (Path(temporary_directory) / "landed" / "hubspot" / "pipelines").rglob(
                        "*.parquet"
                    )
                )
            finally:
                source_module._resource_iterator = original_iterator

            self.assertEqual(len(thread_names), len(source_module.RESOURCE_SPECS))
            self.assertIn("company_id", parquet.ParquetFile(deal_file).schema.names)
            self.assertIn("updated_at", parquet.ParquetFile(deal_file).schema.names)
            self.assertIn("object_type", parquet.ParquetFile(pipeline_file).schema.names)


if __name__ == "__main__":
    unittest.main()