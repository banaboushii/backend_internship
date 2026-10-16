"""dlt pipeline configuration for compressed Parquet files in MinIO."""

from __future__ import annotations

import os
from typing import Any

import dlt

from .source import hubspot_source


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def run_pipeline() -> Any:
    workers = int(os.getenv("HUBSPOT_WORKERS", "8"))
    if workers < 1:
        raise RuntimeError("HUBSPOT_WORKERS must be at least 1")
    dlt.config["extract.workers"] = workers

    token = _required("HUBSPOT_ACCESS_TOKEN")
    bucket = os.getenv("MINIO_BUCKET", "hubspot")
    prefix = os.getenv("MINIO_PREFIX", "hubspot").strip("/")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    filesystem = dlt.destinations.filesystem(
        bucket_url=f"s3://{bucket}",
        credentials={
            "aws_access_key_id": _required("MINIO_ACCESS_KEY"),
            "aws_secret_access_key": _required("MINIO_SECRET_KEY"),
            "endpoint_url": endpoint,
        },
        layout=(
            "{table_name}/year={YYYY}/month={MM}/"
            "{load_id}.{file_id}.{ext}"
        ),
    )
    pipeline = dlt.pipeline(
        pipeline_name="hubspot_ingestion",
        destination=filesystem,
        dataset_name=prefix,
        pipelines_dir=os.getenv("DLT_PIPELINES_DIR", ".dlt"),
    )
    source = hubspot_source(
        token=token,
        control_db_path=os.getenv("HUBSPOT_CONTROL_DB", "state/control.sqlite"),
    )
    return pipeline.run(source, loader_file_format="parquet")