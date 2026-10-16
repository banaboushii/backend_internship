"""Load MinIO Parquet files into ClickHouse and expose a curated view."""

from __future__ import annotations

import os
import re
from typing import Any

import clickhouse_connect

RESOURCES = (
    "contacts",
    "companies",
    "deals",
    "tickets",
    "line_items",
    "engagements",
    "pipelines",
    "owners",
)


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _config() -> dict[str, str]:
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000").rstrip("/"),
        "bucket": os.getenv("MINIO_BUCKET", "hubspot"),
        "prefix": os.getenv("MINIO_PREFIX", "hubspot").strip("/"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", ""),
        "secret_key": os.getenv("MINIO_SECRET_KEY", ""),
        "database": os.getenv("CLICKHOUSE_DATABASE", "default"),
    }


def load_into_clickhouse(client: Any | None = None) -> None:
    config = _config()
    if client is None:
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )

    database = _identifier(config["database"])
    client.command(f"CREATE DATABASE IF NOT EXISTS {database}")

    for resource in RESOURCES:
        table = _identifier(f"hubspot_{resource}")
        object_path = (
            f"{config['endpoint']}/{config['bucket']}/{config['prefix']}/"
            f"{resource}/year=*/month=*/*.parquet"
        )
        parquet = (
            f"s3({_literal(object_path)}, {_literal(config['access_key'])}, "
            f"{_literal(config['secret_key'])}, 'Parquet')"
        )
        client.command(
            f"CREATE TABLE IF NOT EXISTS {database}.{table} "
            f"ENGINE = ReplacingMergeTree(_version) ORDER BY id "
            f"AS SELECT * FROM {parquet} LIMIT 0"
        )
        client.command(
            f"INSERT INTO {database}.{table} SELECT * FROM {parquet}"
        )

    client.command(
        f"""CREATE OR REPLACE VIEW {database}.v_hubspot_deal_pipeline AS
        SELECT
            d.id AS deal_id,
            d.dealname AS deal_name,
            toFloat64OrNull(d.amount) AS amount,
            d.dealstage AS deal_stage,
            d.updated_at AS deal_updated_at,
            c.id AS company_id,
            c.name AS company_name,
            p.label AS pipeline_name
        FROM (SELECT * FROM {database}.hubspot_deals FINAL) AS d
        LEFT JOIN (SELECT * FROM {database}.hubspot_companies FINAL) AS c
            ON d.company_id = c.id
        LEFT JOIN (SELECT * FROM {database}.hubspot_pipelines FINAL) AS p
            ON d.pipeline = p.id AND p.object_type = 'deals'"""
    )