"""Load new MinIO Parquet files into ClickHouse and expose a curated view."""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import quote

import boto3
import clickhouse_connect
from botocore.config import Config

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

VIEW_COLUMNS = {
    "deals": ("dealname", "amount", "dealstage", "updated_at", "company_id", "pipeline"),
    "companies": ("name",),
    "pipelines": ("label", "object_type"),
}


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _config() -> dict[str, str]:
    return {
        "endpoint": os.getenv(
            "CLICKHOUSE_S3_ENDPOINT",
            os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        ).rstrip("/"),
        "client_endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "bucket": os.getenv("MINIO_BUCKET", "hubspot"),
        "prefix": os.getenv("MINIO_PREFIX", "hubspot").strip("/"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", ""),
        "secret_key": os.getenv("MINIO_SECRET_KEY", ""),
        "database": os.getenv("CLICKHOUSE_DATABASE", "default"),
    }


def _list_parquet_keys(config: dict[str, str], resource: str) -> list[str]:
    client = boto3.client(
        "s3",
        endpoint_url=config["client_endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        config=Config(s3={"addressing_style": "path"}),
    )
    prefix = f"{config['prefix']}/{resource}/"
    paginator = client.get_paginator("list_objects_v2")
    return sorted(
        item["Key"]
        for page in paginator.paginate(Bucket=config["bucket"], Prefix=prefix)
        for item in page.get("Contents", [])
        if item["Key"].endswith(".parquet")
    )


def _s3_object_url(config: dict[str, str], key: str) -> str:
    return (
        f"{config['endpoint']}/{config['bucket']}/"
        f"{quote(key, safe='/')}"
    )


def _source_schema(client: Any, config: dict[str, str], key: str) -> list[str]:
    url = _literal(_s3_object_url(config, key))
    source = (
        f"s3({url}, {_literal(config['access_key'])}, "
        f"{_literal(config['secret_key'])}, 'Parquet')"
    )
    rows = client.query(f"DESCRIBE TABLE {source}").result_rows
    columns = [str(row[0]) for row in rows]
    if not {"id", "_version"}.issubset(columns):
        raise ValueError(f"Parquet file {key!r} is missing required id/_version columns")
    return columns


def _ensure_table(client: Any, database: str, resource: str) -> None:
    table = _identifier(f"hubspot_{resource}")
    predefined = [
        f"{_identifier(column)} Nullable(String)"
        for column in VIEW_COLUMNS.get(resource, ())
    ]
    columns = [
        "`id` String",
        "`_version` DateTime64(3, 'UTC')",
        "`_source_file` String",
        *predefined,
    ]
    client.command(
        f"CREATE TABLE IF NOT EXISTS {database}.{table} "
        f"({', '.join(columns)}) "
        "ENGINE = ReplacingMergeTree(_version) ORDER BY id"
    )
    client.command(
        f"ALTER TABLE {database}.{table} "
        "ADD COLUMN IF NOT EXISTS `_source_file` String DEFAULT ''"
    )
    for column in VIEW_COLUMNS.get(resource, ()):
        client.command(
            f"ALTER TABLE {database}.{table} "
            f"ADD COLUMN IF NOT EXISTS {_identifier(column)} Nullable(String)"
        )


def _ensure_columns(
    client: Any, database: str, resource: str, source_columns: list[str]
) -> list[str]:
    table = _identifier(f"hubspot_{resource}")
    added: list[str] = []
    for column in source_columns:
        if column in {"id", "_version", "_source_file"}:
            continue
        quoted = _identifier(column)
        client.command(
            f"ALTER TABLE {database}.{table} "
            f"ADD COLUMN IF NOT EXISTS {quoted} Nullable(String)"
        )
        added.append(column)
    return added


def _load_file(
    client: Any,
    config: dict[str, str],
    database: str,
    resource: str,
    key: str,
) -> None:
    table = _identifier(f"hubspot_{resource}")
    url = _literal(_s3_object_url(config, key))
    source = (
        f"s3({url}, {_literal(config['access_key'])}, "
        f"{_literal(config['secret_key'])}, 'Parquet')"
    )
    source_columns = _source_schema(client, config, key)
    columns = _ensure_columns(client, database, resource, source_columns)
    target_columns = ["id", "_version", *columns, "_source_file"]
    selected_columns = [
        "ifNull(toString(`id`), '')",
        "toDateTime64(`_version`, 3, 'UTC')",
        *(f"toString({_identifier(column)})" for column in columns),
        _literal(key),
    ]
    client.command(
        f"INSERT INTO {database}.{table} "
        f"({', '.join(_identifier(column) for column in target_columns)}) "
        f"SELECT {', '.join(selected_columns)} FROM {source} "
        f"WHERE {_literal(key)} NOT IN "
        f"(SELECT `_source_file` FROM {database}.{table})"
    )


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
        _ensure_table(client, database, resource)
        for key in _list_parquet_keys(config, resource):
            _load_file(client, config, database, resource, key)

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
