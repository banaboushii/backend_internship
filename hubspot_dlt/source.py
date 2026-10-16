"""HubSpot CRM source resources with incremental dlt cursors."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

import dlt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .state import ControlStore

API_BASE = "https://api.hubapi.com"


@dataclass(frozen=True)
class ResourceSpec:
    name: str
    endpoint: str
    properties: tuple[str, ...] = ()
    cursor_field: str | None = "updatedAt"
    pagination: str = "after"
    associations: str | None = None


RESOURCE_SPECS = (
    ResourceSpec("contacts", "/crm/v3/objects/contacts", ("email", "firstname", "lastname", "hs_lastmodifieddate")),
    ResourceSpec("companies", "/crm/v3/objects/companies", ("name", "domain", "hs_lastmodifieddate")),
    ResourceSpec("deals", "/crm/v3/objects/deals", ("dealname", "amount", "pipeline", "dealstage", "hs_lastmodifieddate"), associations="companies"),
    ResourceSpec("tickets", "/crm/v3/objects/tickets", ("subject", "hs_lastmodifieddate")),
    ResourceSpec("line_items", "/crm/v3/objects/line_items", ("name", "quantity", "price", "hs_lastmodifieddate")),
    ResourceSpec("engagements", "/engagements/v1/engagements/paged", cursor_field="updatedAt", pagination="offset"),
    ResourceSpec("pipelines", "/crm/v3/pipelines/deals", cursor_field=None),
    ResourceSpec("owners", "/crm/v3/owners", cursor_field="updatedAt"),
)


def _session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None or value is False or value is True:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        seconds = float(value) / 1000.0 if abs(float(value)) >= 1_000_000_000_000 else float(value)
        if not math.isfinite(seconds):
            return None
        parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            normalized = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return None
        else:
            if not math.isfinite(number):
                return None
            seconds = number / 1000.0 if abs(number) >= 1_000_000_000_000 else number
            parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_engagement(item: dict[str, Any]) -> dict[str, Any]:
    engagement = item.get("engagement", {})
    updated = engagement.get("lastUpdated") or engagement.get("createdAt")
    parsed_updated = _parse_timestamp(updated)
    updated_at = parsed_updated.isoformat() if parsed_updated is not None else None
    return {
        "id": str(engagement.get("id", item.get("id", ""))),
        "updatedAt": updated_at,
        "createdAt": engagement.get("createdAt"),
        "type": engagement.get("type"),
        "archived": engagement.get("archived", False),
        "metadata": item.get("metadata", {}),
        "associations": item.get("associations", {}),
        "_version": _version(updated_at),
    }


def _version(updated_at: Any) -> datetime:
    parsed = _parse_timestamp(updated_at)
    if parsed is None:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize(item: dict[str, Any], spec: ResourceSpec) -> dict[str, Any]:
    if spec.name == "engagements":
        return _normalize_engagement(item)

    record = {
        key: value
        for key, value in item.items()
        if key not in {"properties", "associations"}
    }
    record.update(item.get("properties") or {})
    updated_at = item.get("updatedAt") or item.get("createdAt")
    record.update(
        {
            "id": str(item.get("id", "")),
            "createdAt": item.get("createdAt"),
            "updatedAt": updated_at,
            "archived": item.get("archived", False),
            "_version": _version(updated_at),
        }
    )
    if spec.name == "deals":
        company_results = (
            item.get("associations", {}).get("companies", {}).get("results", [])
        )
        record["company_id"] = str(company_results[0]["id"]) if company_results else None
    if spec.name == "pipelines":
        record["objectType"] = "deals"
        record["updatedAt"] = datetime.now(timezone.utc).isoformat()
        record["_version"] = _version(record["updatedAt"])
    return record


def _resource_iterator(
    spec: ResourceSpec, token: str, control: ControlStore
) -> Iterator[dict[str, Any]]:
    session = _session()
    headers = {"Authorization": f"Bearer {token}"}
    offset: str | None = None
    while True:
        control.wait_until_resumed()
        params: dict[str, Any]
        if spec.pagination == "offset":
            params = {"limit": 250}
            if offset:
                params["offset"] = offset
        else:
            params = {"limit": 100}
            if offset:
                params["after"] = offset
            if spec.properties:
                params["properties"] = ",".join(spec.properties)
            if spec.associations:
                params["associations"] = spec.associations

        response = session.get(
            f"{API_BASE}{spec.endpoint}", headers=headers, params=params, timeout=30
        )
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results") or []
        records = [_normalize(item, spec) for item in results]

        next_offset: str | None = None
        if spec.pagination == "offset":
            if payload.get("hasMore"):
                next_offset = str(payload.get("offset", "")) or None
        else:
            next_offset = payload.get("paging", {}).get("next", {}).get("after")

        cursor_value = None
        timestamps: list[datetime] = []
        for record in records:
            timestamp = _parse_timestamp(record.get("updatedAt"))
            if timestamp is not None:
                timestamps.append(timestamp)
        if timestamps:
            cursor_value = max(timestamps).isoformat()
        for record in records:
            yield record

        control.record_page(spec.name, next_offset, cursor_value)
        if not next_offset:
            break
        offset = str(next_offset)


def _make_resource(spec: ResourceSpec, token: str, control: ControlStore) -> Any:
    columns = {
        "id": {"data_type": "text", "nullable": False},
        "createdAt": {"data_type": "text"},
        "updatedAt": {"data_type": "text"},
        "_version": {"data_type": "timestamp", "nullable": False},
        "archived": {"data_type": "bool"},
    }
    if spec.name == "deals":
        columns.update(
            {
                "dealname": {"data_type": "text"},
                "amount": {"data_type": "text"},
                "pipeline": {"data_type": "text"},
                "dealstage": {"data_type": "text"},
                "company_id": {"data_type": "text"},
            }
        )
    elif spec.name == "companies":
        columns["name"] = {"data_type": "text"}
    elif spec.name == "pipelines":
        columns.update(
            {
                "label": {"data_type": "text"},
                "objectType": {"data_type": "text"},
            }
        )
    elif spec.name == "engagements":
        columns["type"] = {"data_type": "text"}

    if spec.cursor_field:
        @dlt.resource(
            name=spec.name,
            write_disposition="append",
            parallelized=True,
            columns=columns,
        )
        def extract(
            cursor=dlt.sources.incremental(
                spec.cursor_field, on_cursor_value_missing="include"
            )
        ):
            yield from _resource_iterator(spec, token, control)

        return extract

    @dlt.resource(
        name=spec.name,
        write_disposition="append",
        parallelized=True,
        columns=columns,
    )
    def extract_snapshot():
        yield from _resource_iterator(spec, token, control)

    return extract_snapshot


@dlt.source(name="hubspot")
def hubspot_source(token: str, control_db_path: str):
    control = ControlStore(control_db_path)
    return [_make_resource(spec, token, control) for spec in RESOURCE_SPECS]