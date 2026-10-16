"""Paginated synthetic HubSpot API for local development and demos."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from typing import Any

from dotenv import load_dotenv


def _timestamp(index: int) -> str:
    value = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=index)
    return value.isoformat().replace("+00:00", "Z")


def _records(resource: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    record_count = int(os.getenv("HUBSPOT_MOCK_RECORDS", "20"))
    for index in range(1, record_count + 1):
        record: dict[str, Any] = {
            "id": str(index),
            "createdAt": _timestamp(index),
            "updatedAt": _timestamp(index),
            "properties": {
                "hs_lastmodifieddate": _timestamp(index),
                "name": f"Mock {resource} {index}",
            },
        }
        if resource == "deals":
            record["properties"].update(
                {
                    "dealname": f"Mock deal {index}",
                    "amount": str(index * 100),
                    "pipeline": "sales",
                    "dealstage": "qualified",
                }
            )
            record["associations"] = {
                "companies": {"results": [{"id": str(index)}]}
            }
        elif resource == "companies":
            record["properties"]["name"] = f"Mock company {index}"
        elif resource == "contacts":
            record["properties"]["email"] = f"contact{index}@example.test"
        elif resource == "tickets":
            record["properties"]["subject"] = f"Mock ticket {index}"
        elif resource == "line_items":
            record["properties"].update({"quantity": "1", "price": "100"})
        elif resource == "owners":
            record = {
                "id": str(index),
                "email": f"owner{index}@example.test",
                "firstName": "Mock",
                "lastName": f"Owner {index}",
                "updatedAt": _timestamp(index),
            }
        elif resource == "pipelines":
            record = {
                "id": "sales",
                "label": "Mock sales pipeline",
                "stages": [{"id": "qualified", "label": "Qualified"}],
                "displayOrder": index,
            }
        elif resource == "engagements":
            record = {
                "engagement": {
                    "id": index,
                    "createdAt": int(
                        (datetime(2025, 1, 1, tzinfo=timezone.utc)
                         + timedelta(minutes=index)).timestamp() * 1000
                    ),
                    "lastUpdated": int(
                        (datetime(2025, 1, 1, tzinfo=timezone.utc)
                         + timedelta(minutes=index)).timestamp() * 1000
                    ),
                    "type": "NOTE",
                },
                "metadata": {"body": f"Mock engagement {index}"},
            }
        records.append(record)
    return records


class MockHubSpotHandler(BaseHTTPRequestHandler):
    server_version = "MockHubSpot/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        resource = self._resource_for_path(path)
        if resource is None:
            self.send_error(404)
            return
        records = _records(resource)
        if resource == "engagements":
            since_value = params.get("since", ["0"])[0]
            floor = int(since_value)
            records = [
                record
                for record in records
                if record["engagement"]["lastUpdated"] >= floor
            ]
            self._write_page(records, params, legacy=True)
        else:
            self._write_page(records, params)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        if path == "/crm/v4/associations/deals/companies/batch/read":
            encoded = json.dumps(
                {
                    "results": [
                        {
                            "from": {"id": str(item["id"])},
                            "to": [{"toObjectId": str(item["id"])}],
                        }
                        for item in body.get("inputs", [])
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
            return
        resource = self._resource_for_path(path.removesuffix("/search"))
        if resource is None or not path.endswith("/search"):
            self.send_error(404)
            return
        records = _records(resource)
        filters = body.get("filterGroups", [])
        if filters:
            floor = int(filters[0]["filters"][0]["value"])
            records = [
                record
                for record in records
                if int(
                    datetime.fromisoformat(
                        record["properties"]["hs_lastmodifieddate"].replace(
                            "Z", "+00:00"
                        )
                    ).timestamp()
                    * 1000
                ) > floor
            ]
        self._write_page(records, body)

    def _resource_for_path(self, path: str) -> str | None:
        if path.startswith("/crm/v3/objects/"):
            resource = path.rsplit("/", 1)[-1]
            return resource if resource in {
                "contacts", "companies", "deals", "tickets", "line_items"
            } else None
        if path == "/crm/v3/pipelines/deals":
            return "pipelines"
        if path == "/crm/v3/owners":
            return "owners"
        if path == "/engagements/v1/engagements/paged":
            return "engagements"
        return None

    def _write_page(
        self, records: list[dict[str, Any]], params: dict[str, Any], legacy: bool = False
    ) -> None:
        time.sleep(float(os.getenv("HUBSPOT_MOCK_DELAY_SECONDS", "0")))
        limit = max(1, min(int(os.getenv("HUBSPOT_MOCK_PAGE_SIZE", "2")), 100))
        raw_start = params.get("after", params.get("offset", 0)) or 0
        if isinstance(raw_start, list):
            raw_start = raw_start[0]
        start = int(raw_start)
        page = records[start : start + limit]
        next_index = start + len(page)
        more = next_index < len(records)
        if legacy:
            response = {"results": page, "hasMore": more}
            if more:
                response["offset"] = str(next_index)
        else:
            response = {"results": page}
            if more:
                response["paging"] = {"next": {"after": str(next_index)}}
        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main() -> None:
    load_dotenv()
    host = os.getenv("HUBSPOT_MOCK_HOST", "127.0.0.1")
    port = int(os.getenv("HUBSPOT_MOCK_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), MockHubSpotHandler)
    print(f"Mock HubSpot API listening at http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
