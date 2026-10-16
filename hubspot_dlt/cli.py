"""Command-line controls for running and operating the ingestion pipeline."""

from __future__ import annotations

import argparse
import json
import os

from .clickhouse_loader import load_into_clickhouse
from .pipeline import run_pipeline
from .state import ControlStore


def main() -> None:
    parser = argparse.ArgumentParser(prog="hubspot-dlt")
    parser.add_argument(
        "command", choices=("run", "pause", "resume", "status", "load-clickhouse")
    )
    args = parser.parse_args()
    state = ControlStore(os.getenv("HUBSPOT_CONTROL_DB", "state/control.sqlite"))

    if args.command == "pause":
        state.set_paused(True)
        print("Pipeline paused at the next page boundary.")
    elif args.command == "resume":
        state.set_paused(False)
        print("Pipeline resumed.")
    elif args.command == "status":
        print(json.dumps(state.status(), indent=2))
    elif args.command == "run":
        print(run_pipeline())
    else:
        load_into_clickhouse()
        print("ClickHouse tables and analytical view updated.")


if __name__ == "__main__":
    main()