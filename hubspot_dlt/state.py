"""Persistent operator controls and observable extraction progress."""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


class ControlStore:
    """SQLite-backed pause flag and per-resource page progress."""

    def __init__(self, path: str | Path, poll_seconds: float = 1.0) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.poll_seconds = poll_seconds
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS pipeline_control (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    paused INTEGER NOT NULL DEFAULT 0
                );
                INSERT OR IGNORE INTO pipeline_control (singleton, paused)
                VALUES (1, 0);
                CREATE TABLE IF NOT EXISTS resource_progress (
                    resource_name TEXT PRIMARY KEY,
                    next_offset TEXT,
                    cursor_timestamp TEXT,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def set_paused(self, paused: bool) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE pipeline_control SET paused = ? WHERE singleton = 1",
                (int(paused),),
            )

    def is_paused(self) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT paused FROM pipeline_control WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return False
        return bool(row["paused"])

    def wait_until_resumed(self) -> None:
        while self.is_paused():
            time.sleep(self.poll_seconds)

    def record_page(
        self,
        resource_name: str,
        next_offset: str | None,
        cursor_timestamp: str | None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO resource_progress (
                    resource_name, next_offset, cursor_timestamp, updated_at
                ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(resource_name) DO UPDATE SET
                    next_offset = excluded.next_offset,
                    cursor_timestamp = excluded.cursor_timestamp,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (resource_name, next_offset, cursor_timestamp),
            )

    def status(self) -> dict[str, Any]:
        with self._connect() as connection:
            paused_row = connection.execute(
                "SELECT paused FROM pipeline_control WHERE singleton = 1"
            ).fetchone()
            paused = 0 if paused_row is None else paused_row["paused"]
            progress = connection.execute(
                """
                SELECT resource_name, next_offset, cursor_timestamp, updated_at
                FROM resource_progress ORDER BY resource_name
                """
            ).fetchall()
        return {
            "paused": bool(paused),
            "resources": [dict(row) for row in progress],
        }