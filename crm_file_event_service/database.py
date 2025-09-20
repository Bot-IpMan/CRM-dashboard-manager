"""Database helpers for persisting file events."""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Iterable

from .models import FileEvent

LOGGER = logging.getLogger(__name__)


class EventDatabase:
    """SQLite backed event storage."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        if self.path.parent and not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._prepare()

    def _prepare(self) -> None:
        LOGGER.debug("Preparing database at %s", self.path)
        with self._connection:
            self._connection.execute("PRAGMA journal_mode=WAL;")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS file_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_time TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    project TEXT,
                    username TEXT,
                    file_size INTEGER,
                    checksum TEXT,
                    details TEXT
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_file_events_time ON file_events(event_time)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_file_events_path ON file_events(path)"
            )

    def insert_event(self, event: FileEvent) -> None:
        """Insert a single event into the database."""
        row = event.as_db_row()
        with self._lock:
            LOGGER.debug("Persisting event: %s", row)
            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO file_events (
                        event_time, event_type, path, project, username, file_size, checksum, details
                    ) VALUES (:event_time, :event_type, :path, :project, :username, :file_size, :checksum, :details)
                    """,
                    row,
                )

    def insert_events(self, events: Iterable[FileEvent]) -> None:
        """Insert multiple events in a single transaction."""
        rows = [event.as_db_row() for event in events]
        if not rows:
            return
        with self._lock:
            LOGGER.debug("Persisting %s events", len(rows))
            with self._connection:
                self._connection.executemany(
                    """
                    INSERT INTO file_events (
                        event_time, event_type, path, project, username, file_size, checksum, details
                    ) VALUES (:event_time, :event_type, :path, :project, :username, :file_size, :checksum, :details)
                    """,
                    rows,
                )

    def close(self) -> None:
        """Close the underlying database connection."""
        LOGGER.debug("Closing database connection")
        self._connection.close()
