"""Database helpers for persisting file events and analytics."""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from .config import DatabaseConfig
from .models import CallMetricSnapshot, CallQualityInsight, FileEvent

LOGGER = logging.getLogger(__name__)


class EventDatabase:
    """SQLite backed event storage."""

    def __init__(self, config: Union[DatabaseConfig, Path]) -> None:
        if isinstance(config, DatabaseConfig):
            self._config = config
        else:
            self._config = DatabaseConfig(path=Path(config))

        self.path = Path(self._config.path)
        if self.path.parent and not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        timeout_seconds = (
            float(self._config.busy_timeout) / 1000.0
            if self._config.busy_timeout
            else 5.0
        )
        self._connection = sqlite3.connect(
            self.path, check_same_thread=False, timeout=timeout_seconds
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._prepare()
        if self._config.vacuum_on_start:
            self.vacuum()

    def _prepare(self) -> None:
        LOGGER.debug("Preparing database at %s", self.path)
        self._apply_pragmas()
        with self._connection:
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
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS call_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    queue TEXT NOT NULL,
                    total_calls INTEGER NOT NULL,
                    active_calls INTEGER NOT NULL,
                    avg_handle_time REAL NOT NULL,
                    service_level REAL NOT NULL,
                    customer_satisfaction REAL NOT NULL,
                    abandonment_rate REAL NOT NULL,
                    escalation_rate REAL,
                    nps REAL,
                    first_call_resolution REAL,
                    after_call_work REAL,
                    update_source TEXT
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_call_metrics_queue ON call_metrics(queue)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_call_metrics_captured ON call_metrics(captured_at)"
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS call_quality_insights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    agent TEXT,
                    customer TEXT,
                    language TEXT,
                    sentiment TEXT NOT NULL,
                    sentiment_score REAL NOT NULL,
                    dominant_emotion TEXT,
                    keywords TEXT,
                    key_phrases TEXT,
                    summary TEXT,
                    transcript TEXT,
                    duration_seconds REAL,
                    ai_flags TEXT,
                    UNIQUE(call_id, captured_at)
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_call_quality_captured ON call_quality_insights(captured_at)"
            )

    def _apply_pragmas(self) -> None:
        pragmas: Dict[str, Any] = {}
        default_journal = self._config.journal_mode or "WAL"
        pragmas["journal_mode"] = default_journal
        if self._config.synchronous:
            pragmas["synchronous"] = self._config.synchronous
        for key, value in self._config.pragmas.items():
            pragmas[key] = value

        for key, value in pragmas.items():
            formatted = self._format_pragma_value(value)
            self._connection.execute(f"PRAGMA {key} = {formatted}")

        if self._config.busy_timeout is not None:
            busy_timeout = max(0, int(self._config.busy_timeout))
            self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout}")

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

    def insert_call_metric(self, metric: CallMetricSnapshot) -> None:
        """Persist a call centre KPI snapshot."""

        row = metric.as_db_row()
        with self._lock:
            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO call_metrics (
                        captured_at,
                        queue,
                        total_calls,
                        active_calls,
                        avg_handle_time,
                        service_level,
                        customer_satisfaction,
                        abandonment_rate,
                        escalation_rate,
                        nps,
                        first_call_resolution,
                        after_call_work,
                        update_source
                    ) VALUES (
                        :captured_at,
                        :queue,
                        :total_calls,
                        :active_calls,
                        :avg_handle_time,
                        :service_level,
                        :customer_satisfaction,
                        :abandonment_rate,
                        :escalation_rate,
                        :nps,
                        :first_call_resolution,
                        :after_call_work,
                        :update_source
                    )
                    """,
                    row,
                )

    def fetch_latest_metrics_by_queue(self) -> List[Dict[str, Any]]:
        """Return the latest KPI snapshot for each queue."""

        sql = """
            SELECT cm.*
            FROM call_metrics cm
            JOIN (
                SELECT queue, MAX(captured_at) AS captured_at
                FROM call_metrics
                GROUP BY queue
            ) latest
            ON latest.queue = cm.queue AND latest.captured_at = cm.captured_at
            ORDER BY cm.queue ASC
        """
        with self._lock:
            cursor = self._connection.execute(sql)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_metrics_history(
        self,
        *,
        limit: int = 72,
        queue: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return a historical window of KPI snapshots."""

        parameters: List[Any] = []
        query = [
            "SELECT captured_at, queue, total_calls, active_calls, avg_handle_time,",
            "       service_level, customer_satisfaction, abandonment_rate,",
            "       escalation_rate, nps, first_call_resolution, after_call_work, update_source",
            "FROM call_metrics",
        ]
        if queue:
            query.append("WHERE queue = ?")
            parameters.append(queue)
        query.append("ORDER BY captured_at DESC")
        query.append("LIMIT ?")
        parameters.append(limit)

        sql = " ".join(query)
        with self._lock:
            cursor = self._connection.execute(sql, parameters)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def insert_call_quality_insight(self, insight: CallQualityInsight) -> None:
        """Persist AI quality analysis results."""

        row = insight.as_db_row()
        with self._lock:
            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO call_quality_insights (
                        call_id,
                        captured_at,
                        agent,
                        customer,
                        language,
                        sentiment,
                        sentiment_score,
                        dominant_emotion,
                        keywords,
                        key_phrases,
                        summary,
                        transcript,
                        duration_seconds,
                        ai_flags
                    ) VALUES (
                        :call_id,
                        :captured_at,
                        :agent,
                        :customer,
                        :language,
                        :sentiment,
                        :sentiment_score,
                        :dominant_emotion,
                        :keywords,
                        :key_phrases,
                        :summary,
                        :transcript,
                        :duration_seconds,
                        :ai_flags
                    )
                    ON CONFLICT(call_id, captured_at) DO UPDATE SET
                        agent=excluded.agent,
                        customer=excluded.customer,
                        language=excluded.language,
                        sentiment=excluded.sentiment,
                        sentiment_score=excluded.sentiment_score,
                        dominant_emotion=excluded.dominant_emotion,
                        keywords=excluded.keywords,
                        key_phrases=excluded.key_phrases,
                        summary=excluded.summary,
                        transcript=excluded.transcript,
                        duration_seconds=excluded.duration_seconds,
                        ai_flags=excluded.ai_flags
                    """,
                    row,
                )

    def fetch_recent_call_quality(
        self,
        *,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Return the latest analysed call transcripts."""

        sql = """
            SELECT
                call_id,
                captured_at,
                agent,
                customer,
                language,
                sentiment,
                sentiment_score,
                dominant_emotion,
                keywords,
                key_phrases,
                summary,
                transcript,
                duration_seconds,
                ai_flags
            FROM call_quality_insights
            ORDER BY captured_at DESC
            LIMIT ?
        """
        with self._lock:
            cursor = self._connection.execute(sql, (limit,))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_call_quality_for_call(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Return AI insights for a particular call, if available."""

        sql = """
            SELECT
                call_id,
                captured_at,
                agent,
                customer,
                language,
                sentiment,
                sentiment_score,
                dominant_emotion,
                keywords,
                key_phrases,
                summary,
                transcript,
                duration_seconds,
                ai_flags
            FROM call_quality_insights
            WHERE call_id = ?
            ORDER BY captured_at DESC
            LIMIT 1
        """
        with self._lock:
            cursor = self._connection.execute(sql, (call_id,))
            row = cursor.fetchone()
        return dict(row) if row else None

    def fetch_events(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        project: Optional[str] = None,
        username: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Return a slice of events ordered from newest to oldest."""

        conditions = []
        parameters: List[Any] = []
        if project:
            conditions.append("project = ?")
            parameters.append(project)
        if username:
            conditions.append("username = ?")
            parameters.append(username)
        if since:
            conditions.append("event_time >= ?")
            parameters.append(since.isoformat())

        query = [
            "SELECT event_time, event_type, path, project, username, file_size, checksum, details",
            "FROM file_events",
        ]
        if conditions:
            query.append("WHERE " + " AND ".join(conditions))
        query.append("ORDER BY event_time DESC")
        query.append("LIMIT ? OFFSET ?")
        parameters.extend([limit, offset])

        sql = " ".join(query)
        with self._lock:
            cursor = self._connection.execute(sql, parameters)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def purge_older_than(
        self,
        threshold: datetime,
        *,
        limit: Optional[int] = None,
    ) -> int:
        """Delete events older than ``threshold`` and return the number removed."""

        LOGGER.debug(
            "Purging events older than %s (limit=%s)", threshold.isoformat(), limit
        )
        cutoff = threshold.isoformat()
        with self._lock:
            if limit is not None and limit > 0:
                query = (
                    "DELETE FROM file_events WHERE id IN ("
                    "SELECT id FROM file_events WHERE event_time < ? "
                    "ORDER BY event_time ASC LIMIT ?"
                    ")"
                )
                parameters: Iterable[Any] = (cutoff, limit)
            else:
                query = "DELETE FROM file_events WHERE event_time < ?"
                parameters = (cutoff,)

            with self._connection:
                cursor = self._connection.execute(query, parameters)
            deleted = cursor.rowcount or 0
            if deleted < 0:
                deleted = 0
        return deleted

    def vacuum(self) -> None:
        """Run ``VACUUM`` on the SQLite database to reclaim free pages."""

        with self._lock:
            LOGGER.debug("Running VACUUM on %s", self.path)
            self._connection.commit()
            self._connection.execute("VACUUM")

    def fetch_newer_events(
        self,
        *,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return events newer than ``since`` ordered from oldest to newest."""

        parameters: List[Any] = []
        query = [
            "SELECT event_time, event_type, path, project, username, file_size, checksum, details",
            "FROM file_events",
        ]

        if since:
            query.append("WHERE event_time > ?")
            parameters.append(since.isoformat())

        query.append("ORDER BY event_time ASC")
        query.append("LIMIT ?")
        parameters.append(limit)

        sql = " ".join(query)
        with self._lock:
            cursor = self._connection.execute(sql, parameters)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        """Close the underlying database connection."""
        LOGGER.debug("Closing database connection")
        self._connection.close()

    @staticmethod
    def _format_pragma_value(value: Any) -> str:
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        return str(value)
