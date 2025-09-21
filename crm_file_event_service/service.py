"""Service entry point for the CRM file event monitor."""
from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from .config import DirectoryConfig, ServiceConfig
from .database import EventDatabase
from .models import FileEvent
from .watcher import DirectoryWatcher, WatchfilesDirectoryWatcher

LOGGER = logging.getLogger(__name__)


@dataclass
class ManagedWatcher:
    """Wraps a watcher together with its scheduling metadata."""

    watcher: DirectoryWatcher
    interval: float
    next_poll: float = 0.0


class FileEventService:
    """Background service that polls watchers and persists file events."""

    def __init__(self, config: ServiceConfig) -> None:
        self.config = config
        self.database = EventDatabase(config.database)
        self.watchers: List[ManagedWatcher] = [
            ManagedWatcher(
                watcher=self._create_watcher(directory),
                interval=directory.poll_interval or config.poll_interval,
            )
            for directory in config.directories
        ]
        self._stop_event = threading.Event()
        if not self.watchers:
            raise ValueError("Service cannot start without configured watchers")
        self._max_batch_size = config.max_events_per_batch
        self._maintenance_interval = config.database.maintenance_interval
        self._maintenance_batch_size = config.database.maintenance_batch_size
        self._vacuum_on_maintenance = config.database.vacuum_on_maintenance
        self._retention_days = config.database.retention_days
        maintenance_required = (
            self._retention_days is not None or self._vacuum_on_maintenance
        )
        if maintenance_required:
            if config.database.maintenance_on_start:
                initial_delay = 0.0
            elif self._maintenance_interval is not None:
                initial_delay = self._maintenance_interval
            else:
                initial_delay = None
            self._next_maintenance = (
                time.monotonic() + initial_delay if initial_delay is not None else None
            )
        else:
            self._next_maintenance = None

    def start(self) -> None:
        """Run the service loop until stop is requested."""
        LOGGER.info("Starting FileEventService with %s watchers", len(self.watchers))
        self._install_signal_handlers()
        try:
            while not self._stop_event.is_set():
                sleep_for = self._run_iteration()
                if sleep_for is None:
                    # No watcher scheduled yet; fall back to default interval
                    sleep_for = self.config.poll_interval
                LOGGER.debug("Sleeping for %.2fs", sleep_for)
                idle_sleep = (
                    self.config.idle_sleep_interval
                    if self.config.idle_sleep_interval > 0
                    else 0.1
                )
                self._stop_event.wait(timeout=max(sleep_for, idle_sleep))
        finally:
            self._shutdown_watchers()
            self.database.close()
            LOGGER.info("FileEventService stopped")

    def run_once(self) -> None:
        """Execute a single polling cycle.

        This helper is primarily intended for smoke tests and manual execution
        where a long running background loop is not required.
        """
        try:
            self._run_iteration()
        finally:
            self._shutdown_watchers()
            self.database.close()

    def stop(self, *_args: object) -> None:
        """Request the service loop to stop."""
        LOGGER.info("Stop requested")
        self._stop_event.set()

    def _run_iteration(self) -> Optional[float]:
        now = time.monotonic()
        next_deadline: Optional[float] = None

        for managed in self.watchers:
            if now >= managed.next_poll:
                LOGGER.debug("Polling watcher for %s", managed.watcher.config.path)
                events = managed.watcher.poll()
                self._handle_events(events)
                managed.next_poll = now + managed.interval
            if next_deadline is None or managed.next_poll < next_deadline:
                next_deadline = managed.next_poll

        maintenance_deadline = self._maybe_schedule_maintenance(now)
        if maintenance_deadline is not None:
            if next_deadline is None or maintenance_deadline < next_deadline:
                next_deadline = maintenance_deadline

        if next_deadline is None:
            return None
        return max(0.0, next_deadline - now)

    def _handle_events(self, events: List[FileEvent]) -> None:
        if not events:
            return
        LOGGER.info("Persisting %s events", len(events))
        max_batch = self._max_batch_size
        if not max_batch or max_batch <= 0:
            self.database.insert_events(events)
            return
        for index in range(0, len(events), max_batch):
            batch = events[index : index + max_batch]
            self.database.insert_events(batch)

    def _create_watcher(self, directory: DirectoryConfig) -> DirectoryWatcher:
        if directory.backend == "watchfiles":
            return WatchfilesDirectoryWatcher(
                directory, checksum_algorithm=self.config.checksum_algorithm
            )
        return DirectoryWatcher(directory, checksum_algorithm=self.config.checksum_algorithm)

    def _shutdown_watchers(self) -> None:
        for managed in self.watchers:
            watcher = managed.watcher
            try:
                watcher.close()
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.debug(
                    "Error while closing watcher for %s: %s",
                    watcher.config.path,
                    exc,
                )

    def _maybe_schedule_maintenance(self, now: float) -> Optional[float]:
        deadline = self._next_maintenance
        if deadline is None:
            return None
        if now >= deadline:
            self._perform_maintenance()
            deadline = self._next_maintenance
        return deadline

    def _perform_maintenance(self) -> None:
        LOGGER.debug("Running scheduled maintenance tasks")
        if self._maintenance_interval:
            self._next_maintenance = time.monotonic() + self._maintenance_interval
        else:
            self._next_maintenance = None

        deleted = 0
        if self._retention_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
            deleted = self.database.purge_older_than(
                cutoff,
                limit=self._maintenance_batch_size,
            )
            if deleted:
                LOGGER.info(
                    "Purged %s events older than %s", deleted, cutoff.isoformat()
                )
            else:
                LOGGER.debug(
                    "No events older than %s to purge", cutoff.isoformat()
                )

        if self._vacuum_on_maintenance and (deleted or self._retention_days is None):
            LOGGER.info("Running VACUUM as part of maintenance")
            self.database.vacuum()

    def _install_signal_handlers(self) -> None:
        try:
            signal.signal(signal.SIGTERM, self.stop)
            signal.signal(signal.SIGINT, self.stop)
        except ValueError:
            # Signal handlers can only be installed in the main thread; fall back silently.
            LOGGER.debug("Signal handlers could not be installed (non-main thread)")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
