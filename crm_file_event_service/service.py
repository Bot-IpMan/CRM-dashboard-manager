"""Service entry point for the CRM file event monitor."""
from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass
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
        self.database = EventDatabase(config.database_path)
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
                self._stop_event.wait(timeout=max(sleep_for, 0.1))
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

        if next_deadline is None:
            return None
        return max(0.0, next_deadline - now)

    def _handle_events(self, events: List[FileEvent]) -> None:
        if not events:
            return
        LOGGER.info("Persisting %s events", len(events))
        self.database.insert_events(events)

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
