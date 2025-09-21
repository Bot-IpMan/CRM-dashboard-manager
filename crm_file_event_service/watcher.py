"""Directory watching logic for the CRM file event service."""
from __future__ import annotations

import fnmatch
import hashlib
import logging
import os
import threading
from queue import Empty, Queue
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .config import DirectoryConfig
from .models import FileEvent, FileState

LOGGER = logging.getLogger(__name__)


class DirectoryWatcher:
    """Poll-based watcher that tracks file changes for a directory."""

    def __init__(self, config: DirectoryConfig, checksum_algorithm: str = "md5") -> None:
        self.config = config
        self.checksum_algorithm = checksum_algorithm
        self._snapshot: Dict[str, FileState] = {}
        self._initialised = False
        if self.config.compute_checksum:
            # Validate algorithm eagerly to fail fast on misconfiguration.
            hashlib.new(self.checksum_algorithm)

    def poll(self) -> List[FileEvent]:
        """Poll the filesystem and return any new events."""
        new_snapshot = self._build_snapshot()
        events: List[FileEvent] = []

        if not self._initialised:
            self._initialised = True
            if self.config.emit_on_start:
                events = self._diff({}, new_snapshot)
            self._snapshot = new_snapshot
            return events

        events = self._diff(self._snapshot, new_snapshot)
        self._snapshot = new_snapshot
        return events

    def close(self) -> None:  # pragma: no cover - default implementation
        """Release any resources held by the watcher."""

    def _build_snapshot(self) -> Dict[str, FileState]:
        snapshot: Dict[str, FileState] = {}
        root = self.config.path
        if not root.exists():
            LOGGER.warning("Watched path does not exist: %s", root.as_posix())
            return snapshot

        for dirpath, dirnames, filenames in os.walk(
            root, followlinks=self.config.follow_symlinks
        ):
            # Apply exclusions to directories to avoid unnecessary traversal.
            if self.config.recursive:
                dirnames[:] = [
                    d
                    for d in dirnames
                    if self._should_consider(Path(dirpath) / d, is_dir=True)
                ]
            else:
                dirnames[:] = []
            for filename in filenames:
                path = Path(dirpath) / filename
                if not self._should_consider(path):
                    continue
                try:
                    stat = path.stat()
                except OSError as exc:  # pragma: no cover - platform specific
                    LOGGER.debug("Skipping %s due to stat error: %s", path, exc)
                    continue

                if not self._passes_size_filter(stat.st_size):
                    continue

                checksum = self._compute_checksum(path) if self.config.compute_checksum else None
                snapshot[str(path)] = FileState.from_stat(
                    path=str(path),
                    modified_at=stat.st_mtime,
                    size=stat.st_size,
                    checksum=checksum,
                )
        return snapshot

    def _diff(
        self, previous: Dict[str, FileState], current: Dict[str, FileState]
    ) -> List[FileEvent]:
        events: List[FileEvent] = []
        previous_keys = set(previous)
        current_keys = set(current)

        created = current_keys - previous_keys
        deleted = previous_keys - current_keys
        maybe_modified = previous_keys & current_keys

        for path in sorted(created):
            state = current[path]
            events.append(self._build_event("created", state))

        for path in sorted(deleted):
            state = previous[path]
            events.append(self._build_event("deleted", state))

        for path in sorted(maybe_modified):
            old_state = previous[path]
            new_state = current[path]
            if self._has_changed(old_state, new_state):
                events.append(self._build_event("modified", new_state))

        return events

    def _should_consider(self, path: Path, *, is_dir: bool = False) -> bool:
        relative = self._relative_path(path)
        if relative is None:
            return False

        match_target = relative.as_posix()
        if self.config.ignore_hidden and self._is_hidden(relative):
            return False
        if is_dir:
            match_target = f"{match_target}/"

        for pattern in self.config.exclude:
            if fnmatch.fnmatch(match_target, pattern) or fnmatch.fnmatch(path.name, pattern):
                return False

        if not self.config.include:
            return True

        return any(
            fnmatch.fnmatch(match_target, pattern) or fnmatch.fnmatch(path.name, pattern)
            for pattern in self.config.include
        )

    def _relative_path(self, path: Path) -> Path | None:
        try:
            return path.resolve().relative_to(self.config.path)
        except (OSError, RuntimeError) as exc:
            LOGGER.debug("Unable to resolve path %s: %s", path, exc)
            return None
        except ValueError:
            LOGGER.debug("Path %s is outside of watched root %s", path, self.config.path)
            return None

    def _build_event(self, event_type: str, state: FileState) -> FileEvent:
        details_parts = [f"watched_root={self.config.path}"]
        if self.config.metadata:
            details_parts.extend(
                f"{key}={value}" for key, value in sorted(self.config.metadata.items())
            )
        details = "; ".join(details_parts)
        return FileEvent(
            event_type=event_type,
            path=state.path,
            project=self.config.project,
            username=self.config.username,
            file_size=state.size,
            checksum=state.checksum,
            details=details,
        )

    def _has_changed(self, old_state: FileState, new_state: FileState) -> bool:
        if old_state.size != new_state.size:
            return True
        if old_state.modified_at != new_state.modified_at:
            return True
        if self.config.compute_checksum and old_state.checksum != new_state.checksum:
            return True
        return False

    def _is_hidden(self, relative: Path) -> bool:
        for part in relative.parts:
            if part in {"", "."}:
                continue
            if part.startswith("."):
                return True
        return False

    def _compute_checksum(self, path: Path) -> str | None:
        try:
            hasher = hashlib.new(self.checksum_algorithm)
        except ValueError as exc:  # pragma: no cover - validated earlier
            LOGGER.error("Unsupported checksum algorithm '%s': %s", self.checksum_algorithm, exc)
            return None

        try:
            with path.open("rb") as fh:
                for chunk in iter(lambda: fh.read(65536), b""):
                    hasher.update(chunk)
        except OSError as exc:
            LOGGER.debug("Unable to compute checksum for %s: %s", path, exc)
            return None

        return hasher.hexdigest()

    def _passes_size_filter(self, size: int) -> bool:
        minimum = self.config.min_file_size
        if minimum is not None and size < minimum:
            return False
        maximum = self.config.max_file_size
        if maximum is not None and size > maximum:
            return False
        return True


class WatchfilesDirectoryWatcher(DirectoryWatcher):
    """Directory watcher backed by the ``watchfiles`` library."""

    def __init__(self, config: DirectoryConfig, checksum_algorithm: str = "md5") -> None:
        super().__init__(config, checksum_algorithm=checksum_algorithm)
        self._event_queue: "Queue[FileEvent]" = Queue()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._initialised_once = False
        self._closed = False
        self._watch_callable: Any = None
        self._change_map: Dict[Any, str] = {}
        self._load_watchfiles()

    def poll(self) -> List[FileEvent]:
        if not self._initialised_once:
            initial_events = super().poll()
            self._initialised_once = True
            self._ensure_thread()
            initial_events.extend(self._drain_queue())
            return initial_events

        self._ensure_thread()
        return self._drain_queue()

    def close(self) -> None:
        self._closed = True
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5.0)
        self._thread = None

    def _load_watchfiles(self) -> None:
        try:
            from watchfiles import Change, watch  # type: ignore import-not-found
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "The 'watchfiles' backend requires the optional 'watchfiles' package"
            ) from exc

        self._watch_callable = watch
        self._change_map = {
            Change.added: "created",
            Change.modified: "modified",
            Change.deleted: "deleted",
        }

    def _ensure_thread(self) -> None:
        if self._closed:
            return
        thread = self._thread
        if thread and thread.is_alive():
            return
        if thread and not thread.is_alive():
            LOGGER.warning(
                "Watchfiles thread for %s stopped unexpectedly; restarting",
                self.config.path,
            )
            new_snapshot = self._build_snapshot()
            lost_events = self._diff(self._snapshot, new_snapshot)
            self._snapshot = new_snapshot
            for event in lost_events:
                self._event_queue.put(event)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._watch_loop,
            name=f"watchfiles:{self.config.path}",
            daemon=True,
        )
        self._thread.start()

    def _watch_loop(self) -> None:
        assert self._watch_callable is not None
        try:
            for changes in self._watch_callable(
                self.config.path,
                recursive=self.config.recursive,
                stop_event=self._stop_event,
            ):
                if self._stop_event.is_set():
                    break
                events = self._process_changes(changes)
                for event in events:
                    self._event_queue.put(event)
        except Exception as exc:  # pragma: no cover - runtime safeguard
            LOGGER.exception(
                "Watchfiles watcher for %s stopped due to error: %s",
                self.config.path,
                exc,
            )

    def _process_changes(self, changes: Iterable[Any]) -> List[FileEvent]:
        events: List[FileEvent] = []
        for change, raw_path in changes:
            event_type = self._change_map.get(change)
            if event_type is None:
                continue

            path = Path(raw_path)
            if event_type == "deleted":
                state = self._snapshot.pop(str(path), None)
                if state is not None:
                    events.append(self._build_event("deleted", state))
                continue

            if not path.exists() or not path.is_file():
                continue
            if not self._should_consider(path):
                continue

            try:
                stat = path.stat()
            except OSError as exc:
                LOGGER.debug("Skipping %s due to stat error: %s", path, exc)
                continue

            if not self._passes_size_filter(stat.st_size):
                continue

            checksum = self._compute_checksum(path) if self.config.compute_checksum else None
            state = FileState.from_stat(
                path=str(path),
                modified_at=stat.st_mtime,
                size=stat.st_size,
                checksum=checksum,
            )
            self._snapshot[str(path)] = state
            events.append(self._build_event(event_type, state))
        return events

    def _drain_queue(self) -> List[FileEvent]:
        events: List[FileEvent] = []
        while True:
            try:
                events.append(self._event_queue.get_nowait())
            except Empty:
                break
        return events


def poll_all(watchers: Iterable[DirectoryWatcher]) -> List[FileEvent]:
    """Convenience helper that polls all watchers and flattens their events."""
    events: List[FileEvent] = []
    for watcher in watchers:
        events.extend(watcher.poll())
    return events
