"""Directory watching logic for the CRM file event service."""
from __future__ import annotations

import fnmatch
import hashlib
import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List

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

    def _build_snapshot(self) -> Dict[str, FileState]:
        snapshot: Dict[str, FileState] = {}
        root = self.config.path
        if not root.exists():
            LOGGER.warning("Watched path does not exist: %s", root.as_posix())
            return snapshot

        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            # Apply exclusions to directories to avoid unnecessary traversal.
            dirnames[:] = [
                d
                for d in dirnames
                if self._should_consider(Path(dirpath) / d, is_dir=True)
            ]
            for filename in filenames:
                path = Path(dirpath) / filename
                if not self._should_consider(path):
                    continue
                try:
                    stat = path.stat()
                except OSError as exc:  # pragma: no cover - platform specific
                    LOGGER.debug("Skipping %s due to stat error: %s", path, exc)
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
        details = f"watched_root={self.config.path}"
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


def poll_all(watchers: Iterable[DirectoryWatcher]) -> List[FileEvent]:
    """Convenience helper that polls all watchers and flattens their events."""
    events: List[FileEvent] = []
    for watcher in watchers:
        events.extend(watcher.poll())
    return events
