"""Data models for the CRM file event monitoring service."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass(slots=True)
class FileState:
    """Represents the observed state of a file at a point in time."""

    path: str
    modified_at: float
    size: int
    checksum: Optional[str] = None

    @classmethod
    def from_stat(
        cls, path: str, modified_at: float, size: int, checksum: Optional[str] = None
    ) -> "FileState":
        """Create a state instance from stat information."""
        return cls(path=path, modified_at=modified_at, size=size, checksum=checksum)


@dataclass(slots=True)
class FileEvent:
    """Represents a tracked file system event."""

    event_type: str
    path: str
    project: Optional[str]
    username: Optional[str]
    file_size: Optional[int]
    checksum: Optional[str]
    details: Optional[str] = None
    event_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_db_row(self) -> Dict[str, object]:
        """Convert the event into a mapping that can be written to the database."""
        return {
            "event_time": self.event_time.isoformat(),
            "event_type": self.event_type,
            "path": self.path,
            "project": self.project,
            "username": self.username,
            "file_size": self.file_size,
            "checksum": self.checksum,
            "details": self.details,
        }
