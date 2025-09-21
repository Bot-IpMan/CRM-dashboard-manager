"""Data models for the CRM file event monitoring service."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


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


@dataclass(slots=True)
class CallMetricSnapshot:
    """Represents a real-time KPI snapshot for a contact centre queue."""

    queue: str
    total_calls: int
    active_calls: int
    avg_handle_time: float
    service_level: float
    customer_satisfaction: float
    abandonment_rate: float
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    escalation_rate: Optional[float] = None
    nps: Optional[float] = None
    first_call_resolution: Optional[float] = None
    after_call_work: Optional[float] = None
    update_source: str = "zoom"

    def as_db_row(self) -> Dict[str, Any]:
        """Convert the snapshot into a mapping suitable for SQLite operations."""

        return {
            "captured_at": self.captured_at.isoformat(),
            "queue": self.queue,
            "total_calls": int(self.total_calls),
            "active_calls": int(self.active_calls),
            "avg_handle_time": float(self.avg_handle_time),
            "service_level": float(self.service_level),
            "customer_satisfaction": float(self.customer_satisfaction),
            "abandonment_rate": float(self.abandonment_rate),
            "escalation_rate": None
            if self.escalation_rate is None
            else float(self.escalation_rate),
            "nps": None if self.nps is None else float(self.nps),
            "first_call_resolution": None
            if self.first_call_resolution is None
            else float(self.first_call_resolution),
            "after_call_work": None
            if self.after_call_work is None
            else float(self.after_call_work),
            "update_source": self.update_source,
        }


@dataclass(slots=True)
class CallQualityInsight:
    """Represents AI powered analysis of a call transcript."""

    call_id: str
    sentiment: str
    sentiment_score: float
    dominant_emotion: Optional[str]
    keywords: List[str]
    key_phrases: List[str]
    summary: Optional[str]
    transcript: Optional[str]
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    agent: Optional[str] = None
    customer: Optional[str] = None
    language: Optional[str] = None
    duration_seconds: Optional[float] = None
    ai_flags: Dict[str, Any] = field(default_factory=dict)

    def as_db_row(self) -> Dict[str, Any]:
        """Convert the insight into a mapping suitable for SQLite storage."""

        return {
            "call_id": self.call_id,
            "captured_at": self.captured_at.isoformat(),
            "agent": self.agent,
            "customer": self.customer,
            "language": self.language,
            "sentiment": self.sentiment,
            "sentiment_score": float(self.sentiment_score),
            "dominant_emotion": self.dominant_emotion,
            "keywords": json.dumps(self.keywords, ensure_ascii=False),
            "key_phrases": json.dumps(self.key_phrases, ensure_ascii=False),
            "summary": self.summary,
            "transcript": self.transcript,
            "duration_seconds": None
            if self.duration_seconds is None
            else float(self.duration_seconds),
            "ai_flags": json.dumps(self.ai_flags, ensure_ascii=False),
        }
