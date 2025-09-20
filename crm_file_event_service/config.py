"""Configuration helpers for the CRM file event monitoring service."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass(slots=True)
class DirectoryConfig:
    """Configuration of a watched directory."""

    path: Path
    project: Optional[str] = None
    username: Optional[str] = None
    include: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)
    poll_interval: Optional[float] = None
    compute_checksum: bool = False
    emit_on_start: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DirectoryConfig":
        path = Path(data["path"]).expanduser().resolve()
        include = _ensure_list(data.get("include", []))
        exclude = _ensure_list(data.get("exclude", []))
        return cls(
            path=path,
            project=data.get("project"),
            username=data.get("username"),
            include=include,
            exclude=exclude,
            poll_interval=_to_float_or_none(data.get("poll_interval")),
            compute_checksum=bool(data.get("compute_checksum", False)),
            emit_on_start=bool(data.get("emit_on_start", False)),
        )


@dataclass(slots=True)
class ServiceConfig:
    """Top level configuration object."""

    database_path: Path
    poll_interval: float = 5.0
    checksum_algorithm: str = "md5"
    directories: List[DirectoryConfig] = field(default_factory=list)
    log_level: str = "INFO"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServiceConfig":
        if "database" not in data:
            raise ValueError("Configuration missing 'database' section")

        db_section = data["database"]
        database_path = Path(db_section.get("path", "file_events.db")).expanduser().resolve()

        directories = [DirectoryConfig.from_dict(entry) for entry in data.get("directories", [])]
        if not directories:
            raise ValueError("Configuration must include at least one directory entry")

        poll_interval = _to_float_or_none(data.get("poll_interval")) or 5.0

        checksum_algorithm = data.get("checksum_algorithm", "md5")
        log_level = data.get("log_level", "INFO")

        return cls(
            database_path=database_path,
            poll_interval=poll_interval,
            checksum_algorithm=checksum_algorithm,
            directories=directories,
            log_level=log_level,
        )


def load_config(path: Path) -> ServiceConfig:
    """Load the configuration from a JSON file."""
    path = path.expanduser().resolve()
    with path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return ServiceConfig.from_dict(raw)


def _ensure_list(value: Any) -> List[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable):
        return [str(item) for item in value]
    return []


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        result = float(value)
        return result if result > 0 else None
    except (TypeError, ValueError):
        return None
