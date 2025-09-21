"""Configuration helpers for the CRM file event monitoring service."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass(slots=True)
class DatabaseConfig:
    """Configuration of the SQLite database backend."""

    path: Path
    busy_timeout: Optional[int] = None
    journal_mode: Optional[str] = None
    synchronous: Optional[str] = None
    pragmas: Dict[str, str] = field(default_factory=dict)
    retention_days: Optional[int] = None
    maintenance_interval: Optional[float] = None
    maintenance_batch_size: Optional[int] = 500
    maintenance_on_start: bool = False
    vacuum_on_start: bool = False
    vacuum_on_maintenance: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseConfig":
        path = Path(data.get("path", "file_events.db")).expanduser().resolve()

        busy_timeout = _to_int_or_none(data.get("busy_timeout"))

        journal_mode_raw = data.get("journal_mode")
        journal_mode = str(journal_mode_raw).upper() if journal_mode_raw else None

        synchronous_raw = data.get("synchronous")
        synchronous = str(synchronous_raw).upper() if synchronous_raw else None

        retention_days = _to_int_or_none(data.get("retention_days"))
        if retention_days is not None and retention_days <= 0:
            retention_days = None

        raw_batch_size = data.get("maintenance_batch_size")
        if raw_batch_size is None:
            maintenance_batch_size = 500
        else:
            maintenance_batch_size = _to_int_or_none(raw_batch_size)
            if maintenance_batch_size is None:
                raise ValueError(
                    "'maintenance_batch_size' must be a non-negative integer"
                )
            if maintenance_batch_size == 0:
                maintenance_batch_size = None

        maintenance_on_start = _to_bool(data.get("maintenance_on_start"), False)
        vacuum_on_start = _to_bool(data.get("vacuum_on_start"), False)
        vacuum_on_maintenance = _to_bool(data.get("vacuum_on_maintenance"), False)

        maintenance_interval = _to_float_or_none(data.get("maintenance_interval"))
        if maintenance_interval is None and (
            retention_days is not None or vacuum_on_maintenance
        ):
            maintenance_interval = 3600.0

        pragmas = _ensure_str_mapping(data.get("pragmas", {}))

        return cls(
            path=path,
            busy_timeout=busy_timeout,
            journal_mode=journal_mode,
            synchronous=synchronous,
            pragmas=pragmas,
            retention_days=retention_days,
            maintenance_interval=maintenance_interval,
            maintenance_batch_size=maintenance_batch_size,
            maintenance_on_start=maintenance_on_start,
            vacuum_on_start=vacuum_on_start,
            vacuum_on_maintenance=vacuum_on_maintenance,
        )


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
    backend: str = "polling"
    min_file_size: Optional[int] = None
    max_file_size: Optional[int] = None
    recursive: bool = True
    follow_symlinks: bool = False
    ignore_hidden: bool = False
    metadata: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DirectoryConfig":
        path = Path(data["path"]).expanduser().resolve()
        include = _ensure_list(data.get("include", []))
        exclude = _ensure_list(data.get("exclude", []))
        backend = str(data.get("backend", "polling")).lower()
        if backend not in {"polling", "watchfiles"}:
            raise ValueError(
                "Directory configuration 'backend' must be either 'polling' or 'watchfiles'"
            )
        min_file_size = _to_int_or_none(data.get("min_file_size"))
        max_file_size = _to_int_or_none(data.get("max_file_size"))
        if (
            min_file_size is not None
            and max_file_size is not None
            and min_file_size > max_file_size
        ):
            raise ValueError("'min_file_size' cannot be greater than 'max_file_size'")
        recursive = _to_bool(data.get("recursive"), True)
        follow_symlinks = _to_bool(data.get("follow_symlinks"), False)
        ignore_hidden = _to_bool(data.get("ignore_hidden"), False)
        metadata = _ensure_str_mapping(data.get("metadata", {}))
        return cls(
            path=path,
            project=data.get("project"),
            username=data.get("username"),
            include=include,
            exclude=exclude,
            poll_interval=_to_float_or_none(data.get("poll_interval")),
            compute_checksum=_to_bool(data.get("compute_checksum"), False),
            emit_on_start=_to_bool(data.get("emit_on_start"), False),
            backend=backend,
            min_file_size=min_file_size,
            max_file_size=max_file_size,
            recursive=recursive,
            follow_symlinks=follow_symlinks,
            ignore_hidden=ignore_hidden,
            metadata=metadata,
        )


@dataclass(slots=True)
class ServiceConfig:
    """Top level configuration object."""

    database: DatabaseConfig
    poll_interval: float = 5.0
    checksum_algorithm: str = "md5"
    directories: List[DirectoryConfig] = field(default_factory=list)
    log_level: str = "INFO"
    max_events_per_batch: Optional[int] = None
    idle_sleep_interval: float = 0.1
    shutdown_grace_period: float = 5.0

    @property
    def database_path(self) -> Path:
        """Retain backwards compatibility with older code paths."""

        return self.database.path

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServiceConfig":
        if "database" not in data:
            raise ValueError("Configuration missing 'database' section")

        db_section = data["database"]
        database = DatabaseConfig.from_dict(db_section)

        directories = [DirectoryConfig.from_dict(entry) for entry in data.get("directories", [])]
        if not directories:
            raise ValueError("Configuration must include at least one directory entry")

        poll_interval = _to_float_or_none(data.get("poll_interval")) or 5.0

        checksum_algorithm = data.get("checksum_algorithm", "md5")
        log_level = data.get("log_level", "INFO")
        max_events_per_batch = _to_int_or_none(data.get("max_events_per_batch"))
        if max_events_per_batch == 0:
            max_events_per_batch = None
        idle_sleep_interval = _to_float_or_none(data.get("idle_sleep_interval")) or 0.1
        shutdown_grace_period = (
            _to_float_or_none(data.get("shutdown_grace_period"))
            or max(poll_interval * 2, 5.0)
        )

        return cls(
            database=database,
            poll_interval=poll_interval,
            checksum_algorithm=checksum_algorithm,
            directories=directories,
            log_level=log_level,
            max_events_per_batch=max_events_per_batch,
            idle_sleep_interval=idle_sleep_interval,
            shutdown_grace_period=shutdown_grace_period,
        )


def load_config(path: Path) -> ServiceConfig:
    """Load the configuration from a JSON file."""
    path = path.expanduser().resolve()
    with path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return ServiceConfig.from_dict(raw)


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in {"true", "1", "yes", "on"}:
            return True
        if normalised in {"false", "0", "no", "off"}:
            return False
    return default


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


def _to_int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        result = int(value)
        return result if result >= 0 else None
    except (TypeError, ValueError):
        return None


def _ensure_str_mapping(value: Any) -> Dict[str, str]:
    if isinstance(value, dict):
        return {str(key): str(val) for key, val in value.items()}
    if isinstance(value, (str, bytes)):
        return {}
    if isinstance(value, Iterable):
        mapping: Dict[str, str] = {}
        for item in value:
            if isinstance(item, (str, bytes)):
                return {}
            try:
                pair = tuple(item)
            except TypeError:
                return {}
            if len(pair) != 2:
                return {}
            key, val = pair
            mapping[str(key)] = str(val)
        return mapping
    return {}
