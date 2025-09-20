"""FastAPI application for exposing file events and running the monitor service."""
from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict

from .config import ServiceConfig, load_config
from .database import EventDatabase
from .service import FileEventService, configure_logging
from .settings import resolve_config_path
STREAM_SLEEP_SECONDS = 1.0

LOGGER = logging.getLogger(__name__)


class EventPayload(BaseModel):
    """Serialised representation of a file system event."""

    model_config = ConfigDict(from_attributes=True)

    event_time: datetime
    event_type: str
    path: str
    project: Optional[str] = None
    username: Optional[str] = None
    file_size: Optional[int] = None
    checksum: Optional[str] = None
    details: Optional[str] = None


class ServiceRunner:
    """Utility that executes :class:`FileEventService` on a background thread."""

    def __init__(self, config: ServiceConfig) -> None:
        self._config = config
        self._thread: Optional[threading.Thread] = None
        self._service: Optional[FileEventService] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                LOGGER.debug("FileEventService already running")
                return
            LOGGER.info("Starting background FileEventService thread")
            self._service = FileEventService(self._config)
            self._thread = threading.Thread(
                target=self._service.start,
                name="file-event-service",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
            service = self._service
        if not thread or not service:
            return

        LOGGER.info("Stopping background FileEventService thread")
        service.stop()
        timeout = max(self._config.poll_interval * 2, 5.0)
        thread.join(timeout=timeout)
        if thread.is_alive():
            LOGGER.warning("FileEventService thread did not exit within %.1fs", timeout)

        with self._lock:
            self._thread = None
            self._service = None


def create_app(config_path: Optional[Path] = None) -> FastAPI:
    """Create a configured FastAPI application.

    Parameters
    ----------
    config_path:
        Optional path to the JSON configuration. When omitted, the path is loaded
        from the ``CRM_SERVICE_CONFIG`` environment variable or defaults to
        ``config.json`` in the working directory.
    """

    resolved_path = resolve_config_path(config_path)
    try:
        service_config = load_config(resolved_path)
    except FileNotFoundError as exc:  # pragma: no cover - depends on local setup
        raise RuntimeError(str(exc)) from exc
    except ValueError as exc:
        raise RuntimeError(f"Invalid configuration: {exc}") from exc

    configure_logging(service_config.log_level)
    LOGGER.info("Loaded configuration from %s", resolved_path)

    runner = ServiceRunner(service_config)

    app = FastAPI(title="CRM File Event Monitor", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    async def _on_startup() -> None:
        runner.start()

    @app.on_event("shutdown")
    async def _on_shutdown() -> None:
        runner.stop()

    def get_database() -> Iterator[EventDatabase]:
        database = EventDatabase(service_config.database_path)
        try:
            yield database
        finally:
            database.close()

    @app.get("/health")
    async def healthcheck() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/events", response_model=List[EventPayload])
    async def list_events(
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        project: Optional[str] = Query(None),
        username: Optional[str] = Query(None),
        since: Optional[datetime] = Query(None),
        db: EventDatabase = Depends(get_database),
    ) -> List[EventPayload]:
        rows = db.fetch_events(
            limit=limit,
            offset=offset,
            project=project,
            username=username,
            since=since,
        )
        return [_row_to_payload(row) for row in rows]

    @app.websocket("/ws/events")
    async def events_stream(websocket: WebSocket) -> None:
        await websocket.accept()
        last_seen: Optional[datetime] = None
        database = EventDatabase(service_config.database_path)
        try:
            while True:
                rows = database.fetch_newer_events(since=last_seen)
                if rows:
                    events = [_row_to_payload(row) for row in rows]
                    last_seen = events[-1].event_time
                    payload = [_serialise_event(event) for event in events]
                    await websocket.send_json(payload)
                await asyncio.sleep(STREAM_SLEEP_SECONDS)
        except WebSocketDisconnect:
            LOGGER.debug("WebSocket disconnected")
        except Exception as exc:  # pragma: no cover - runtime safeguard
            LOGGER.exception("Error while streaming events: %s", exc)
            await websocket.close(code=1011, reason="internal server error")
        finally:
            database.close()

    return app


def _row_to_payload(row: Dict[str, Any]) -> EventPayload:
    data = dict(row)
    event_time = data.get("event_time")
    if isinstance(event_time, str):
        data["event_time"] = datetime.fromisoformat(event_time)
    return EventPayload(**data)


def _serialise_event(event: EventPayload) -> Dict[str, Any]:
    payload = event.model_dump() if hasattr(event, "model_dump") else event.dict()
    payload["event_time"] = event.event_time.isoformat()
    return payload


try:  # pragma: no cover - depends on runtime environment
    app = create_app()
except RuntimeError as exc:
    LOGGER.error("Unable to initialise FastAPI application: %s", exc)
    error_message = str(exc)

    app = FastAPI(title="CRM File Event Monitor", version="1.0.0")

    @app.get("/health")
    async def _health_error() -> Dict[str, str]:
        raise HTTPException(status_code=500, detail=error_message)

    @app.get("/")
    async def _root_error() -> Dict[str, str]:
        raise HTTPException(status_code=500, detail=error_message)


