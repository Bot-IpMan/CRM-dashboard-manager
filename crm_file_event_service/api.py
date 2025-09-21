"""FastAPI application for exposing file events and advanced analytics."""
from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from .analytics import CallAnalyticsEngine
from .config import ServiceConfig, load_config
from .database import EventDatabase
from .models import CallMetricSnapshot
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


class QueueSummary(BaseModel):
    """Serialised representation of KPI metrics for a queue."""

    model_config = ConfigDict(from_attributes=True)

    queue: str
    captured_at: datetime
    total_calls: int
    active_calls: int
    avg_handle_time: float
    service_level: float
    customer_satisfaction: float
    abandonment_rate: float
    escalation_rate: Optional[float] = None
    nps: Optional[float] = None
    first_call_resolution: Optional[float] = None
    after_call_work: Optional[float] = None
    update_source: Optional[str] = None


class CallMetricsSummary(BaseModel):
    """Aggregated KPI snapshot used by dashboards."""

    model_config = ConfigDict(from_attributes=True)

    last_updated: datetime
    total_calls: int
    active_calls: int
    avg_handle_time: float
    service_level: float
    customer_satisfaction: float
    abandonment_rate: float
    queues: List[QueueSummary]


class CallMetricHistoryEntry(BaseModel):
    """Historical KPI record aggregated across queues."""

    model_config = ConfigDict(from_attributes=True)

    captured_at: datetime
    total_calls: int
    active_calls: int
    avg_handle_time: float
    service_level: float
    customer_satisfaction: float
    abandonment_rate: float


class CallMetricPayload(BaseModel):
    """Incoming payload for ingesting KPI snapshots."""

    model_config = ConfigDict(from_attributes=True)

    queue: str = Field(..., description="Назва черги або напрямку")
    total_calls: int = Field(..., ge=0)
    active_calls: int = Field(..., ge=0)
    avg_handle_time: float = Field(..., ge=0)
    service_level: float = Field(..., ge=0, le=100)
    customer_satisfaction: float = Field(..., ge=0, le=100)
    abandonment_rate: float = Field(..., ge=0, le=100)
    captured_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Час вимірювання у форматі ISO 8601",
    )
    escalation_rate: Optional[float] = Field(default=None, ge=0, le=100)
    nps: Optional[float] = Field(default=None)
    first_call_resolution: Optional[float] = Field(default=None, ge=0, le=100)
    after_call_work: Optional[float] = Field(default=None, ge=0)
    update_source: str = Field(default="zoom")

    def to_snapshot(self) -> CallMetricSnapshot:
        return CallMetricSnapshot(
            queue=self.queue,
            total_calls=self.total_calls,
            active_calls=self.active_calls,
            avg_handle_time=self.avg_handle_time,
            service_level=self.service_level,
            customer_satisfaction=self.customer_satisfaction,
            abandonment_rate=self.abandonment_rate,
            captured_at=self.captured_at,
            escalation_rate=self.escalation_rate,
            nps=self.nps,
            first_call_resolution=self.first_call_resolution,
            after_call_work=self.after_call_work,
            update_source=self.update_source,
        )


class CallQualityRequest(BaseModel):
    """Incoming payload for AI transcript analysis."""

    model_config = ConfigDict(from_attributes=True)

    call_id: str
    transcript: str
    agent: Optional[str] = None
    customer: Optional[str] = None
    captured_at: Optional[datetime] = None
    language: Optional[str] = None
    duration_seconds: Optional[float] = Field(default=None, ge=0)


class CallQualityResponse(BaseModel):
    """Outgoing representation of AI analysis."""

    model_config = ConfigDict(from_attributes=True)

    call_id: str
    captured_at: datetime
    agent: Optional[str]
    customer: Optional[str]
    language: Optional[str]
    sentiment: str
    sentiment_score: float
    dominant_emotion: Optional[str]
    keywords: List[str]
    key_phrases: List[str]
    summary: Optional[str]
    duration_seconds: Optional[float]
    ai_flags: Dict[str, Any]
    transcript_excerpt: Optional[str]

    @classmethod
    def from_insight(cls, insight: Any) -> "CallQualityResponse":
        if isinstance(insight, dict):
            def _get(key: str, default: Any = None) -> Any:
                return insight.get(key, default)
        else:
            def _get(key: str, default: Any = None) -> Any:
                return getattr(insight, key, default)

        transcript = _get("transcript")
        excerpt = None
        if isinstance(transcript, str) and transcript:
            excerpt = transcript[:240]
            if len(transcript) > 240:
                excerpt += "…"
        return cls(
            call_id=_get("call_id", ""),
            captured_at=_get("captured_at"),
            agent=_get("agent"),
            customer=_get("customer"),
            language=_get("language"),
            sentiment=str(_get("sentiment", "neutral")),
            sentiment_score=float(_get("sentiment_score", 0.0)),
            dominant_emotion=_get("dominant_emotion"),
            keywords=list(_get("keywords", []) or []),
            key_phrases=list(_get("key_phrases", []) or _get("keyPhrases", []) or []),
            summary=_get("summary"),
            duration_seconds=_get("duration_seconds"),
            ai_flags=dict(_get("ai_flags", {}) or {}),
            transcript_excerpt=excerpt,
        )


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
        timeout = max(self._config.shutdown_grace_period, 1.0)
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
    analytics_engine = CallAnalyticsEngine(EventDatabase(service_config.database))

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
        analytics_engine.close()

    def get_database() -> Iterator[EventDatabase]:
        database = EventDatabase(service_config.database)
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

    @app.get("/kpi/summary", response_model=CallMetricsSummary)
    async def get_kpi_summary() -> CallMetricsSummary:
        summary = analytics_engine.current_summary_dict()
        return _summary_to_model(summary)

    @app.post("/kpi/ingest", response_model=CallMetricsSummary)
    async def ingest_kpi(payload: CallMetricPayload) -> CallMetricsSummary:
        snapshot = payload.to_snapshot()
        summary = analytics_engine.record_snapshot(snapshot)
        await analytics_engine.broadcast_summary(summary)
        return _summary_to_model(summary.as_dict())

    @app.get("/kpi/history", response_model=List[CallMetricHistoryEntry])
    async def get_kpi_history(
        limit: int = Query(48, ge=1, le=240),
        queue: Optional[str] = Query(None),
    ) -> List[CallMetricHistoryEntry]:
        history = analytics_engine.history(limit=limit, queue=queue)
        return [CallMetricHistoryEntry(**entry) for entry in history]

    @app.post("/quality/analyse", response_model=CallQualityResponse)
    async def analyse_transcript(payload: CallQualityRequest) -> CallQualityResponse:
        insight = analytics_engine.analyse_transcript(
            call_id=payload.call_id,
            transcript=payload.transcript,
            agent=payload.agent,
            customer=payload.customer,
            captured_at=payload.captured_at,
            language=payload.language,
            duration_seconds=payload.duration_seconds,
        )
        response = CallQualityResponse.from_insight(insight)
        await analytics_engine.broadcast_quality(insight)
        return response

    @app.get("/quality/recent", response_model=List[CallQualityResponse])
    async def recent_quality(limit: int = Query(10, ge=1, le=50)) -> List[CallQualityResponse]:
        insights = analytics_engine.recent_quality(limit=limit)
        return [CallQualityResponse.from_insight(insight) for insight in insights]

    @app.websocket("/ws/events")
    async def events_stream(websocket: WebSocket) -> None:
        await websocket.accept()
        last_seen: Optional[datetime] = None
        database = EventDatabase(service_config.database)
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

    @app.websocket("/ws/kpi")
    async def kpi_stream(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = await analytics_engine.subscribe_kpi()
        try:
            summary = analytics_engine.current_summary_dict()
            await websocket.send_json(_serialise_summary(summary))
            while True:
                update = await queue.get()
                await websocket.send_json(_serialise_summary(update))
        except WebSocketDisconnect:
            LOGGER.debug("KPI WebSocket disconnected")
        except Exception as exc:  # pragma: no cover - runtime safeguard
            LOGGER.exception("Error in KPI stream: %s", exc)
            await websocket.close(code=1011, reason="internal server error")
        finally:
            await analytics_engine.unsubscribe_kpi(queue)

    @app.websocket("/ws/quality")
    async def quality_stream(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = await analytics_engine.subscribe_quality()
        try:
            insights = analytics_engine.recent_quality(limit=5)
            await websocket.send_json([
                _serialise_quality(CallQualityResponse.from_insight(insight)) for insight in insights
            ])
            while True:
                update = await queue.get()
                response = CallQualityResponse.from_insight(update)
                await websocket.send_json([_serialise_quality(response)])
        except WebSocketDisconnect:
            LOGGER.debug("Quality WebSocket disconnected")
        except Exception as exc:  # pragma: no cover - runtime safeguard
            LOGGER.exception("Error in quality stream: %s", exc)
            await websocket.close(code=1011, reason="internal server error")
        finally:
            await analytics_engine.unsubscribe_quality(queue)

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


def _summary_to_model(summary: Dict[str, Any]) -> CallMetricsSummary:
    queues = [QueueSummary(**queue) for queue in summary.get("queues", [])]
    return CallMetricsSummary(
        last_updated=summary.get("last_updated", datetime.now(timezone.utc)),
        total_calls=int(summary.get("total_calls", 0)),
        active_calls=int(summary.get("active_calls", 0)),
        avg_handle_time=float(summary.get("avg_handle_time", 0.0)),
        service_level=float(summary.get("service_level", 0.0)),
        customer_satisfaction=float(summary.get("customer_satisfaction", 0.0)),
        abandonment_rate=float(summary.get("abandonment_rate", 0.0)),
        queues=queues,
    )


def _serialise_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    model = _summary_to_model(summary)
    payload = model.model_dump()
    payload["last_updated"] = model.last_updated.isoformat()
    payload["queues"] = [
        {**queue.model_dump(), "captured_at": queue.captured_at.isoformat()} for queue in model.queues
    ]
    return payload


def _serialise_quality(response: CallQualityResponse) -> Dict[str, Any]:
    payload = response.model_dump()
    payload["captured_at"] = response.captured_at.isoformat()
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


