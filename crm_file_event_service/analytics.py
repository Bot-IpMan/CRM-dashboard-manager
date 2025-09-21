"""Analytics helpers for real-time KPI monitoring and call quality insights."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .database import EventDatabase
from .models import CallMetricSnapshot, CallQualityInsight

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class QueueKPISummary:
    """Aggregated KPI information for a single contact centre queue."""

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

    def as_dict(self) -> Dict[str, Any]:
        return {
            "queue": self.queue,
            "captured_at": self.captured_at,
            "total_calls": self.total_calls,
            "active_calls": self.active_calls,
            "avg_handle_time": self.avg_handle_time,
            "service_level": self.service_level,
            "customer_satisfaction": self.customer_satisfaction,
            "abandonment_rate": self.abandonment_rate,
            "escalation_rate": self.escalation_rate,
            "nps": self.nps,
            "first_call_resolution": self.first_call_resolution,
            "after_call_work": self.after_call_work,
            "update_source": self.update_source,
        }


@dataclass(slots=True)
class RealtimeKPISummary:
    """Aggregated KPI information across all queues."""

    last_updated: datetime
    total_calls: int
    active_calls: int
    avg_handle_time: float
    service_level: float
    customer_satisfaction: float
    abandonment_rate: float
    queues: List[QueueKPISummary] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "last_updated": self.last_updated,
            "total_calls": self.total_calls,
            "active_calls": self.active_calls,
            "avg_handle_time": self.avg_handle_time,
            "service_level": self.service_level,
            "customer_satisfaction": self.customer_satisfaction,
            "abandonment_rate": self.abandonment_rate,
            "queues": [queue.as_dict() for queue in self.queues],
        }


class CallAnalyticsEngine:
    """Central hub for KPI monitoring and AI transcript analysis."""

    def __init__(self, database: EventDatabase) -> None:
        self._database = database
        self._summary_cache: Optional[RealtimeKPISummary] = None
        self._kpi_subscribers: List[asyncio.Queue] = []
        self._quality_subscribers: List[asyncio.Queue] = []
        self._subscriber_lock = asyncio.Lock()
        self._transcript_analyzer = TranscriptAnalyzer()

    def close(self) -> None:
        """Release the underlying database connection."""

        self._database.close()

    def record_snapshot(self, snapshot: CallMetricSnapshot) -> RealtimeKPISummary:
        """Persist a KPI snapshot and refresh the cached summary."""

        LOGGER.debug("Recording KPI snapshot for queue %s", snapshot.queue)
        self._database.insert_call_metric(snapshot)
        self._summary_cache = None
        return self.current_summary()

    def current_summary(self) -> RealtimeKPISummary:
        """Return the most up-to-date KPI summary."""

        if self._summary_cache is None:
            self._summary_cache = self._build_summary()
        return self._summary_cache

    def current_summary_dict(self) -> Dict[str, Any]:
        """Return the current KPI summary serialised as a dictionary."""

        return self.current_summary().as_dict()

    def history(self, *, limit: int = 48, queue: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return aggregated historical KPI measurements."""

        rows = self._database.fetch_metrics_history(limit=limit, queue=queue)
        aggregated: Dict[datetime, Dict[str, Any]] = defaultdict(
            lambda: {
                "captured_at": None,
                "total_calls": 0,
                "active_calls": 0,
                "handle_time": 0.0,
                "service_level": 0.0,
                "customer_satisfaction": 0.0,
                "abandonment_rate": 0.0,
                "weight": 0.0,
            }
        )

        for row in rows:
            captured_at = _parse_datetime(row.get("captured_at"))
            bucket = aggregated[captured_at]
            bucket["captured_at"] = captured_at
            total_calls = max(_parse_int(row.get("total_calls")), 0)
            active_calls = max(_parse_int(row.get("active_calls")), 0)
            weight = max(total_calls, 1)
            bucket["total_calls"] += total_calls
            bucket["active_calls"] += active_calls
            bucket["handle_time"] += _parse_float(row.get("avg_handle_time")) * weight
            bucket["service_level"] += _parse_float(row.get("service_level")) * weight
            bucket["customer_satisfaction"] += _parse_float(
                row.get("customer_satisfaction")
            ) * weight
            bucket["abandonment_rate"] += _parse_float(row.get("abandonment_rate")) * weight
            bucket["weight"] += weight

        history: List[Dict[str, Any]] = []
        for captured_at in sorted(aggregated.keys()):
            bucket = aggregated[captured_at]
            weight = bucket["weight"] or 1.0
            history.append(
                {
                    "captured_at": bucket["captured_at"],
                    "total_calls": bucket["total_calls"],
                    "active_calls": bucket["active_calls"],
                    "avg_handle_time": bucket["handle_time"] / weight,
                    "service_level": bucket["service_level"] / weight,
                    "customer_satisfaction": bucket["customer_satisfaction"] / weight,
                    "abandonment_rate": bucket["abandonment_rate"] / weight,
                }
            )

        return history

    def analyse_transcript(
        self,
        *,
        call_id: str,
        transcript: str,
        agent: Optional[str] = None,
        customer: Optional[str] = None,
        captured_at: Optional[datetime] = None,
        language: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ) -> CallQualityInsight:
        """Run AI-powered analysis for a call transcript."""

        insight = self._transcript_analyzer.analyse(
            call_id=call_id,
            transcript=transcript,
            agent=agent,
            customer=customer,
            captured_at=captured_at,
            language=language,
            duration_seconds=duration_seconds,
        )
        LOGGER.debug("Persisting call quality insight for %s", call_id)
        self._database.insert_call_quality_insight(insight)
        return insight

    def recent_quality(self, *, limit: int = 10) -> List[CallQualityInsight]:
        """Return the most recent transcript analyses."""

        rows = self._database.fetch_recent_call_quality(limit=limit)
        return [self._row_to_quality(row) for row in rows]

    def quality_for_call(self, call_id: str) -> Optional[CallQualityInsight]:
        """Return the latest insight for a specific call."""

        row = self._database.fetch_call_quality_for_call(call_id)
        if not row:
            return None
        return self._row_to_quality(row)

    async def subscribe_kpi(self) -> asyncio.Queue:
        """Register a listener for KPI updates."""

        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        async with self._subscriber_lock:
            self._kpi_subscribers.append(queue)
        return queue

    async def unsubscribe_kpi(self, queue: asyncio.Queue) -> None:
        async with self._subscriber_lock:
            if queue in self._kpi_subscribers:
                self._kpi_subscribers.remove(queue)

    async def subscribe_quality(self) -> asyncio.Queue:
        """Register a listener for AI transcript insights."""

        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        async with self._subscriber_lock:
            self._quality_subscribers.append(queue)
        return queue

    async def unsubscribe_quality(self, queue: asyncio.Queue) -> None:
        async with self._subscriber_lock:
            if queue in self._quality_subscribers:
                self._quality_subscribers.remove(queue)

    async def broadcast_summary(self, summary: RealtimeKPISummary) -> None:
        await self._broadcast(self._kpi_subscribers, summary.as_dict())

    async def broadcast_quality(self, insight: CallQualityInsight) -> None:
        await self._broadcast(self._quality_subscribers, self._format_quality_payload(insight))

    def _build_summary(self) -> RealtimeKPISummary:
        rows = self._database.fetch_latest_metrics_by_queue()
        queues = [self._row_to_queue(row) for row in rows]
        if not queues:
            now = datetime.now(timezone.utc)
            return RealtimeKPISummary(
                last_updated=now,
                total_calls=0,
                active_calls=0,
                avg_handle_time=0.0,
                service_level=0.0,
                customer_satisfaction=0.0,
                abandonment_rate=0.0,
                queues=[],
            )

        total_calls = sum(queue.total_calls for queue in queues)
        active_calls = sum(queue.active_calls for queue in queues)
        weights = [max(queue.total_calls, 1) for queue in queues]
        weight_sum = float(sum(weights)) or 1.0
        avg_handle_time = sum(queue.avg_handle_time * weight for queue, weight in zip(queues, weights)) / weight_sum
        service_level = sum(queue.service_level * weight for queue, weight in zip(queues, weights)) / weight_sum
        customer_satisfaction = (
            sum(queue.customer_satisfaction * weight for queue, weight in zip(queues, weights)) / weight_sum
        )
        abandonment_rate = sum(queue.abandonment_rate * weight for queue, weight in zip(queues, weights)) / weight_sum
        last_updated = max(queue.captured_at for queue in queues)

        return RealtimeKPISummary(
            last_updated=last_updated,
            total_calls=total_calls,
            active_calls=active_calls,
            avg_handle_time=avg_handle_time,
            service_level=service_level,
            customer_satisfaction=customer_satisfaction,
            abandonment_rate=abandonment_rate,
            queues=queues,
        )

    def _row_to_queue(self, row: Dict[str, Any]) -> QueueKPISummary:
        return QueueKPISummary(
            queue=str(row.get("queue") or "unknown"),
            captured_at=_parse_datetime(row.get("captured_at")),
            total_calls=max(_parse_int(row.get("total_calls")), 0),
            active_calls=max(_parse_int(row.get("active_calls")), 0),
            avg_handle_time=_parse_float(row.get("avg_handle_time")),
            service_level=_parse_float(row.get("service_level")),
            customer_satisfaction=_parse_float(row.get("customer_satisfaction")),
            abandonment_rate=_parse_float(row.get("abandonment_rate")),
            escalation_rate=_parse_optional_float(row.get("escalation_rate")),
            nps=_parse_optional_float(row.get("nps")),
            first_call_resolution=_parse_optional_float(row.get("first_call_resolution")),
            after_call_work=_parse_optional_float(row.get("after_call_work")),
            update_source=row.get("update_source"),
        )

    def _row_to_quality(self, row: Dict[str, Any]) -> CallQualityInsight:
        keywords = _decode_json_list(row.get("keywords"))
        key_phrases = _decode_json_list(row.get("key_phrases"))
        ai_flags = _decode_json_dict(row.get("ai_flags"))
        duration = _parse_optional_float(row.get("duration_seconds"))
        return CallQualityInsight(
            call_id=str(row.get("call_id")),
            sentiment=str(row.get("sentiment") or "neutral"),
            sentiment_score=_parse_float(row.get("sentiment_score")),
            dominant_emotion=row.get("dominant_emotion"),
            keywords=keywords,
            key_phrases=key_phrases,
            summary=row.get("summary"),
            transcript=row.get("transcript"),
            captured_at=_parse_datetime(row.get("captured_at")),
            agent=row.get("agent"),
            customer=row.get("customer"),
            language=row.get("language"),
            duration_seconds=duration,
            ai_flags=ai_flags,
        )

    async def _broadcast(self, subscribers: Iterable[asyncio.Queue], payload: Dict[str, Any]) -> None:
        stale: List[asyncio.Queue] = []
        async with self._subscriber_lock:
            for queue in list(subscribers):
                try:
                    if queue.full():
                        try:
                            queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    LOGGER.debug("Queue full, skipping KPI broadcast")
                except RuntimeError:
                    stale.append(queue)
            for queue in stale:
                if queue in self._kpi_subscribers:
                    self._kpi_subscribers.remove(queue)
                if queue in self._quality_subscribers:
                    self._quality_subscribers.remove(queue)

    def _format_quality_payload(self, insight: CallQualityInsight) -> Dict[str, Any]:
        payload = insight.as_db_row()
        payload["captured_at"] = insight.captured_at
        payload["keywords"] = insight.keywords
        payload["key_phrases"] = insight.key_phrases
        payload["ai_flags"] = insight.ai_flags
        return payload


class TranscriptAnalyzer:
    """Rule-based analyser that extracts sentiment, emotions and keywords."""

    WORD_RE = re.compile(r"[\w']+", re.UNICODE)

    POSITIVE_WORDS = {
        "дякую",
        "класно",
        "добре",
        "задоволений",
        "задоволена",
        "щасливий",
        "щаслива",
        "great",
        "awesome",
        "perfect",
        "love",
        "grateful",
        "cool",
    }
    NEGATIVE_WORDS = {
        "скарга",
        "незадоволений",
        "незадоволена",
        "затримка",
        "повільно",
        "помилка",
        "проблема",
        "дорого",
        "злий",
        "angry",
        "issue",
        "bad",
        "cancel",
        "complaint",
    }
    EMOTION_KEYWORDS = {
        "joy": {"дякую", "щасливий", "great", "love", "cool", "задоволений"},
        "frustration": {"помилка", "затримка", "проблема", "angry", "скарга", "дорого"},
        "anxiety": {"хвилюю", "боюсь", "worry", "невпевнений", "ризик", "просрочка"},
        "calm": {"зрозуміло", "ок", "домовились", "готово", "resolved", "спокійно"},
    }
    STOPWORDS = {
        "і",
        "в",
        "та",
        "на",
        "що",
        "це",
        "як",
        "ми",
        "ви",
        "вони",
        "з",
        "to",
        "the",
        "and",
        "for",
        "with",
    }
    QUESTION_WORDS = {
        "коли",
        "як",
        "де",
        "чому",
        "скільки",
        "чи",
        "можу",
        "можемо",
        "when",
        "how",
        "why",
        "where",
        "price",
        "cost",
    }

    def analyse(
        self,
        *,
        call_id: str,
        transcript: str,
        agent: Optional[str] = None,
        customer: Optional[str] = None,
        captured_at: Optional[datetime] = None,
        language: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ) -> CallQualityInsight:
        captured = captured_at or datetime.now(timezone.utc)
        text = transcript or ""
        tokens = self._tokenise(text)
        counts = Counter(tokens)
        total_terms = sum(counts.values()) or 1
        positive_hits = sum(counts[word] for word in self.POSITIVE_WORDS if word in counts)
        negative_hits = sum(counts[word] for word in self.NEGATIVE_WORDS if word in counts)
        sentiment_score = (positive_hits - negative_hits) / math.sqrt(total_terms)
        if sentiment_score > 0.15:
            sentiment = "positive"
        elif sentiment_score < -0.15:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        emotion_scores = {
            emotion: sum(counts[word] for word in keywords if word in counts)
            for emotion, keywords in self.EMOTION_KEYWORDS.items()
        }
        dominant_emotion = None
        if any(emotion_scores.values()):
            dominant_emotion = max(emotion_scores, key=emotion_scores.get)

        keywords = self._extract_keywords(counts)
        key_questions = self._extract_questions(text)
        summary = self._build_summary(sentiment, dominant_emotion, keywords, key_questions)
        ai_flags: Dict[str, Any] = {
            "positive_mentions": positive_hits,
            "negative_mentions": negative_hits,
        }
        if negative_hits > positive_hits * 1.3 and negative_hits >= 2:
            ai_flags["escalation_alert"] = 1

        truncated_transcript = text[:5000] if text else None

        return CallQualityInsight(
            call_id=call_id,
            sentiment=sentiment,
            sentiment_score=round(sentiment_score, 3),
            dominant_emotion=dominant_emotion,
            keywords=keywords,
            key_phrases=key_questions,
            summary=summary,
            transcript=truncated_transcript,
            captured_at=captured,
            agent=agent,
            customer=customer,
            language=language,
            duration_seconds=duration_seconds,
            ai_flags=ai_flags,
        )

    def _tokenise(self, text: str) -> List[str]:
        return [match.group(0).lower() for match in self.WORD_RE.finditer(text)]

    def _extract_keywords(self, counts: Counter) -> List[str]:
        items = [
            (word, freq)
            for word, freq in counts.items()
            if len(word) >= 3 and word not in self.STOPWORDS and not word.isdigit()
        ]
        items.sort(key=lambda item: (-item[1], item[0]))
        return [word for word, _ in items[:6]]

    def _extract_questions(self, transcript: str) -> List[str]:
        sentences = re.split(r"(?<=[.?!])\s+", transcript.strip())
        questions: List[str] = []
        for sentence in sentences:
            cleaned = sentence.strip()
            if not cleaned:
                continue
            lower = cleaned.lower()
            if cleaned.endswith("?") or any(keyword in lower for keyword in self.QUESTION_WORDS):
                questions.append(cleaned)
            if len(questions) >= 4:
                break
        return questions

    def _build_summary(
        self,
        sentiment: str,
        emotion: Optional[str],
        keywords: List[str],
        questions: List[str],
    ) -> str:
        parts = [f"Тон розмови: {sentiment}{f' ({emotion})' if emotion else ''}."]
        if keywords:
            parts.append("Ключові теми: " + ", ".join(keywords[:4]) + ".")
        if questions:
            parts.append("Ключові питання: " + "; ".join(questions[:2]) + ".")
        return " ".join(parts)


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value).astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _parse_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decode_json_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str) and value:
        try:
            loaded = json.loads(value)
            if isinstance(loaded, list):
                return [str(item) for item in loaded]
        except json.JSONDecodeError:
            pass
    return []


def _decode_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            loaded = json.loads(value)
            if isinstance(loaded, dict):
                return loaded
        except json.JSONDecodeError:
            pass
    return {}

