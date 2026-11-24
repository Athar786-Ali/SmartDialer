"""
Telecom provider abstraction.

Every provider — real or mock — implements the ``TelecomProvider`` ABC.
The dialer talks exclusively through this interface and never reaches
into provider internals.

Events from the provider flow back through an ``EventCallback`` that
the dialer registers at startup.  Each event carries a unique
``event_id`` for idempotent processing.
"""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Awaitable, Optional
from uuid import UUID, uuid4


# ------------------------------------------------------------------ #
# Provider event types                                                #
# ------------------------------------------------------------------ #

class ProviderEventType(str, enum.Enum):
    RINGING   = "RINGING"
    ANSWERED  = "ANSWERED"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    BUSY      = "BUSY"
    NO_ANSWER = "NO_ANSWER"
    REJECTED  = "REJECTED"


@dataclass(frozen=True)
class ProviderEvent:
    """An event delivered by a telecom provider (via webhook / callback)."""
    event_id: UUID                           # globally unique — for idempotency
    provider_name: str
    provider_call_id: str                    # provider's opaque call identifier
    call_id: UUID                            # our internal call UUID
    event_type: ProviderEventType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)   # provider-specific extras

    @staticmethod
    def make(
        provider_name: str,
        provider_call_id: str,
        call_id: UUID,
        event_type: ProviderEventType,
        **meta,
    ) -> "ProviderEvent":
        return ProviderEvent(
            event_id=uuid4(),
            provider_name=provider_name,
            provider_call_id=provider_call_id,
            call_id=call_id,
            event_type=event_type,
            metadata=meta,
        )


# Callback signature: the dialer passes a coroutine that processes events.
EventCallback = Callable[[ProviderEvent], Awaitable[None]]


# ------------------------------------------------------------------ #
# Provider health snapshot                                            #
# ------------------------------------------------------------------ #

@dataclass
class ProviderHealth:
    """Point-in-time health of a provider (used by safety controller)."""
    name: str
    is_healthy: bool = True
    active_calls: int = 0
    recent_failures: int = 0
    recent_successes: int = 0
    avg_latency_ms: float = 0.0
    error_rate: float = 0.0
    last_failure_at: Optional[datetime] = None


# ------------------------------------------------------------------ #
# Place-call result                                                   #
# ------------------------------------------------------------------ #

@dataclass(frozen=True)
class PlaceCallResult:
    """Returned immediately when a call is submitted to the provider."""
    success: bool
    provider_call_id: Optional[str] = None
    error: Optional[str] = None


# ------------------------------------------------------------------ #
# Abstract provider                                                   #
# ------------------------------------------------------------------ #

class TelecomProvider(ABC):
    """
    Interface that every telecom provider must implement.

    Guarantees the dialer can:
    1. Place an outbound call.
    2. Cancel a ringing / in-progress call.
    3. Query provider health.
    4. Receive asynchronous events via a registered callback.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique provider identifier (e.g. 'reliable', 'flaky')."""

    @abstractmethod
    async def start(self, callback: EventCallback) -> None:
        """Start the provider and register the event callback."""

    @abstractmethod
    async def stop(self) -> None:
        """Graceful shutdown — cancel pending timers, drain queues."""

    @abstractmethod
    async def place_call(
        self,
        call_id: UUID,
        phone_number: str,
    ) -> PlaceCallResult:
        """
        Initiate an outbound call.

        Returns immediately with a ``PlaceCallResult``.  Subsequent
        state changes arrive via the event callback.
        """

    @abstractmethod
    async def cancel_call(self, provider_call_id: str) -> bool:
        """
        Best-effort cancellation of a ringing / active call.

        Returns True if the cancellation was accepted by the provider.
        A COMPLETED or FAILED event will follow.
        """

    @abstractmethod
    def health(self) -> ProviderHealth:
        """Return current health metrics for this provider."""
