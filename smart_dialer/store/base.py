"""
Store abstraction — the data-access layer for the SmartDialer.

Two implementations:
  - ``PgStore``       — production PostgreSQL with asyncpg
  - ``InMemoryStore`` — zero-dependency, single-process (for simulation/tests)

Both provide identical semantics, including:
  - Atomic agent reservation (SKIP LOCKED / try-lock)
  - Optimistic concurrency (version checks)
  - Idempotent event recording
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from smart_dialer.models import (
    AgentRecord, AgentState,
    CallRecord, CallState,
    CampaignRecord, BorrowerRecord,
)
from smart_dialer.engine.base import CampaignSnapshot


class Store(ABC):
    """Abstract data store."""

    # ---- lifecycle ----

    @abstractmethod
    async def initialise(self) -> None:
        """Create tables / prepare pool."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources."""

    # ---- campaigns ----

    @abstractmethod
    async def create_campaign(self, campaign: CampaignRecord) -> None: ...

    @abstractmethod
    async def get_campaign(self, campaign_id: UUID) -> Optional[CampaignRecord]: ...

    @abstractmethod
    async def list_active_campaigns(self) -> List[CampaignRecord]: ...

    # ---- agents ----

    @abstractmethod
    async def create_agent(self, agent: AgentRecord) -> None: ...

    @abstractmethod
    async def reserve_available_agent(
        self, campaign_id: UUID, worker_id: str,
    ) -> Optional[AgentRecord]:
        """
        Atomically find one AVAILABLE agent for *campaign_id*, transition
        it to RESERVED, and return the updated record.

        Returns ``None`` if no agent is available (or all are locked by
        other workers).

        **PostgreSQL**: ``SELECT … FOR UPDATE SKIP LOCKED``
        **In-memory**: non-blocking per-row try-lock
        """

    @abstractmethod
    async def transition_agent(
        self,
        agent_id: UUID,
        to_state: AgentState,
        expected_version: int,
        current_call_id: Optional[UUID] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        """
        CAS-style agent state transition.

        Returns ``True`` if the update applied (version matched and
        transition was valid).  Returns ``False`` on version conflict
        or invalid transition — the caller must re-read and decide.
        """

    @abstractmethod
    async def get_agent(self, agent_id: UUID) -> Optional[AgentRecord]: ...

    @abstractmethod
    async def count_agents_by_state(
        self, campaign_id: UUID,
    ) -> Dict[AgentState, int]: ...

    @abstractmethod
    async def get_stale_reserved_agents(
        self, threshold_secs: int,
    ) -> List[AgentRecord]:
        """Agents stuck in RESERVED or DIALING longer than threshold."""

    # ---- borrowers ----

    @abstractmethod
    async def create_borrower(self, borrower: BorrowerRecord) -> None: ...

    @abstractmethod
    async def reserve_next_borrower(
        self, campaign_id: UUID, worker_id: str,
    ) -> Optional[BorrowerRecord]:
        """
        Atomically claim the highest-priority callable borrower.
        Uses SKIP LOCKED to avoid contention.
        """

    @abstractmethod
    async def release_borrower(self, borrower_id: UUID) -> None:
        """Release a borrower reservation (e.g. after call failure)."""

    @abstractmethod
    async def mark_borrower_called(
        self, borrower_id: UUID, success: bool,
    ) -> None:
        """Update call_attempts, last_called_at, is_callable."""

    # ---- calls ----

    @abstractmethod
    async def create_call(self, call: CallRecord) -> None: ...

    @abstractmethod
    async def transition_call(
        self,
        call_id: UUID,
        to_state: CallState,
        expected_version: int,
        **extra_fields,
    ) -> bool:
        """CAS-style call state transition."""

    @abstractmethod
    async def get_call(self, call_id: UUID) -> Optional[CallRecord]: ...

    @abstractmethod
    async def get_calls_by_state(
        self, campaign_id: UUID, state: CallState,
    ) -> List[CallRecord]: ...

    @abstractmethod
    async def get_stale_calls(
        self, state: CallState, threshold_secs: int,
    ) -> List[CallRecord]:
        """Calls stuck in *state* longer than threshold."""

    @abstractmethod
    async def count_calls_by_state(
        self, campaign_id: UUID,
    ) -> Dict[CallState, int]: ...

    # ---- idempotency ----

    @abstractmethod
    async def record_event(
        self, event_id: UUID, call_id: UUID, event_type: str,
        provider_name: Optional[str] = None,
    ) -> bool:
        """
        Record a processed event.  Returns ``True`` if this is the first
        time (event was inserted).  Returns ``False`` on duplicate
        (ON CONFLICT DO NOTHING — the event was already processed).
        """

    # ---- snapshots ----

    @abstractmethod
    async def campaign_snapshot(self, campaign_id: UUID) -> CampaignSnapshot:
        """Build a point-in-time snapshot for the pacing engine."""

    # ---- metrics ----

    @abstractmethod
    async def record_metrics(
        self,
        campaign_id: UUID,
        window_start: datetime,
        window_end: datetime,
        calls_initiated: int,
        calls_answered: int,
        calls_abandoned: int,
        calls_failed: int,
        answer_rate: float,
        abandon_rate: float,
    ) -> None: ...

    # ---- worker heartbeats ----

    @abstractmethod
    async def heartbeat(self, worker_id: str, campaign_ids: List[UUID]) -> None:
        """Upsert worker heartbeat."""

    @abstractmethod
    async def get_dead_workers(self, threshold_secs: int) -> List[str]:
        """Workers whose last heartbeat is older than threshold."""
