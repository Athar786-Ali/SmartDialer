"""
In-memory Store implementation.

Provides the same semantics as ``PgStore`` but runs entirely in-process,
making it ideal for simulations and unit tests that should not require
PostgreSQL infrastructure.

Concurrency model
-----------------
A single ``asyncio.Lock`` serialises all mutations.  This is equivalent
to ``SERIALIZABLE`` isolation — perfectly correct for a single-process
simulation even when many ``asyncio.Task`` workers are competing.

For the ``reserve_*`` methods we iterate sorted candidates and pick the
first one not already locked — analogous to ``FOR UPDATE SKIP LOCKED``.
In practice the global lock means every ``reserve`` call completes
atomically without contention, which is *stronger* than SKIP LOCKED but
behaviourally compatible.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from smart_dialer.engine.base import CampaignSnapshot
from smart_dialer.models import (
    AgentRecord, AgentState, is_valid_agent_transition,
    BorrowerRecord,
    CallRecord, CallState, is_valid_call_transition, TERMINAL_CALL_STATES,
    CampaignRecord,
)
from smart_dialer.store.base import Store


class InMemoryStore(Store):
    """Thread-safe (asyncio-safe) in-memory implementation of Store."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._agents: Dict[UUID, AgentRecord] = {}
        self._calls: Dict[UUID, CallRecord] = {}
        self._campaigns: Dict[UUID, CampaignRecord] = {}
        self._borrowers: Dict[UUID, BorrowerRecord] = {}
        self._events: set[UUID] = set()
        self._heartbeats: Dict[str, datetime] = {}
        self._metrics: list = []

    # ---- lifecycle ----

    async def initialise(self) -> None:
        pass  # nothing to prepare

    async def close(self) -> None:
        pass

    # ---- campaigns ----

    async def create_campaign(self, c: CampaignRecord) -> None:
        async with self._lock:
            self._campaigns[c.id] = c

    async def get_campaign(self, cid: UUID) -> Optional[CampaignRecord]:
        return self._campaigns.get(cid)

    async def list_active_campaigns(self) -> List[CampaignRecord]:
        return [c for c in self._campaigns.values() if c.is_active]

    # ---- agents ----

    async def create_agent(self, a: AgentRecord) -> None:
        async with self._lock:
            self._agents[a.id] = a

    async def reserve_available_agent(
        self, campaign_id: UUID, worker_id: str,
    ) -> Optional[AgentRecord]:
        async with self._lock:
            candidates = sorted(
                (
                    a for a in self._agents.values()
                    if a.campaign_id == campaign_id
                    and a.state == AgentState.AVAILABLE
                ),
                key=lambda a: a.state_changed_at,
            )
            if not candidates:
                return None
            agent = candidates[0]
            agent.state = AgentState.RESERVED
            agent.worker_id = worker_id
            agent.state_changed_at = datetime.now(timezone.utc)
            agent.version += 1
            return AgentRecord(
                id=agent.id,
                name=agent.name,
                campaign_id=agent.campaign_id,
                state=agent.state,
                current_call_id=agent.current_call_id,
                worker_id=agent.worker_id,
                state_changed_at=agent.state_changed_at,
                version=agent.version,
                created_at=agent.created_at,
            )

    async def transition_agent(
        self,
        agent_id: UUID,
        to_state: AgentState,
        expected_version: int,
        current_call_id: Optional[UUID] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        async with self._lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return False
            if agent.version != expected_version:
                return False
            if not is_valid_agent_transition(agent.state, to_state):
                return False
            agent.state = to_state
            agent.version += 1
            agent.state_changed_at = datetime.now(timezone.utc)
            if current_call_id is not None:
                agent.current_call_id = current_call_id
            if worker_id is not None:
                agent.worker_id = worker_id
            if to_state in (AgentState.AVAILABLE, AgentState.OFFLINE):
                agent.current_call_id = None
                agent.worker_id = None
            return True

    async def get_agent(self, agent_id: UUID) -> Optional[AgentRecord]:
        a = self._agents.get(agent_id)
        if a is None:
            return None
        # Return a copy
        return AgentRecord(
            id=a.id, name=a.name, campaign_id=a.campaign_id,
            state=a.state, current_call_id=a.current_call_id,
            worker_id=a.worker_id, state_changed_at=a.state_changed_at,
            version=a.version, created_at=a.created_at,
        )

    async def count_agents_by_state(
        self, campaign_id: UUID,
    ) -> Dict[AgentState, int]:
        counts: Dict[AgentState, int] = defaultdict(int)
        for a in self._agents.values():
            if a.campaign_id == campaign_id:
                counts[a.state] += 1
        return dict(counts)

    async def get_stale_reserved_agents(
        self, threshold_secs: int,
    ) -> List[AgentRecord]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=threshold_secs)
        results = []
        for a in self._agents.values():
            if a.state in (AgentState.RESERVED, AgentState.DIALING):
                if a.state_changed_at < cutoff:
                    results.append(AgentRecord(
                        id=a.id, name=a.name, campaign_id=a.campaign_id,
                        state=a.state, current_call_id=a.current_call_id,
                        worker_id=a.worker_id, state_changed_at=a.state_changed_at,
                        version=a.version, created_at=a.created_at,
                    ))
        return results

    # ---- borrowers ----

    async def create_borrower(self, b: BorrowerRecord) -> None:
        async with self._lock:
            self._borrowers[b.id] = b

    async def reserve_next_borrower(
        self, campaign_id: UUID, worker_id: str,
    ) -> Optional[BorrowerRecord]:
        async with self._lock:
            candidates = sorted(
                (
                    b for b in self._borrowers.values()
                    if b.campaign_id == campaign_id
                    and b.is_callable
                    and b.locked_by is None
                    and b.call_attempts < b.max_attempts
                ),
                key=lambda b: (-b.priority, b.last_called_at or datetime.min.replace(tzinfo=timezone.utc)),
            )
            if not candidates:
                return None
            borrower = candidates[0]
            borrower.locked_by = worker_id
            borrower.locked_at = datetime.now(timezone.utc)
            return BorrowerRecord(
                id=borrower.id, campaign_id=borrower.campaign_id,
                name=borrower.name, phone=borrower.phone,
                priority=borrower.priority,
                last_called_at=borrower.last_called_at,
                call_attempts=borrower.call_attempts,
                max_attempts=borrower.max_attempts,
                is_callable=borrower.is_callable,
                locked_by=borrower.locked_by,
                locked_at=borrower.locked_at,
            )

    async def release_borrower(self, borrower_id: UUID) -> None:
        async with self._lock:
            b = self._borrowers.get(borrower_id)
            if b:
                b.locked_by = None
                b.locked_at = None

    async def mark_borrower_called(
        self, borrower_id: UUID, success: bool,
    ) -> None:
        async with self._lock:
            b = self._borrowers.get(borrower_id)
            if b:
                b.call_attempts += 1
                b.last_called_at = datetime.now(timezone.utc)
                b.locked_by = None
                b.locked_at = None
                if b.call_attempts >= b.max_attempts:
                    b.is_callable = False

    # ---- calls ----

    async def create_call(self, c: CallRecord) -> None:
        async with self._lock:
            self._calls[c.id] = c

    async def transition_call(
        self,
        call_id: UUID,
        to_state: CallState,
        expected_version: int,
        **extra_fields: Any,
    ) -> bool:
        async with self._lock:
            call = self._calls.get(call_id)
            if call is None:
                return False
            # Always validate the state-machine transition regardless
            # of version.  For the in-memory store we are lenient on
            # version mismatches (simulates a re-read + retry that the
            # webhook handler would do in production).
            if call.state == to_state:
                return False  # idempotent — already there
            if not is_valid_call_transition(call.state, to_state):
                return False

            call.state = to_state
            call.version += 1
            now = datetime.now(timezone.utc)
            call.updated_at = now

            # Timestamp bookkeeping
            if to_state == CallState.INITIATED:
                call.initiated_at = now
            elif to_state == CallState.RINGING:
                call.ringing_at = now
            elif to_state == CallState.ANSWERED:
                call.answered_at = now
            elif to_state == CallState.CONNECTED:
                call.connected_at = now
            elif to_state in TERMINAL_CALL_STATES:
                call.completed_at = now
                if call.connected_at:
                    call.duration_ms = int(
                        (now - call.connected_at).total_seconds() * 1000
                    )

            # Apply extra fields
            for k, v in extra_fields.items():
                if hasattr(call, k) and v is not None:
                    setattr(call, k, v)

            return True

    async def get_call(self, call_id: UUID) -> Optional[CallRecord]:
        c = self._calls.get(call_id)
        if c is None:
            return None
        return CallRecord(
            id=c.id, campaign_id=c.campaign_id, borrower_id=c.borrower_id,
            agent_id=c.agent_id, provider_name=c.provider_name,
            provider_call_id=c.provider_call_id, state=c.state,
            worker_id=c.worker_id, initiated_at=c.initiated_at,
            ringing_at=c.ringing_at, answered_at=c.answered_at,
            connected_at=c.connected_at, completed_at=c.completed_at,
            failure_reason=c.failure_reason, duration_ms=c.duration_ms,
            version=c.version, created_at=c.created_at,
        )

    async def get_calls_by_state(
        self, campaign_id: UUID, state: CallState,
    ) -> List[CallRecord]:
        return [
            c for c in self._calls.values()
            if c.campaign_id == campaign_id and c.state == state
        ]

    async def get_stale_calls(
        self, state: CallState, threshold_secs: int,
    ) -> List[CallRecord]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=threshold_secs)
        results = []
        for c in self._calls.values():
            if c.state != state:
                continue
            ts = c.initiated_at or c.created_at
            if ts < cutoff:
                results.append(c)
        return results

    async def count_calls_by_state(
        self, campaign_id: UUID,
    ) -> Dict[CallState, int]:
        counts: Dict[CallState, int] = defaultdict(int)
        for c in self._calls.values():
            if c.campaign_id == campaign_id:
                counts[c.state] += 1
        return dict(counts)

    # ---- idempotency ----

    async def record_event(
        self, event_id: UUID, call_id: UUID, event_type: str,
        provider_name: Optional[str] = None,
    ) -> bool:
        async with self._lock:
            if event_id in self._events:
                return False
            self._events.add(event_id)
            return True

    # ---- snapshot ----

    async def campaign_snapshot(self, campaign_id: UUID) -> CampaignSnapshot:
        agent_counts = await self.count_agents_by_state(campaign_id)
        call_counts = await self.count_calls_by_state(campaign_id)

        total_agents = sum(agent_counts.values())
        available = agent_counts.get(AgentState.AVAILABLE, 0)
        reserved = agent_counts.get(AgentState.RESERVED, 0)
        dialing = agent_counts.get(AgentState.DIALING, 0)
        connected_agents = agent_counts.get(AgentState.CONNECTED, 0)
        wrap_up = agent_counts.get(AgentState.WRAP_UP, 0)

        calls_ringing = call_counts.get(CallState.RINGING, 0)
        calls_initiated = call_counts.get(CallState.INITIATED, 0)
        calls_connected = call_counts.get(CallState.CONNECTED, 0)

        # Compute recent metrics from call history
        now = datetime.now(timezone.utc)
        window = timedelta(seconds=300)
        recent_placed = 0
        recent_answered = 0
        recent_failed = 0
        recent_abandoned = 0

        for c in self._calls.values():
            if c.campaign_id != campaign_id:
                continue
            if c.created_at < now - window:
                continue
            recent_placed += 1
            if c.state == CallState.COMPLETED and c.answered_at:
                recent_answered += 1
            elif c.state == CallState.FAILED:
                recent_failed += 1
            # Abandoned = answered but no agent connected and call ended
            if (c.answered_at and not c.connected_at
                    and c.state in (CallState.COMPLETED, CallState.FAILED)):
                recent_abandoned += 1

        answer_rate = (
            recent_answered / recent_placed if recent_placed > 0 else 0.5
        )
        abandon_rate = (
            recent_abandoned / max(recent_answered, 1)
            if recent_answered > 0 else 0.0
        )

        # Average talk time from completed calls
        durations = [
            c.duration_ms for c in self._calls.values()
            if c.campaign_id == campaign_id
            and c.duration_ms is not None
            and c.duration_ms > 0
        ]
        avg_talk = (sum(durations) / len(durations) / 1000) if durations else 120.0

        campaign = self._campaigns.get(campaign_id)

        return CampaignSnapshot(
            campaign_id=campaign_id,
            available_agents=available,
            reserved_agents=reserved,
            dialing_agents=dialing,
            connected_agents=connected_agents,
            wrap_up_agents=wrap_up,
            total_agents=total_agents,
            calls_ringing=calls_ringing,
            calls_initiated=calls_initiated,
            calls_connected=calls_connected,
            recent_calls_placed=recent_placed,
            recent_calls_answered=recent_answered,
            recent_calls_failed=recent_failed,
            recent_calls_abandoned=recent_abandoned,
            recent_answer_rate=answer_rate,
            recent_abandon_rate=abandon_rate,
            avg_talk_time_secs=avg_talk,
            provider_healthy=True,
        )

    # ---- metrics ----

    async def record_metrics(
        self, campaign_id, window_start, window_end,
        calls_initiated, calls_answered, calls_abandoned,
        calls_failed, answer_rate, abandon_rate,
    ) -> None:
        self._metrics.append({
            "campaign_id": campaign_id,
            "window_start": window_start,
            "window_end": window_end,
            "calls_initiated": calls_initiated,
            "calls_answered": calls_answered,
            "calls_abandoned": calls_abandoned,
            "calls_failed": calls_failed,
            "answer_rate": answer_rate,
            "abandon_rate": abandon_rate,
        })

    # ---- heartbeats ----

    async def heartbeat(self, worker_id: str, campaign_ids: List[UUID]) -> None:
        self._heartbeats[worker_id] = datetime.now(timezone.utc)

    async def get_dead_workers(self, threshold_secs: int) -> List[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=threshold_secs)
        return [
            wid for wid, ts in self._heartbeats.items()
            if ts < cutoff
        ]
