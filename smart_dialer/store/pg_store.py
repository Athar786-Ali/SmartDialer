"""
PostgreSQL Store implementation using asyncpg.

Key concurrency patterns:
  - ``FOR UPDATE SKIP LOCKED`` for contention-free agent/borrower reservation
  - Version-based optimistic concurrency for state transitions
  - ``INSERT … ON CONFLICT DO NOTHING`` for idempotent event processing
"""

from __future__ import annotations

import asyncpg
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

from smart_dialer.config import DatabaseConfig
from smart_dialer.engine.base import CampaignSnapshot
from smart_dialer.models import (
    AgentRecord, AgentState, is_valid_agent_transition,
    BorrowerRecord,
    CallRecord, CallState, is_valid_call_transition, TERMINAL_CALL_STATES,
    CampaignRecord,
)
from smart_dialer.store.base import Store


def _agent_from_row(row: asyncpg.Record) -> AgentRecord:
    return AgentRecord(
        id=row["id"],
        name=row["name"],
        campaign_id=row["campaign_id"],
        state=AgentState(row["state"]),
        current_call_id=row.get("current_call_id"),
        worker_id=row.get("worker_id"),
        state_changed_at=row["state_changed_at"],
        version=row["version"],
        created_at=row["created_at"],
    )


def _call_from_row(row: asyncpg.Record) -> CallRecord:
    return CallRecord(
        id=row["id"],
        campaign_id=row["campaign_id"],
        borrower_id=row["borrower_id"],
        agent_id=row.get("agent_id"),
        provider_name=row.get("provider_name"),
        provider_call_id=row.get("provider_call_id"),
        state=CallState(row["state"]),
        worker_id=row.get("worker_id"),
        initiated_at=row.get("initiated_at"),
        ringing_at=row.get("ringing_at"),
        answered_at=row.get("answered_at"),
        connected_at=row.get("connected_at"),
        completed_at=row.get("completed_at"),
        failure_reason=row.get("failure_reason"),
        duration_ms=row.get("duration_ms"),
        version=row["version"],
        created_at=row["created_at"],
    )


def _borrower_from_row(row: asyncpg.Record) -> BorrowerRecord:
    return BorrowerRecord(
        id=row["id"],
        campaign_id=row["campaign_id"],
        name=row["name"],
        phone=row["phone"],
        priority=row["priority"],
        last_called_at=row.get("last_called_at"),
        call_attempts=row["call_attempts"],
        max_attempts=row["max_attempts"],
        is_callable=row["is_callable"],
        locked_by=row.get("locked_by"),
        locked_at=row.get("locked_at"),
    )


class PgStore(Store):
    """Production PostgreSQL store using asyncpg connection pool."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._pool: Optional[asyncpg.Pool] = None

    # ---- lifecycle ----

    async def initialise(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self._config.dsn,
            min_size=self._config.min_pool_size,
            max_size=self._config.max_pool_size,
            statement_cache_size=self._config.statement_cache_size,
        )
        # Apply schema
        schema_path = Path(__file__).parent / "schema.sql"
        schema_sql = schema_path.read_text()
        async with self._pool.acquire() as conn:
            await conn.execute(schema_sql)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    @property
    def pool(self) -> asyncpg.Pool:
        assert self._pool is not None, "Store not initialised"
        return self._pool

    # ---- campaigns ----

    async def create_campaign(self, c: CampaignRecord) -> None:
        await self.pool.execute(
            """INSERT INTO campaigns (id, name, mode, provider_name,
               max_abandon_rate, target_answer_rate, avg_talk_time_secs, is_active)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT (id) DO NOTHING""",
            c.id, c.name, c.mode, c.provider_name,
            c.max_abandon_rate, c.target_answer_rate, c.avg_talk_time_secs,
            c.is_active,
        )

    async def get_campaign(self, cid: UUID) -> Optional[CampaignRecord]:
        row = await self.pool.fetchrow(
            "SELECT * FROM campaigns WHERE id = $1", cid,
        )
        if row is None:
            return None
        return CampaignRecord(
            id=row["id"], name=row["name"], mode=row["mode"],
            provider_name=row["provider_name"],
            max_abandon_rate=float(row["max_abandon_rate"]),
            target_answer_rate=float(row["target_answer_rate"]),
            avg_talk_time_secs=row["avg_talk_time_secs"],
            is_active=row["is_active"],
        )

    async def list_active_campaigns(self) -> List[CampaignRecord]:
        rows = await self.pool.fetch(
            "SELECT * FROM campaigns WHERE is_active = TRUE",
        )
        return [
            CampaignRecord(
                id=r["id"], name=r["name"], mode=r["mode"],
                provider_name=r["provider_name"],
                max_abandon_rate=float(r["max_abandon_rate"]),
                target_answer_rate=float(r["target_answer_rate"]),
                avg_talk_time_secs=r["avg_talk_time_secs"],
                is_active=r["is_active"],
            )
            for r in rows
        ]

    # ---- agents ----

    async def create_agent(self, a: AgentRecord) -> None:
        await self.pool.execute(
            """INSERT INTO agents (id, name, campaign_id, state,
               current_call_id, worker_id, state_changed_at, version)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT (id) DO NOTHING""",
            a.id, a.name, a.campaign_id, a.state.value,
            a.current_call_id, a.worker_id, a.state_changed_at, a.version,
        )

    async def reserve_available_agent(
        self, campaign_id: UUID, worker_id: str,
    ) -> Optional[AgentRecord]:
        """
        Atomically reserve one AVAILABLE agent using FOR UPDATE SKIP LOCKED.

        This is the key concurrency primitive: when Worker A and Worker B
        both execute this query concurrently, the first locks a row; the
        second's SKIP LOCKED skips that row and finds the next one.
        Neither blocks.  Neither double-books.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id, name, campaign_id, state, current_call_id,
                           worker_id, state_changed_at, version, created_at
                    FROM agents
                    WHERE state = 'AVAILABLE'
                      AND campaign_id = $1
                    ORDER BY state_changed_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    campaign_id,
                )
                if row is None:
                    return None

                new_version = row["version"] + 1
                now = datetime.now(timezone.utc)
                await conn.execute(
                    """
                    UPDATE agents
                    SET state = 'RESERVED',
                        worker_id = $1,
                        state_changed_at = $2,
                        version = $3,
                        updated_at = $2
                    WHERE id = $4
                    """,
                    worker_id, now, new_version, row["id"],
                )
                return AgentRecord(
                    id=row["id"],
                    name=row["name"],
                    campaign_id=row["campaign_id"],
                    state=AgentState.RESERVED,
                    current_call_id=row["current_call_id"],
                    worker_id=worker_id,
                    state_changed_at=now,
                    version=new_version,
                    created_at=row["created_at"],
                )

    async def transition_agent(
        self,
        agent_id: UUID,
        to_state: AgentState,
        expected_version: int,
        current_call_id: Optional[UUID] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        # Build SET clause dynamically
        sets = [
            "state = $1",
            "version = version + 1",
            "state_changed_at = now()",
            "updated_at = now()",
        ]
        params: list = [to_state.value]
        idx = 2

        if current_call_id is not None:
            sets.append(f"current_call_id = ${idx}")
            params.append(current_call_id)
            idx += 1
        elif to_state in (AgentState.AVAILABLE, AgentState.OFFLINE):
            sets.append("current_call_id = NULL")
            sets.append("worker_id = NULL")

        if worker_id is not None:
            sets.append(f"worker_id = ${idx}")
            params.append(worker_id)
            idx += 1

        # WHERE clause: check version AND that the transition is valid
        # We load valid source states from Python to build the SQL
        valid_sources = [
            s.value for s in AgentState
            if is_valid_agent_transition(s, to_state)
        ]
        if not valid_sources:
            return False

        source_placeholders = ", ".join(
            f"${idx + i}" for i in range(len(valid_sources))
        )
        params.extend(valid_sources)
        idx += len(valid_sources)

        params.append(agent_id)
        params.append(expected_version)

        sql = f"""
            UPDATE agents
            SET {', '.join(sets)}
            WHERE id = ${idx}
              AND version = ${idx + 1}
              AND state IN ({source_placeholders})
        """

        result = await self.pool.execute(sql, *params)
        return result.endswith("1")  # "UPDATE 1" means success

    async def get_agent(self, agent_id: UUID) -> Optional[AgentRecord]:
        row = await self.pool.fetchrow(
            "SELECT * FROM agents WHERE id = $1", agent_id,
        )
        return _agent_from_row(row) if row else None

    async def count_agents_by_state(
        self, campaign_id: UUID,
    ) -> Dict[AgentState, int]:
        rows = await self.pool.fetch(
            """SELECT state, COUNT(*) AS cnt
               FROM agents WHERE campaign_id = $1
               GROUP BY state""",
            campaign_id,
        )
        return {AgentState(r["state"]): r["cnt"] for r in rows}

    async def get_stale_reserved_agents(
        self, threshold_secs: int,
    ) -> List[AgentRecord]:
        rows = await self.pool.fetch(
            """SELECT * FROM agents
               WHERE state IN ('RESERVED', 'DIALING')
                 AND state_changed_at < now() - $1 * interval '1 second'""",
            threshold_secs,
        )
        return [_agent_from_row(r) for r in rows]

    # ---- borrowers ----

    async def create_borrower(self, b: BorrowerRecord) -> None:
        await self.pool.execute(
            """INSERT INTO borrowers (id, campaign_id, name, phone, priority,
               call_attempts, max_attempts, is_callable)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT (id) DO NOTHING""",
            b.id, b.campaign_id, b.name, b.phone, b.priority,
            b.call_attempts, b.max_attempts, b.is_callable,
        )

    async def reserve_next_borrower(
        self, campaign_id: UUID, worker_id: str,
    ) -> Optional[BorrowerRecord]:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT * FROM borrowers
                    WHERE campaign_id = $1
                      AND is_callable = TRUE
                      AND locked_by IS NULL
                      AND call_attempts < max_attempts
                    ORDER BY priority DESC, last_called_at NULLS FIRST
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    campaign_id,
                )
                if row is None:
                    return None
                await conn.execute(
                    """UPDATE borrowers
                       SET locked_by = $1, locked_at = now()
                       WHERE id = $2""",
                    worker_id, row["id"],
                )
                result = _borrower_from_row(row)
                result.locked_by = worker_id
                return result

    async def release_borrower(self, borrower_id: UUID) -> None:
        await self.pool.execute(
            "UPDATE borrowers SET locked_by = NULL, locked_at = NULL WHERE id = $1",
            borrower_id,
        )

    async def mark_borrower_called(
        self, borrower_id: UUID, success: bool,
    ) -> None:
        await self.pool.execute(
            """UPDATE borrowers
               SET call_attempts = call_attempts + 1,
                   last_called_at = now(),
                   locked_by = NULL,
                   locked_at = NULL,
                   is_callable = CASE
                       WHEN call_attempts + 1 >= max_attempts THEN FALSE
                       ELSE is_callable
                   END
               WHERE id = $1""",
            borrower_id,
        )

    # ---- calls ----

    async def create_call(self, c: CallRecord) -> None:
        await self.pool.execute(
            """INSERT INTO calls (id, campaign_id, borrower_id, agent_id,
               provider_name, provider_call_id, state, worker_id, version)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
               ON CONFLICT (id) DO NOTHING""",
            c.id, c.campaign_id, c.borrower_id, c.agent_id,
            c.provider_name, c.provider_call_id, c.state.value,
            c.worker_id, c.version,
        )

    async def transition_call(
        self,
        call_id: UUID,
        to_state: CallState,
        expected_version: int,
        **extra_fields: Any,
    ) -> bool:
        # Determine valid source states
        valid_sources = [
            s.value for s in CallState
            if is_valid_call_transition(s, to_state)
        ]
        if not valid_sources:
            return False

        sets = [
            "state = $1",
            "version = version + 1",
            "updated_at = now()",
        ]
        params: list = [to_state.value]
        idx = 2

        # Timestamp fields
        ts_map = {
            CallState.INITIATED: "initiated_at",
            CallState.RINGING: "ringing_at",
            CallState.ANSWERED: "answered_at",
            CallState.CONNECTED: "connected_at",
        }
        if to_state in ts_map:
            sets.append(f"{ts_map[to_state]} = now()")
        if to_state in TERMINAL_CALL_STATES:
            sets.append("completed_at = now()")

        for k, v in extra_fields.items():
            if v is not None:
                sets.append(f"{k} = ${idx}")
                params.append(v)
                idx += 1

        source_placeholders = ", ".join(
            f"${idx + i}" for i in range(len(valid_sources))
        )
        params.extend(valid_sources)
        idx += len(valid_sources)

        params.append(call_id)
        params.append(expected_version)

        sql = f"""
            UPDATE calls
            SET {', '.join(sets)}
            WHERE id = ${idx}
              AND version = ${idx + 1}
              AND state IN ({source_placeholders})
        """

        result = await self.pool.execute(sql, *params)
        return result.endswith("1")

    async def get_call(self, call_id: UUID) -> Optional[CallRecord]:
        row = await self.pool.fetchrow(
            "SELECT * FROM calls WHERE id = $1", call_id,
        )
        return _call_from_row(row) if row else None

    async def get_calls_by_state(
        self, campaign_id: UUID, state: CallState,
    ) -> List[CallRecord]:
        rows = await self.pool.fetch(
            "SELECT * FROM calls WHERE campaign_id = $1 AND state = $2",
            campaign_id, state.value,
        )
        return [_call_from_row(r) for r in rows]

    async def get_stale_calls(
        self, state: CallState, threshold_secs: int,
    ) -> List[CallRecord]:
        rows = await self.pool.fetch(
            """SELECT * FROM calls
               WHERE state = $1
                 AND COALESCE(initiated_at, created_at)
                     < now() - $2 * interval '1 second'""",
            state.value, threshold_secs,
        )
        return [_call_from_row(r) for r in rows]

    async def count_calls_by_state(
        self, campaign_id: UUID,
    ) -> Dict[CallState, int]:
        rows = await self.pool.fetch(
            """SELECT state, COUNT(*) AS cnt
               FROM calls WHERE campaign_id = $1
               GROUP BY state""",
            campaign_id,
        )
        return {CallState(r["state"]): r["cnt"] for r in rows}

    # ---- idempotency ----

    async def record_event(
        self, event_id: UUID, call_id: UUID, event_type: str,
        provider_name: Optional[str] = None,
    ) -> bool:
        result = await self.pool.execute(
            """INSERT INTO processed_events (event_id, call_id, event_type, provider_name)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (event_id) DO NOTHING""",
            event_id, call_id, event_type, provider_name,
        )
        # "INSERT 0 1" = inserted (new), "INSERT 0 0" = conflict (duplicate)
        return result.endswith("1")

    # ---- snapshot ----

    async def campaign_snapshot(self, campaign_id: UUID) -> CampaignSnapshot:
        agent_counts = await self.count_agents_by_state(campaign_id)
        call_counts = await self.count_calls_by_state(campaign_id)

        # Recent metrics from calls completed in the last 5 minutes
        row = await self.pool.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE state IN ('COMPLETED','FAILED','CANCELLED'))
                    AS total_placed,
                COUNT(*) FILTER (WHERE answered_at IS NOT NULL)
                    AS total_answered,
                COUNT(*) FILTER (WHERE state = 'FAILED')
                    AS total_failed,
                COUNT(*) FILTER (WHERE answered_at IS NOT NULL
                                  AND connected_at IS NULL
                                  AND state IN ('COMPLETED','FAILED'))
                    AS total_abandoned,
                AVG(duration_ms) FILTER (WHERE duration_ms > 0)
                    AS avg_duration_ms
            FROM calls
            WHERE campaign_id = $1
              AND created_at > now() - interval '5 minutes'
            """,
            campaign_id,
        )

        total_placed = row["total_placed"] or 0
        total_answered = row["total_answered"] or 0
        total_failed = row["total_failed"] or 0
        total_abandoned = row["total_abandoned"] or 0
        avg_dur = (row["avg_duration_ms"] or 120000) / 1000.0

        answer_rate = total_answered / total_placed if total_placed > 0 else 0.5
        abandon_rate = (
            total_abandoned / total_answered if total_answered > 0 else 0.0
        )

        return CampaignSnapshot(
            campaign_id=campaign_id,
            available_agents=agent_counts.get(AgentState.AVAILABLE, 0),
            reserved_agents=agent_counts.get(AgentState.RESERVED, 0),
            dialing_agents=agent_counts.get(AgentState.DIALING, 0),
            connected_agents=agent_counts.get(AgentState.CONNECTED, 0),
            wrap_up_agents=agent_counts.get(AgentState.WRAP_UP, 0),
            total_agents=sum(agent_counts.values()),
            calls_ringing=call_counts.get(CallState.RINGING, 0),
            calls_initiated=call_counts.get(CallState.INITIATED, 0),
            calls_connected=call_counts.get(CallState.CONNECTED, 0),
            recent_calls_placed=total_placed,
            recent_calls_answered=total_answered,
            recent_calls_failed=total_failed,
            recent_calls_abandoned=total_abandoned,
            recent_answer_rate=answer_rate,
            recent_abandon_rate=abandon_rate,
            avg_talk_time_secs=avg_dur,
            provider_healthy=True,
        )

    # ---- metrics ----

    async def record_metrics(
        self, campaign_id, window_start, window_end,
        calls_initiated, calls_answered, calls_abandoned,
        calls_failed, answer_rate, abandon_rate,
    ) -> None:
        await self.pool.execute(
            """INSERT INTO campaign_metrics
               (campaign_id, window_start, window_end,
                calls_initiated, calls_answered, calls_abandoned,
                calls_failed, answer_rate, abandon_rate)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
            campaign_id, window_start, window_end,
            calls_initiated, calls_answered, calls_abandoned,
            calls_failed, answer_rate, abandon_rate,
        )

    # ---- heartbeats ----

    async def heartbeat(self, worker_id: str, campaign_ids: List[UUID]) -> None:
        await self.pool.execute(
            """INSERT INTO worker_heartbeats (worker_id, last_heartbeat, campaigns)
               VALUES ($1, now(), $2)
               ON CONFLICT (worker_id) DO UPDATE
               SET last_heartbeat = now(), campaigns = $2""",
            worker_id, campaign_ids,
        )

    async def get_dead_workers(self, threshold_secs: int) -> List[str]:
        rows = await self.pool.fetch(
            """SELECT worker_id FROM worker_heartbeats
               WHERE last_heartbeat < now() - $1 * interval '1 second'""",
            threshold_secs,
        )
        return [r["worker_id"] for r in rows]
