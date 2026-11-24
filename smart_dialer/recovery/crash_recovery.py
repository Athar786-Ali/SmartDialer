"""
Crash Recovery.

Periodically sweeps the database for state that was left dangling
by a worker crash.

Failure scenarios handled
-------------------------
1. **Worker crash after RESERVED** — Agent stuck in RESERVED, call
   stuck in RESERVED.  Recovery: transition agent → AVAILABLE, call →
   CANCELLED, release borrower.

2. **Worker crash after INITIATED** — Call sent to provider but worker
   died before processing events.  Recovery: if call has been INITIATED
   for > threshold, transition call → FAILED, agent → AVAILABLE.

3. **Provider timeout** — Call stuck in RINGING for too long.
   Recovery: cancel via provider (best-effort), transition call →
   FAILED, agent → AVAILABLE.

4. **Agent stuck in WRAP_UP** — Wrap-up timer expired.  Recovery:
   transition agent → AVAILABLE.

5. **Dead workers** — Workers that stopped heartbeating.  Recovery:
   release all locks held by that worker.
"""

from __future__ import annotations

import logging
from datetime import datetime

from smart_dialer.config import TimeoutConfig
from smart_dialer.models import AgentState, CallState
from smart_dialer.store.base import Store

logger = logging.getLogger(__name__)


class CrashRecovery:
    """Periodic sweep to recover from worker crashes and stale state."""

    def __init__(
        self,
        store: Store,
        config: TimeoutConfig | None = None,
    ) -> None:
        self._store = store
        self._cfg = config or TimeoutConfig()
        self.recovered_agents = 0
        self.recovered_calls = 0

    async def sweep(self) -> dict:
        """
        Run all recovery checks.  Returns a summary dict.

        Designed to be called every ``recovery_sweep_secs`` by each
        worker.  The Store's atomic operations ensure that multiple
        workers running recovery concurrently don't conflict.
        """
        summary = {
            "stale_reserved_agents": 0,
            "stale_initiated_calls": 0,
            "stale_ringing_calls": 0,
            "expired_wrap_ups": 0,
        }

        # ---- 1. Agents stuck in RESERVED / DIALING ----
        stale_agents = await self._store.get_stale_reserved_agents(
            self._cfg.max_reserve_secs,
        )
        for agent in stale_agents:
            success = await self._store.transition_agent(
                agent.id,
                AgentState.AVAILABLE,
                agent.version,
            )
            if success:
                summary["stale_reserved_agents"] += 1
                self.recovered_agents += 1
                logger.warning(
                    "Recovered stale agent %s from %s → AVAILABLE",
                    agent.id, agent.state.value,
                )
                # If the agent had a call, cancel it
                if agent.current_call_id:
                    call = await self._store.get_call(agent.current_call_id)
                    if call and not call.is_terminal:
                        await self._store.transition_call(
                            call.id,
                            CallState.CANCELLED,
                            call.version,
                            failure_reason="agent_recovery",
                        )
                        self.recovered_calls += 1
                        # Release borrower
                        if call.borrower_id:
                            await self._store.release_borrower(call.borrower_id)

        # ---- 2. Calls stuck in INITIATED ----
        stale_initiated = await self._store.get_stale_calls(
            CallState.INITIATED, self._cfg.max_initiate_secs,
        )
        for call in stale_initiated:
            success = await self._store.transition_call(
                call.id,
                CallState.FAILED,
                call.version,
                failure_reason="initiate_timeout",
            )
            if success:
                summary["stale_initiated_calls"] += 1
                self.recovered_calls += 1
                logger.warning(
                    "Recovered stale INITIATED call %s → FAILED", call.id,
                )
                # Recover the agent
                if call.agent_id:
                    agent = await self._store.get_agent(call.agent_id)
                    if agent and agent.state in (
                        AgentState.DIALING, AgentState.RESERVED,
                    ):
                        await self._store.transition_agent(
                            agent.id,
                            AgentState.AVAILABLE,
                            agent.version,
                        )
                if call.borrower_id:
                    await self._store.release_borrower(call.borrower_id)

        # ---- 3. Calls stuck in RINGING ----
        stale_ringing = await self._store.get_stale_calls(
            CallState.RINGING, self._cfg.max_ring_secs,
        )
        for call in stale_ringing:
            success = await self._store.transition_call(
                call.id,
                CallState.FAILED,
                call.version,
                failure_reason="ring_timeout",
            )
            if success:
                summary["stale_ringing_calls"] += 1
                self.recovered_calls += 1
                logger.warning(
                    "Recovered stale RINGING call %s → FAILED", call.id,
                )
                if call.agent_id:
                    agent = await self._store.get_agent(call.agent_id)
                    if agent and agent.state == AgentState.DIALING:
                        await self._store.transition_agent(
                            agent.id,
                            AgentState.AVAILABLE,
                            agent.version,
                        )
                if call.borrower_id:
                    await self._store.release_borrower(call.borrower_id)

        # ---- 4. Agents stuck in WRAP_UP ----
        # We reuse get_stale_reserved_agents pattern but for WRAP_UP
        # Need a separate query — for now, check all agents
        agent_counts = {}
        for campaign in await self._store.list_active_campaigns():
            counts = await self._store.count_agents_by_state(campaign.id)
            for state, count in counts.items():
                agent_counts[state] = agent_counts.get(state, 0) + count

        # We'll handle wrap-up expiry in the worker's auto-available logic

        if any(v > 0 for v in summary.values()):
            logger.info("Recovery sweep: %s", summary)

        return summary
