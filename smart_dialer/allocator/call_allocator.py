"""
Call Allocator.

Responsible for the atomic sequence:
  1. Reserve an available agent (SKIP LOCKED).
  2. Reserve the next callable borrower (SKIP LOCKED).
  3. Create a call record linking agent ↔ borrower.
  4. Submit the call to the telecom provider.
  5. Transition agent → DIALING, call → INITIATED.

If any step fails, all prior reservations are rolled back so neither
the agent nor the borrower is left in a dangling state.

The allocator is called by the DialerWorker after the Safety Controller
has approved a batch of calls.  It allocates one call at a time in a
loop, stopping early if agents or borrowers run out.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional
from uuid import UUID

from smart_dialer.models import (
    AgentRecord, AgentState,
    CallRecord, CallState,
    BorrowerRecord,
)
from smart_dialer.providers.base import TelecomProvider, PlaceCallResult
from smart_dialer.store.base import Store

logger = logging.getLogger(__name__)


@dataclass
class AllocationResult:
    """Result of a single call allocation attempt."""
    success: bool
    call_id: Optional[UUID] = None
    agent_id: Optional[UUID] = None
    borrower_id: Optional[UUID] = None
    error: Optional[str] = None


class CallAllocator:
    """
    Allocates agents to borrowers and places calls.

    Thread-safety: each ``allocate_one`` call is self-contained and uses
    the Store's atomic primitives.  Multiple workers can call this
    concurrently without coordination — the Store's SKIP LOCKED
    semantics prevent double-booking.
    """

    def __init__(
        self,
        store: Store,
        providers: Dict[str, TelecomProvider],
    ) -> None:
        self._store = store
        self._providers = providers

    async def allocate_batch(
        self,
        campaign_id: UUID,
        provider_name: str,
        count: int,
        worker_id: str,
    ) -> list[AllocationResult]:
        """
        Attempt to allocate up to *count* calls for *campaign_id*.

        Returns one ``AllocationResult`` per attempt.  Stops early if
        agents or borrowers are exhausted.
        """
        results: list[AllocationResult] = []
        for _ in range(count):
            result = await self.allocate_one(
                campaign_id, provider_name, worker_id,
            )
            results.append(result)
            if not result.success:
                # If we ran out of agents or borrowers, stop the batch
                if result.error in (
                    "no_available_agent",
                    "no_callable_borrower",
                ):
                    break
        return results

    async def allocate_one(
        self,
        campaign_id: UUID,
        provider_name: str,
        worker_id: str,
    ) -> AllocationResult:
        """
        Allocate a single agent → borrower → call → provider chain.

        On failure at any step, prior reservations are rolled back.
        """
        agent: Optional[AgentRecord] = None
        borrower: Optional[BorrowerRecord] = None

        try:
            # Step 1: Reserve an agent (atomic, SKIP LOCKED)
            agent = await self._store.reserve_available_agent(
                campaign_id, worker_id,
            )
            if agent is None:
                return AllocationResult(
                    success=False, error="no_available_agent",
                )

            # Step 2: Reserve a borrower (atomic, SKIP LOCKED)
            borrower = await self._store.reserve_next_borrower(
                campaign_id, worker_id,
            )
            if borrower is None:
                # Roll back agent reservation
                await self._store.transition_agent(
                    agent.id, AgentState.AVAILABLE, agent.version,
                )
                return AllocationResult(
                    success=False, error="no_callable_borrower",
                )

            # Step 3: Create the call record
            call = CallRecord.new(
                campaign_id=campaign_id,
                borrower_id=borrower.id,
                agent_id=agent.id,
            )
            call.state = CallState.RESERVED
            call.worker_id = worker_id
            call.provider_name = provider_name
            await self._store.create_call(call)

            # Step 4: Transition agent to DIALING
            agent_dialing = await self._store.transition_agent(
                agent.id,
                AgentState.DIALING,
                agent.version,
                current_call_id=call.id,
                worker_id=worker_id,
            )
            if not agent_dialing:
                # Agent state changed under us — cancel everything
                await self._store.transition_call(
                    call.id, CallState.CANCELLED, call.version,
                    failure_reason="agent_state_conflict",
                )
                await self._store.release_borrower(borrower.id)
                return AllocationResult(
                    success=False, error="agent_state_conflict",
                )

            # Step 5: Place call with the telecom provider
            provider = self._providers.get(provider_name)
            if provider is None:
                # Unknown provider — cancel
                await self._rollback(agent, call, borrower, "unknown_provider")
                return AllocationResult(
                    success=False, error="unknown_provider",
                )

            place_result: PlaceCallResult = await provider.place_call(
                call.id, borrower.phone,
            )

            if not place_result.success:
                await self._rollback(
                    agent, call, borrower,
                    f"provider_error: {place_result.error}",
                )
                return AllocationResult(
                    success=False, error=f"provider_error: {place_result.error}",
                )

            # Step 6: Transition call to INITIATED
            # We need to re-read the call to get current version
            # since create_call set version=0 and state=RESERVED
            await self._store.transition_call(
                call.id,
                CallState.INITIATED,
                call.version,
                provider_call_id=place_result.provider_call_id,
            )

            logger.info(
                "Allocated call %s: agent=%s borrower=%s provider=%s",
                call.id, agent.id, borrower.id, provider_name,
            )

            return AllocationResult(
                success=True,
                call_id=call.id,
                agent_id=agent.id,
                borrower_id=borrower.id,
            )

        except Exception as e:
            logger.exception("Allocation failed: %s", e)
            # Best-effort rollback
            if agent and borrower:
                try:
                    await self._store.transition_agent(
                        agent.id, AgentState.AVAILABLE, agent.version,
                    )
                except Exception:
                    pass
                try:
                    await self._store.release_borrower(borrower.id)
                except Exception:
                    pass
            elif agent:
                try:
                    await self._store.transition_agent(
                        agent.id, AgentState.AVAILABLE, agent.version,
                    )
                except Exception:
                    pass
            return AllocationResult(
                success=False, error=f"exception: {e}",
            )

    async def _rollback(
        self,
        agent: AgentRecord,
        call: CallRecord,
        borrower: BorrowerRecord,
        reason: str,
    ) -> None:
        """Roll back a failed allocation."""
        try:
            # Agent version was bumped when we transitioned to DIALING
            # Re-read to get current version
            current_agent = await self._store.get_agent(agent.id)
            if current_agent:
                await self._store.transition_agent(
                    current_agent.id,
                    AgentState.AVAILABLE,
                    current_agent.version,
                )
        except Exception:
            logger.exception("Failed to rollback agent %s", agent.id)

        try:
            current_call = await self._store.get_call(call.id)
            if current_call and not current_call.is_terminal:
                await self._store.transition_call(
                    call.id,
                    CallState.FAILED,
                    current_call.version,
                    failure_reason=reason,
                )
        except Exception:
            logger.exception("Failed to rollback call %s", call.id)

        try:
            await self._store.release_borrower(borrower.id)
        except Exception:
            logger.exception("Failed to release borrower %s", borrower.id)
