"""
Idempotent Webhook / Event Handler.

Processes events from telecom providers and updates call + agent state.

Idempotency
-----------
Every ``ProviderEvent`` carries a UUID ``event_id``.  Before processing,
we attempt ``INSERT INTO processed_events … ON CONFLICT DO NOTHING``.
If the insert returns False, the event was already processed — we skip it.
This guarantees at-most-once processing even if the provider sends the
same webhook 2–3 times or our own retry logic re-delivers an event.

Out-of-order events
-------------------
The call state machine is designed to handle jumps:
  - COMPLETED arriving before ANSWERED is legal (the FSM allows any
    non-terminal → terminal transition).
  - RINGING arriving after COMPLETED is rejected by the FSM (terminal
    state is absorbing).
  - ANSWERED arriving after COMPLETED is also rejected (terminal).

The handler logs rejected transitions for observability but does not
raise exceptions — the system remains consistent.
"""

from __future__ import annotations

import logging
from typing import Dict

from smart_dialer.models import (
    AgentState, CallRecord, CallState, TERMINAL_CALL_STATES,
)
from smart_dialer.providers.base import ProviderEvent, ProviderEventType
from smart_dialer.store.base import Store

logger = logging.getLogger(__name__)

# Map provider event types to call states
_EVENT_TO_CALL_STATE: Dict[ProviderEventType, CallState] = {
    ProviderEventType.RINGING:   CallState.RINGING,
    ProviderEventType.ANSWERED:  CallState.ANSWERED,
    ProviderEventType.COMPLETED: CallState.COMPLETED,
    ProviderEventType.FAILED:    CallState.FAILED,
    ProviderEventType.CANCELLED: CallState.CANCELLED,
    ProviderEventType.BUSY:      CallState.FAILED,
    ProviderEventType.NO_ANSWER: CallState.FAILED,
    ProviderEventType.REJECTED:  CallState.FAILED,
}

_MAX_AGENT_TRANSITION_RETRIES = 3


class WebhookHandler:
    """
    Processes telecom provider events idempotently.

    Usage::

        handler = WebhookHandler(store)
        # Register as the callback for every provider:
        await provider.start(handler.handle_event)
    """

    def __init__(self, store: Store) -> None:
        self._store = store
        self._processed_count = 0
        self._duplicate_count = 0
        self._rejected_count = 0

    @property
    def stats(self) -> dict:
        return {
            "processed": self._processed_count,
            "duplicates": self._duplicate_count,
            "rejected": self._rejected_count,
        }

    async def handle_event(self, event: ProviderEvent) -> None:
        """
        Process a single provider event.

        This method is safe to call concurrently from multiple providers
        and multiple delivery attempts.
        """
        # ---- Step 1: Idempotency check ----
        is_new = await self._store.record_event(
            event.event_id,
            event.call_id,
            event.event_type.value,
            event.provider_name,
        )
        if not is_new:
            self._duplicate_count += 1
            logger.debug(
                "Duplicate event %s for call %s — skipped",
                event.event_id, event.call_id,
            )
            return

        # ---- Step 2: Map event to target call state ----
        target_call_state = _EVENT_TO_CALL_STATE.get(event.event_type)
        if target_call_state is None:
            logger.warning(
                "Unknown event type %s for call %s",
                event.event_type, event.call_id,
            )
            return

        # ---- Step 3: Load current call ----
        call = await self._store.get_call(event.call_id)
        if call is None:
            logger.warning(
                "Event %s for unknown call %s — ignored",
                event.event_type, event.call_id,
            )
            return

        # Already terminal? Ignore silently.
        if call.state in TERMINAL_CALL_STATES:
            logger.debug(
                "Call %s already %s — ignoring %s",
                call.id, call.state.value, event.event_type.value,
            )
            return

        # ---- Step 4: Transition call state ----
        extra_fields = {}
        if target_call_state in (CallState.FAILED, CallState.CANCELLED):
            extra_fields["failure_reason"] = event.metadata.get(
                "reason", event.event_type.value,
            )

        success = await self._store.transition_call(
            call.id,
            target_call_state,
            call.version,
            **extra_fields,
        )

        if not success:
            self._rejected_count += 1
            logger.info(
                "Rejected transition %s → %s for call %s (version conflict or invalid)",
                call.state.value, target_call_state.value, call.id,
            )
            return

        self._processed_count += 1
        logger.debug(
            "Call %s: %s → %s (event %s)",
            call.id, call.state.value, target_call_state.value,
            event.event_type.value,
        )

        # ---- Step 5: Side-effects on agent & borrower state ----
        if call.agent_id is not None:
            await self._update_agent_for_call_event(call, target_call_state)

        if target_call_state == CallState.ANSWERED:
            # Bridge call to CONNECTED (agent ↔ borrower voice path)
            updated_call = await self._store.get_call(call.id)
            if updated_call and updated_call.state == CallState.ANSWERED:
                await self._store.transition_call(
                    call.id,
                    CallState.CONNECTED,
                    updated_call.version,
                )
        elif target_call_state in (
            CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED,
        ):
            # Unlock / update borrower record
            if call.borrower_id:
                if target_call_state == CallState.CANCELLED:
                    await self._store.release_borrower(call.borrower_id)
                else:
                    was_connected = target_call_state == CallState.COMPLETED
                    await self._store.mark_borrower_called(
                        call.borrower_id,
                        success=was_connected,
                    )

    async def _update_agent_for_call_event(
        self,
        call: CallRecord,
        target_call_state: CallState,
    ) -> None:
        """
        Update the bound agent's state with optimistic-lock retry.

        Handles edge cases safely:
        - Never throws or overwrites an agent in OFFLINE, PAUSED, AVAILABLE, or WRAP_UP.
        - Does not release an agent already bound to a different call.
        """
        assert call.agent_id is not None

        for _ in range(_MAX_AGENT_TRANSITION_RETRIES):
            agent = await self._store.get_agent(call.agent_id)
            if agent is None:
                return

            # Guard against out-of-order / delayed events affecting a newer call
            if agent.current_call_id is not None and agent.current_call_id != call.id:
                logger.debug(
                    "Agent %s bound to different call %s (ignoring %s for %s)",
                    agent.id, agent.current_call_id, target_call_state.value, call.id,
                )
                return

            target_agent_state: AgentState | None = None
            pass_call_id = None

            if target_call_state == CallState.ANSWERED:
                if agent.state in (AgentState.DIALING, AgentState.RESERVED):
                    target_agent_state = AgentState.CONNECTED
                    pass_call_id = call.id
            elif target_call_state in (
                CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED,
            ):
                if agent.state == AgentState.CONNECTED:
                    # Completed or dropped conversation → WRAP_UP
                    target_agent_state = AgentState.WRAP_UP
                    pass_call_id = call.id
                elif agent.state in (AgentState.DIALING, AgentState.RESERVED):
                    # Call failed, was cancelled, or ended before connection →
                    # release agent immediately to AVAILABLE (clears current_call_id)
                    target_agent_state = AgentState.AVAILABLE
                # If agent is already OFFLINE, PAUSED, AVAILABLE, or WRAP_UP — no-op

            if target_agent_state is None:
                return

            updated = await self._store.transition_agent(
                agent.id,
                target_agent_state,
                agent.version,
                current_call_id=pass_call_id,
            )
            if updated:
                return

        logger.warning(
            "Exhausted optimistic-lock retries updating agent %s for call %s → %s",
            call.agent_id, call.id, target_call_state.value,
        )
