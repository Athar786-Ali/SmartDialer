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
    AgentState, CallState, TERMINAL_CALL_STATES,
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
    ProviderEventType.BUSY:      CallState.FAILED,
    ProviderEventType.NO_ANSWER: CallState.FAILED,
    ProviderEventType.REJECTED:  CallState.FAILED,
}


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
        if target_call_state == CallState.FAILED:
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

        # ---- Step 5: Side-effects on agent state ----
        if call.agent_id is None:
            return

        agent = await self._store.get_agent(call.agent_id)
        if agent is None:
            return

        if target_call_state == CallState.ANSWERED:
            # Bridge agent to call → CONNECTED
            if agent.state in (AgentState.DIALING, AgentState.RESERVED):
                await self._store.transition_agent(
                    agent.id,
                    AgentState.CONNECTED,
                    agent.version,
                    current_call_id=call.id,
                )
            # Bridge call to CONNECTED (agent ↔ borrower voice path)
            updated_call = await self._store.get_call(call.id)
            if updated_call and updated_call.state == CallState.ANSWERED:
                await self._store.transition_call(
                    call.id,
                    CallState.CONNECTED,
                    updated_call.version,
                )

        elif target_call_state in (CallState.COMPLETED, CallState.FAILED):
            # Call ended — move agent to WRAP_UP (unless already OFFLINE)
            if agent.state in (
                AgentState.CONNECTED, AgentState.DIALING, AgentState.RESERVED,
            ):
                await self._store.transition_agent(
                    agent.id,
                    AgentState.WRAP_UP,
                    agent.version,
                    current_call_id=call.id,
                )
            # Also mark borrower as called
            if call.borrower_id:
                was_connected = target_call_state == CallState.COMPLETED
                await self._store.mark_borrower_called(
                    call.borrower_id,
                    success=was_connected,
                )
