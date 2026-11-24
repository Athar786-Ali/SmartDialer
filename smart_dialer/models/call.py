"""
Call state machine.

Models the lifecycle of an outbound collections call from queue to
completion.  Transitions are validated against an explicit whitelist,
with special handling for:

* **Terminal states** — once a call reaches COMPLETED / FAILED / CANCELLED,
  further events are silently ignored (idempotent).
* **Out-of-order events** — the provider may send COMPLETED before ANSWERED.
  Direct jumps to terminal states from any non-terminal state are allowed.
* **Duplicate events** — handled upstream by the idempotent event table, but
  the state machine also refuses no-op same-state transitions.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import FrozenSet, Optional, Set, Tuple
from uuid import UUID, uuid4


# ------------------------------------------------------------------ #
# State enum                                                          #
# ------------------------------------------------------------------ #

class CallState(str, enum.Enum):
    """Possible states for an outbound call."""
    QUEUED     = "QUEUED"      # In the call list, not yet claimed
    RESERVED   = "RESERVED"    # Agent assigned, dial not yet sent
    INITIATED  = "INITIATED"   # Dial request sent to telecom provider
    RINGING    = "RINGING"     # Provider confirmed borrower phone is ringing
    ANSWERED   = "ANSWERED"    # Borrower picked up (not yet bridged to agent)
    CONNECTED  = "CONNECTED"   # Agent ↔ Borrower voice path established
    COMPLETED  = "COMPLETED"   # Call ended normally
    FAILED     = "FAILED"      # Call could not be connected
    CANCELLED  = "CANCELLED"   # Call cancelled before connection


# ------------------------------------------------------------------ #
# Transition rules                                                    #
# ------------------------------------------------------------------ #

TERMINAL_CALL_STATES: Set[CallState] = {
    CallState.COMPLETED,
    CallState.FAILED,
    CallState.CANCELLED,
}

# Explicit forward transitions (happy path + known error paths)
_EXPLICIT_TRANSITIONS: FrozenSet[Tuple[CallState, CallState]] = frozenset({
    # Normal flow
    (CallState.QUEUED,    CallState.RESERVED),
    (CallState.RESERVED,  CallState.INITIATED),
    (CallState.INITIATED, CallState.RINGING),
    (CallState.RINGING,   CallState.ANSWERED),
    (CallState.ANSWERED,  CallState.CONNECTED),
    (CallState.CONNECTED, CallState.COMPLETED),

    # Provider may skip RINGING and go straight to ANSWERED
    (CallState.INITIATED, CallState.ANSWERED),

    # Failures at various stages
    (CallState.INITIATED, CallState.FAILED),
    (CallState.RINGING,   CallState.FAILED),
    (CallState.ANSWERED,  CallState.FAILED),
    (CallState.CONNECTED, CallState.FAILED),

    # Cancellations
    (CallState.QUEUED,    CallState.CANCELLED),
    (CallState.RESERVED,  CallState.CANCELLED),
    (CallState.INITIATED, CallState.CANCELLED),
    (CallState.RINGING,   CallState.CANCELLED),
})


def is_valid_call_transition(
    from_state: CallState,
    to_state: CallState,
) -> bool:
    """
    Return True if *from_state* → *to_state* is legal.

    Rules:
    1. No transition *from* a terminal state (call is done).
    2. Any non-terminal state can jump to COMPLETED / FAILED / CANCELLED
       (handles out-of-order events gracefully).
    3. Otherwise the transition must be in the explicit whitelist.
    """
    if from_state in TERMINAL_CALL_STATES:
        return False  # call is already done — ignore further events
    if from_state == to_state:
        return False  # no-op
    if to_state in TERMINAL_CALL_STATES:
        return True   # always allow jumps to terminal
    return (from_state, to_state) in _EXPLICIT_TRANSITIONS


# ------------------------------------------------------------------ #
# Call record (value object)                                          #
# ------------------------------------------------------------------ #

@dataclass
class CallRecord:
    """Lightweight snapshot of a call row."""
    id: UUID
    campaign_id: UUID
    borrower_id: UUID
    agent_id: Optional[UUID] = None
    provider_name: Optional[str] = None
    provider_call_id: Optional[str] = None
    state: CallState = CallState.QUEUED
    worker_id: Optional[str] = None
    initiated_at: Optional[datetime] = None
    ringing_at: Optional[datetime] = None
    answered_at: Optional[datetime] = None
    connected_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    failure_reason: Optional[str] = None
    duration_ms: Optional[int] = None
    version: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @staticmethod
    def new(
        campaign_id: UUID,
        borrower_id: UUID,
        agent_id: Optional[UUID] = None,
    ) -> "CallRecord":
        return CallRecord(
            id=uuid4(),
            campaign_id=campaign_id,
            borrower_id=borrower_id,
            agent_id=agent_id,
            created_at=datetime.now(timezone.utc),
        )

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_CALL_STATES
