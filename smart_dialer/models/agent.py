"""
Agent state machine.

Models the lifecycle of a collections agent from login to logout.
Every state transition is validated against an explicit whitelist;
invalid transitions are rejected rather than silently applied.

Concurrency note
----------------
Two workers seeing the same AVAILABLE agent must not both reserve it.
In PostgreSQL this is solved with ``SELECT … FOR UPDATE SKIP LOCKED``:
the first worker locks the row; the second worker's query *skips* it
and picks the next available agent.  No contention, no double-booking.

In the in-memory store the equivalent is ``asyncio.Lock.acquire(blocking=False)``
per agent record — the loser gets ``False`` and moves on.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import FrozenSet, Optional, Tuple
from uuid import UUID, uuid4


# ------------------------------------------------------------------ #
# State enum                                                          #
# ------------------------------------------------------------------ #

class AgentState(str, enum.Enum):
    """Possible states for a collections agent."""
    OFFLINE   = "OFFLINE"     # Not logged in
    AVAILABLE = "AVAILABLE"   # Ready to take a call
    RESERVED  = "RESERVED"    # Claimed by pacing engine, call not yet placed
    DIALING   = "DIALING"     # Outbound call initiated via provider
    CONNECTED = "CONNECTED"   # Talking to a borrower
    WRAP_UP   = "WRAP_UP"     # Post-call notes / disposition
    PAUSED    = "PAUSED"      # Manually paused (break, training, etc.)


# ------------------------------------------------------------------ #
# Transition rules                                                    #
# ------------------------------------------------------------------ #

VALID_AGENT_TRANSITIONS: FrozenSet[Tuple[AgentState, AgentState]] = frozenset({
    # Login / logout
    (AgentState.OFFLINE,   AgentState.AVAILABLE),

    # Normal call flow
    (AgentState.AVAILABLE, AgentState.RESERVED),
    (AgentState.RESERVED,  AgentState.DIALING),
    (AgentState.DIALING,   AgentState.CONNECTED),
    (AgentState.CONNECTED, AgentState.WRAP_UP),
    (AgentState.WRAP_UP,   AgentState.AVAILABLE),

    # Call failure — agent goes back to AVAILABLE
    (AgentState.RESERVED,  AgentState.AVAILABLE),
    (AgentState.DIALING,   AgentState.AVAILABLE),

    # Manual pause / resume
    (AgentState.AVAILABLE, AgentState.PAUSED),
    (AgentState.WRAP_UP,   AgentState.PAUSED),
    (AgentState.PAUSED,    AgentState.AVAILABLE),

    # Agent disconnect (can happen from any active state)
    (AgentState.AVAILABLE, AgentState.OFFLINE),
    (AgentState.RESERVED,  AgentState.OFFLINE),
    (AgentState.DIALING,   AgentState.OFFLINE),
    (AgentState.CONNECTED, AgentState.OFFLINE),
    (AgentState.WRAP_UP,   AgentState.OFFLINE),
    (AgentState.PAUSED,    AgentState.OFFLINE),
})


def is_valid_agent_transition(
    from_state: AgentState,
    to_state: AgentState,
) -> bool:
    """Return True if *from_state* → *to_state* is a legal transition."""
    return (from_state, to_state) in VALID_AGENT_TRANSITIONS


# ------------------------------------------------------------------ #
# Agent record (value object)                                         #
# ------------------------------------------------------------------ #

@dataclass
class AgentRecord:
    """Lightweight snapshot of an agent row — used across store boundaries."""
    id: UUID
    name: str
    campaign_id: Optional[UUID]
    state: AgentState
    current_call_id: Optional[UUID] = None
    worker_id: Optional[str] = None
    state_changed_at: datetime = field(default_factory=datetime.utcnow)
    version: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)

    @staticmethod
    def new(name: str, campaign_id: Optional[UUID] = None) -> "AgentRecord":
        now = datetime.utcnow()
        return AgentRecord(
            id=uuid4(),
            name=name,
            campaign_id=campaign_id,
            state=AgentState.OFFLINE,
            state_changed_at=now,
            version=0,
            created_at=now,
        )
