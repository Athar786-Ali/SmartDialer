"""
Pacing engine abstraction.

The pacing engine answers one question each tick:

    "How many new outbound calls should we start right now?"

It returns a ``PacingDecision`` that is *advisory* — the Safety Controller
sits downstream and can approve, reduce, or reject the request.

Two implementations are provided:
  - **Progressive** — one call per available agent (safe, deterministic).
  - **Predictive** — over-dials based on statistical estimates (higher
    utilisation, requires the safety controller to prevent abandons).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID


# ------------------------------------------------------------------ #
# Pacing decision                                                     #
# ------------------------------------------------------------------ #

@dataclass(frozen=True)
class PacingDecision:
    """Advisory output of a pacing engine tick."""
    campaign_id: UUID
    requested_calls: int               # how many calls the engine wants to place
    available_agents: int              # agents in AVAILABLE state at decision time
    agents_on_calls: int               # agents in CONNECTED state
    calls_in_flight: int               # calls in INITIATED / RINGING
    mode: str = "PROGRESSIVE"          # which engine produced this
    reasoning: str = ""                # human-readable explanation
    answer_rate_estimate: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"PacingDecision(mode={self.mode}, requested={self.requested_calls}, "
            f"avail_agents={self.available_agents}, in_flight={self.calls_in_flight}, "
            f"reason={self.reasoning!r})"
        )


# ------------------------------------------------------------------ #
# Campaign snapshot (read by the pacing engine each tick)              #
# ------------------------------------------------------------------ #

@dataclass(frozen=True)
class CampaignSnapshot:
    """Point-in-time metrics the pacing engine uses to decide."""
    campaign_id: UUID
    available_agents: int = 0
    reserved_agents: int = 0
    dialing_agents: int = 0
    connected_agents: int = 0
    wrap_up_agents: int = 0
    total_agents: int = 0

    calls_ringing: int = 0
    calls_initiated: int = 0
    calls_connected: int = 0

    # Sliding-window metrics
    recent_calls_placed: int = 0
    recent_calls_answered: int = 0
    recent_calls_failed: int = 0
    recent_calls_abandoned: int = 0
    recent_answer_rate: float = 0.5
    recent_abandon_rate: float = 0.0
    avg_talk_time_secs: float = 120.0
    avg_ring_time_secs: float = 8.0

    # Provider
    provider_healthy: bool = True
    provider_error_rate: float = 0.0


# ------------------------------------------------------------------ #
# Abstract pacing engine                                              #
# ------------------------------------------------------------------ #

class PacingEngine(ABC):
    """Base class for progressive / predictive pacing engines."""

    @property
    @abstractmethod
    def mode(self) -> str:
        """'PROGRESSIVE' or 'PREDICTIVE'."""

    @abstractmethod
    async def compute(self, snapshot: CampaignSnapshot) -> PacingDecision:
        """
        Given the current campaign snapshot, decide how many calls to request.
        """
