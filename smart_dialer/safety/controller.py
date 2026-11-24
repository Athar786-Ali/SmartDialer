"""
Safety Controller.

Sits between the Pacing Engine and the Call Allocator.  The pacing
engine proposes a number of calls; the Safety Controller decides
whether that is *actually allowed*.

The pacing engine **cannot bypass** this layer.  There is no
``force=True`` flag, no admin override, no "just this once" escape
hatch.  The safety boundary is structural, not configurable.

Decisions
---------
The controller can:
  - **APPROVE**     — the full request is safe.
  - **REDUCE**      — allow fewer calls than requested.
  - **REJECT**      — block the request entirely.
  - **FALLBACK**    — switch to progressive mode (1:1) for this tick.

Checks performed (in order)
----------------------------
1. **Hard cap** — never exceed ``max_burst_size`` in a single tick.
2. **Abandon rate** — if recent abandon rate ≥ threshold, fall back.
3. **Agent buffer** — always keep ≥ N agents as a buffer.
4. **Unmatched ringing** — cap calls ringing without an assigned agent.
5. **Provider health** — if the provider is failing, reduce aggressively.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from smart_dialer.config import SafetyConfig
from smart_dialer.engine.base import CampaignSnapshot, PacingDecision


# ------------------------------------------------------------------ #
# Safety decision                                                     #
# ------------------------------------------------------------------ #

class SafetyVerdict(str, enum.Enum):
    APPROVE  = "APPROVE"
    REDUCE   = "REDUCE"
    REJECT   = "REJECT"
    FALLBACK = "FALLBACK"


@dataclass(frozen=True)
class SafetyDecision:
    """Output of the Safety Controller."""
    verdict: SafetyVerdict
    approved_calls: int
    original_request: int
    reasons: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"SafetyDecision({self.verdict.value}, "
            f"approved={self.approved_calls}/{self.original_request}, "
            f"reasons={self.reasons})"
        )


# ------------------------------------------------------------------ #
# Safety Controller                                                   #
# ------------------------------------------------------------------ #

class SafetyController:
    """
    Hard safety boundary — the pacing engine cannot dial without
    passing through this gate.

    Design principle: the controller is *separate* from the pacing
    engine and cannot be disabled, reconfigured at runtime by the
    pacing engine, or bypassed through any code path.
    """

    def __init__(self, config: SafetyConfig | None = None) -> None:
        self._cfg = config or SafetyConfig()
        self._decisions_log: List[SafetyDecision] = []

    @property
    def recent_decisions(self) -> List[SafetyDecision]:
        """Last 100 decisions for observability."""
        return self._decisions_log[-100:]

    def evaluate(
        self,
        request: PacingDecision,
        snapshot: CampaignSnapshot,
    ) -> SafetyDecision:
        """
        Evaluate a pacing request against safety constraints.

        Parameters
        ----------
        request : PacingDecision
            What the pacing engine wants.
        snapshot : CampaignSnapshot
            Current state of the campaign (agents + calls + metrics).

        Returns
        -------
        SafetyDecision
            How many calls are actually approved.
        """
        reasons: List[str] = []
        allowed = request.requested_calls

        if allowed <= 0:
            decision = SafetyDecision(
                verdict=SafetyVerdict.APPROVE,
                approved_calls=0,
                original_request=0,
                reasons=["No calls requested"],
            )
            self._decisions_log.append(decision)
            return decision

        # ---- Check 1: Hard burst cap ----
        if allowed > self._cfg.max_burst_size:
            reasons.append(
                f"Burst cap: {allowed} → {self._cfg.max_burst_size}"
            )
            allowed = self._cfg.max_burst_size

        # ---- Check 2: Abandon rate threshold ----
        if snapshot.recent_abandon_rate >= self._cfg.fallback_threshold:
            # Severe: fall back to progressive (1:1)
            progressive_allowed = max(0, snapshot.available_agents)
            reasons.append(
                f"FALLBACK: abandon rate {snapshot.recent_abandon_rate:.2%} "
                f"≥ {self._cfg.fallback_threshold:.2%} — "
                f"progressive mode ({progressive_allowed} calls)"
            )
            decision = SafetyDecision(
                verdict=SafetyVerdict.FALLBACK,
                approved_calls=min(allowed, progressive_allowed),
                original_request=request.requested_calls,
                reasons=reasons,
            )
            self._decisions_log.append(decision)
            return decision

        if snapshot.recent_abandon_rate >= self._cfg.max_abandon_rate:
            # Warning: reduce by 50 %
            reduced = max(1, allowed // 2)
            reasons.append(
                f"Abandon rate {snapshot.recent_abandon_rate:.2%} "
                f"≥ {self._cfg.max_abandon_rate:.2%} — "
                f"reduced {allowed} → {reduced}"
            )
            allowed = reduced

        # ---- Check 3: Agent buffer ----
        # Always keep min_agent_buffer agents free
        in_flight = snapshot.calls_initiated + snapshot.calls_ringing
        agents_after_dial = snapshot.available_agents - allowed
        if agents_after_dial < self._cfg.min_agent_buffer:
            safe_allowed = max(
                0,
                snapshot.available_agents - self._cfg.min_agent_buffer,
            )
            if safe_allowed < allowed:
                reasons.append(
                    f"Agent buffer: keep ≥{self._cfg.min_agent_buffer} free — "
                    f"{allowed} → {safe_allowed}"
                )
                allowed = safe_allowed

        # ---- Check 4: Unmatched ringing cap ----
        # calls_ringing that don't have a free agent waiting
        unmatched_ringing = max(
            0,
            in_flight - snapshot.available_agents,
        )
        future_unmatched = unmatched_ringing + allowed - snapshot.available_agents
        if future_unmatched > self._cfg.max_unmatched_ringing:
            safe_allowed = max(
                0,
                allowed - (future_unmatched - self._cfg.max_unmatched_ringing),
            )
            if safe_allowed < allowed:
                reasons.append(
                    f"Unmatched ringing cap: {future_unmatched} would exceed "
                    f"{self._cfg.max_unmatched_ringing} — {allowed} → {safe_allowed}"
                )
                allowed = safe_allowed

        # ---- Check 5: Provider health ----
        if not snapshot.provider_healthy:
            # Provider is down — allow at most 1 call (probe)
            if allowed > 1:
                reasons.append(
                    f"Provider unhealthy — {allowed} → 1 (probe only)"
                )
                allowed = 1
        elif snapshot.provider_error_rate > 0.3:
            reduced = max(1, allowed // 3)
            if reduced < allowed:
                reasons.append(
                    f"Provider error rate {snapshot.provider_error_rate:.0%} — "
                    f"{allowed} → {reduced}"
                )
                allowed = reduced

        # ---- Check 6: Ensure we don't go negative ----
        allowed = max(0, allowed)

        # ---- Determine verdict ----
        if allowed == 0:
            verdict = SafetyVerdict.REJECT
            if not reasons:
                reasons.append("No calls can be safely placed")
        elif allowed == request.requested_calls:
            verdict = SafetyVerdict.APPROVE
            if not reasons:
                reasons.append("All checks passed")
        else:
            verdict = SafetyVerdict.REDUCE

        decision = SafetyDecision(
            verdict=verdict,
            approved_calls=allowed,
            original_request=request.requested_calls,
            reasons=reasons,
        )
        self._decisions_log.append(decision)
        return decision
