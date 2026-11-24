"""
Predictive Pacing Engine.

Decides how aggressively to over-dial by estimating how many calls
will actually be answered and how many agents will become free.

Algorithm
---------
The core idea is simple:

    desired_concurrent_ringing = available_agents / answer_rate

If the answer rate is 50 %, we need ~2 calls ringing per agent to keep
one agent busy.  A safety margin (default 0.85) prevents the last few
percent of over-dialing that cause most abandons.

We also consider:

* **Calls already in flight** — subtract INITIATED + RINGING calls.
* **Agents about to become free** — agents in WRAP_UP are likely to
  become AVAILABLE within seconds; we can speculatively count a
  fraction of them.
* **Provider health** — if the provider is failing, slow down.
* **Abandon rate feedback** — if the recent abandon rate is climbing,
  reduce the over-dial ratio dynamically.
* **EMA smoothing** — the answer-rate estimate uses an exponential
  moving average so sudden spikes don't cause whiplash.

Why not Erlang-C?
-----------------
Erlang-C assumes Poisson arrivals and exponential service times, which
is a reasonable approximation but adds complexity without necessarily
improving accuracy in a campaign-based outbound setting where we
*control* the arrival rate.  A direct ratio-based model with feedback
is simpler, easier to explain, and just as effective when paired with
the Safety Controller downstream.
"""

from __future__ import annotations

from smart_dialer.config import PacingConfig
from smart_dialer.engine.base import CampaignSnapshot, PacingDecision, PacingEngine


class PredictivePacingEngine(PacingEngine):
    """Statistical over-dialing with EMA-smoothed answer rate."""

    def __init__(self, config: PacingConfig | None = None) -> None:
        self._cfg = config or PacingConfig()
        self._ema_answer_rate: float | None = None
        self._samples_seen: int = 0

    @property
    def mode(self) -> str:
        return "PREDICTIVE"

    async def compute(self, snapshot: CampaignSnapshot) -> PacingDecision:
        reasons: list[str] = []

        # ---- 1. Update the EMA answer rate ----
        self._samples_seen += snapshot.recent_calls_placed
        if self._ema_answer_rate is None:
            self._ema_answer_rate = snapshot.recent_answer_rate
        else:
            alpha = self._cfg.ema_alpha
            self._ema_answer_rate = (
                alpha * snapshot.recent_answer_rate
                + (1 - alpha) * self._ema_answer_rate
            )

        answer_rate = max(0.05, self._ema_answer_rate)  # floor at 5 %
        reasons.append(f"EMA answer rate: {answer_rate:.2%}")

        # ---- 2. Ramp-up guard ----
        # If we don't have enough samples, be conservative
        if self._samples_seen < self._cfg.min_sample_size:
            ramp = self._cfg.ramp_up_factor
            reasons.append(
                f"Ramp-up phase ({self._samples_seen}/{self._cfg.min_sample_size} samples)"
            )
        else:
            ramp = 1.0

        # ---- 3. Effective agent capacity ----
        # Count AVAILABLE agents plus a fraction of WRAP_UP agents
        # (they'll become available soon)
        wrap_up_credit = int(snapshot.wrap_up_agents * 0.5)
        effective_agents = snapshot.available_agents + wrap_up_credit
        reasons.append(
            f"Effective agents: {effective_agents} "
            f"({snapshot.available_agents} avail + {wrap_up_credit} wrap-up credit)"
        )

        if effective_agents <= 0:
            return PacingDecision(
                campaign_id=snapshot.campaign_id,
                requested_calls=0,
                available_agents=snapshot.available_agents,
                agents_on_calls=snapshot.connected_agents,
                calls_in_flight=(
                    snapshot.calls_ringing + snapshot.calls_initiated
                ),
                mode=self.mode,
                reasoning="No effective agents available",
                answer_rate_estimate=answer_rate,
            )

        # ---- 4. Over-dial ratio ----
        # Base ratio: how many calls per effective agent
        overdial_ratio = 1.0 / answer_rate
        reasons.append(f"Base over-dial ratio: {overdial_ratio:.2f}")

        # Apply safety margin
        overdial_ratio *= self._cfg.safety_margin
        reasons.append(f"After safety margin ({self._cfg.safety_margin}): {overdial_ratio:.2f}")

        # Apply ramp-up factor
        overdial_ratio *= ramp

        # ---- 5. Abandon rate feedback ----
        # If abandon rate is rising, pull back proportionally
        if snapshot.recent_abandon_rate > 0.01:
            # Linear pullback: at 3% abandon → multiply by 0.5
            pullback = max(0.3, 1.0 - (snapshot.recent_abandon_rate / 0.06))
            overdial_ratio *= pullback
            reasons.append(
                f"Abandon rate pullback ({snapshot.recent_abandon_rate:.2%}): "
                f"×{pullback:.2f}"
            )

        # ---- 6. Provider health ----
        if not snapshot.provider_healthy:
            overdial_ratio *= 0.3
            reasons.append("Provider unhealthy — throttled ×0.3")
        elif snapshot.provider_error_rate > 0.1:
            factor = max(0.5, 1.0 - snapshot.provider_error_rate)
            overdial_ratio *= factor
            reasons.append(f"Provider errors ({snapshot.provider_error_rate:.0%}) — ×{factor:.2f}")

        # ---- 7. Compute desired concurrent ringing ----
        desired_ringing = int(effective_agents * overdial_ratio)

        # Subtract what's already in flight
        in_flight = snapshot.calls_initiated + snapshot.calls_ringing
        requested = max(0, desired_ringing - in_flight)
        reasons.append(
            f"Desired ringing: {desired_ringing}, in-flight: {in_flight}, "
            f"new calls: {requested}"
        )

        return PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=requested,
            available_agents=snapshot.available_agents,
            agents_on_calls=snapshot.connected_agents,
            calls_in_flight=in_flight,
            mode=self.mode,
            reasoning=" | ".join(reasons),
            answer_rate_estimate=answer_rate,
        )
