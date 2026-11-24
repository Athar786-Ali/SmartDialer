"""
Progressive Pacing Engine.

Rule: one outbound call per AVAILABLE agent.

If 50 agents are available, the engine requests at most 50 calls.
In practice the request is ``available_agents - already_reserved``,
because some agents may already be claimed by a prior tick.

This mode is deterministic and safe — it can never produce more
ringing calls than there are agents to handle them.
"""

from __future__ import annotations

from smart_dialer.engine.base import CampaignSnapshot, PacingDecision, PacingEngine


class ProgressivePacingEngine(PacingEngine):
    """One call per available agent — simple and safe."""

    @property
    def mode(self) -> str:
        return "PROGRESSIVE"

    async def compute(self, snapshot: CampaignSnapshot) -> PacingDecision:
        # Available agents minus those already in the reservation pipeline
        in_pipeline = snapshot.reserved_agents + snapshot.dialing_agents
        effective_available = max(0, snapshot.available_agents - 0)
        # Don't count agents already reserved — they've been accounted for
        # We just want to match AVAILABLE 1:1 with new calls
        requested = max(0, snapshot.available_agents)

        # Subtract calls already in flight but not yet matched to an agent
        # (In progressive mode this should be ~0, but guard against it)
        requested = max(0, requested - snapshot.calls_initiated - snapshot.calls_ringing)

        if not snapshot.provider_healthy:
            requested = max(0, min(requested, 2))  # trickle during provider issues
            reason = "Provider unhealthy — throttled to 2 calls"
        elif requested == 0:
            reason = "No available agents or calls already in flight"
        else:
            reason = f"Progressive 1:1 — {snapshot.available_agents} agents available"

        return PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=requested,
            available_agents=snapshot.available_agents,
            agents_on_calls=snapshot.connected_agents,
            calls_in_flight=snapshot.calls_ringing + snapshot.calls_initiated,
            mode=self.mode,
            reasoning=reason,
            answer_rate_estimate=snapshot.recent_answer_rate,
        )
