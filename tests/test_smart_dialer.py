"""
Comprehensive test suite for the SmartDialer system.

Tests cover:
  - Agent state machine transitions (valid + invalid)
  - Call state machine transitions (valid + invalid + out-of-order)
  - Safety Controller decisions
  - Idempotent event processing
  - Concurrent agent reservation (no double-booking)
  - Pacing engine calculations
  - Failure scenarios (crash recovery, provider outage)
  - Duplicate event handling

All tests use the InMemoryStore — no PostgreSQL required.
"""

import asyncio
import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from smart_dialer.config import Config, SafetyConfig, PacingConfig
from smart_dialer.models import (
    AgentState, AgentRecord, is_valid_agent_transition,
    CallState, CallRecord, is_valid_call_transition, TERMINAL_CALL_STATES,
    CampaignRecord, BorrowerRecord,
)
from smart_dialer.store.memory_store import InMemoryStore
from smart_dialer.engine.base import CampaignSnapshot, PacingDecision
from smart_dialer.engine.progressive import ProgressivePacingEngine
from smart_dialer.engine.predictive import PredictivePacingEngine
from smart_dialer.safety.controller import SafetyController, SafetyVerdict
from smart_dialer.providers.base import ProviderEvent, ProviderEventType
from smart_dialer.webhooks.handler import WebhookHandler
from smart_dialer.recovery.crash_recovery import CrashRecovery
from smart_dialer.allocator.call_allocator import CallAllocator
from smart_dialer.providers.reliable import ReliableProvider
from smart_dialer.providers.unreliable import UnreliableProvider


# ================================================================== #
# Fixtures                                                             #
# ================================================================== #

@pytest_asyncio.fixture
async def store():
    s = InMemoryStore()
    await s.initialise()
    yield s
    await s.close()


@pytest.fixture
def campaign_id():
    return uuid4()


@pytest_asyncio.fixture
async def seeded_store(store, campaign_id):
    """Store pre-populated with a campaign, agents, and borrowers."""
    campaign = CampaignRecord(
        id=campaign_id, name="Test Campaign",
        mode="PROGRESSIVE", provider_name="reliable",
    )
    await store.create_campaign(campaign)

    for i in range(10):
        agent = AgentRecord.new(f"Agent-{i}", campaign_id)
        agent.state = AgentState.AVAILABLE
        agent.state_changed_at = datetime.now(timezone.utc)
        await store.create_agent(agent)

    for i in range(50):
        borrower = BorrowerRecord.new(
            campaign_id=campaign_id,
            name=f"Borrower-{i}",
            phone=f"+1555000{i:04d}",
        )
        await store.create_borrower(borrower)

    return store


# ================================================================== #
# 1. Agent State Machine Tests                                        #
# ================================================================== #

class TestAgentStateMachine:
    """Test the agent state machine transition rules."""

    def test_valid_happy_path(self):
        """Full lifecycle: OFFLINE → AVAILABLE → RESERVED → … → AVAILABLE."""
        path = [
            (AgentState.OFFLINE, AgentState.AVAILABLE),
            (AgentState.AVAILABLE, AgentState.RESERVED),
            (AgentState.RESERVED, AgentState.DIALING),
            (AgentState.DIALING, AgentState.CONNECTED),
            (AgentState.CONNECTED, AgentState.WRAP_UP),
            (AgentState.WRAP_UP, AgentState.AVAILABLE),
        ]
        for from_s, to_s in path:
            assert is_valid_agent_transition(from_s, to_s), f"{from_s} → {to_s}"

    def test_invalid_skip_states(self):
        """Cannot jump from AVAILABLE directly to CONNECTED."""
        assert not is_valid_agent_transition(
            AgentState.AVAILABLE, AgentState.CONNECTED
        )

    def test_invalid_backward(self):
        """Cannot go from CONNECTED back to RESERVED."""
        assert not is_valid_agent_transition(
            AgentState.CONNECTED, AgentState.RESERVED
        )

    def test_offline_from_any_active_state(self):
        """Agent can disconnect from any active state."""
        for state in AgentState:
            if state != AgentState.OFFLINE:
                assert is_valid_agent_transition(state, AgentState.OFFLINE), (
                    f"{state} → OFFLINE should be valid"
                )

    def test_reservation_cancel(self):
        """RESERVED → AVAILABLE is valid (call failed before dial)."""
        assert is_valid_agent_transition(
            AgentState.RESERVED, AgentState.AVAILABLE
        )

    def test_dialing_failure(self):
        """DIALING → AVAILABLE is valid (provider rejected call)."""
        assert is_valid_agent_transition(
            AgentState.DIALING, AgentState.AVAILABLE
        )

    def test_pause_from_available(self):
        assert is_valid_agent_transition(
            AgentState.AVAILABLE, AgentState.PAUSED
        )

    def test_cannot_pause_from_connected(self):
        assert not is_valid_agent_transition(
            AgentState.CONNECTED, AgentState.PAUSED
        )

    @pytest.mark.asyncio
    async def test_version_conflict_prevents_transition(self, seeded_store, campaign_id):
        """Optimistic locking: wrong version → transition fails."""
        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        assert agent is not None
        # Try to transition with wrong version
        result = await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, expected_version=999,
        )
        assert result is False


# ================================================================== #
# 2. Call State Machine Tests                                          #
# ================================================================== #

class TestCallStateMachine:
    """Test call state machine transitions including edge cases."""

    def test_happy_path(self):
        path = [
            (CallState.QUEUED, CallState.RESERVED),
            (CallState.RESERVED, CallState.INITIATED),
            (CallState.INITIATED, CallState.RINGING),
            (CallState.RINGING, CallState.ANSWERED),
            (CallState.ANSWERED, CallState.CONNECTED),
            (CallState.CONNECTED, CallState.COMPLETED),
        ]
        for from_s, to_s in path:
            assert is_valid_call_transition(from_s, to_s), f"{from_s} → {to_s}"

    def test_terminal_state_absorbs(self):
        """Once in COMPLETED/FAILED/CANCELLED, nothing changes."""
        for terminal in TERMINAL_CALL_STATES:
            for target in CallState:
                assert not is_valid_call_transition(terminal, target), (
                    f"{terminal} → {target} should be invalid"
                )

    def test_out_of_order_completed_before_answered(self):
        """Provider sends COMPLETED while call is RINGING (skips ANSWERED)."""
        assert is_valid_call_transition(
            CallState.RINGING, CallState.COMPLETED
        )

    def test_out_of_order_completed_from_initiated(self):
        """Provider fast-fails: INITIATED → COMPLETED."""
        assert is_valid_call_transition(
            CallState.INITIATED, CallState.COMPLETED
        )

    def test_skip_ringing(self):
        """Provider goes INITIATED → ANSWERED (no RINGING event)."""
        assert is_valid_call_transition(
            CallState.INITIATED, CallState.ANSWERED
        )

    def test_cannot_go_backward(self):
        """ANSWERED → RINGING is invalid."""
        assert not is_valid_call_transition(
            CallState.ANSWERED, CallState.RINGING
        )

    def test_no_self_transition(self):
        """Same-state transitions are rejected."""
        for state in CallState:
            assert not is_valid_call_transition(state, state)


# ================================================================== #
# 3. Safety Controller Tests                                           #
# ================================================================== #

class TestSafetyController:
    """Test the safety boundary — the pacing engine cannot bypass it."""

    def test_approve_normal_request(self):
        safety = SafetyController(SafetyConfig(
            max_burst_size=50, min_agent_buffer=2,
        ))
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=20,
            recent_abandon_rate=0.0,
            provider_healthy=True,
        )
        request = PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=10,
            available_agents=20,
            agents_on_calls=5,
            calls_in_flight=0,
        )
        result = safety.evaluate(request, snapshot)
        assert result.verdict == SafetyVerdict.APPROVE
        assert result.approved_calls == 10

    def test_reduce_exceeds_burst(self):
        safety = SafetyController(SafetyConfig(max_burst_size=5))
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(), available_agents=100,
        )
        request = PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=50,
            available_agents=100,
            agents_on_calls=0,
            calls_in_flight=0,
        )
        result = safety.evaluate(request, snapshot)
        assert result.verdict == SafetyVerdict.REDUCE
        assert result.approved_calls <= 5

    def test_fallback_on_high_abandon_rate(self):
        safety = SafetyController(SafetyConfig(
            fallback_threshold=0.05,
        ))
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=50,
            recent_abandon_rate=0.06,  # above threshold
        )
        request = PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=30,
            available_agents=50,
            agents_on_calls=10,
            calls_in_flight=5,
        )
        result = safety.evaluate(request, snapshot)
        assert result.verdict == SafetyVerdict.FALLBACK

    def test_reject_unhealthy_provider(self):
        safety = SafetyController(SafetyConfig())
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=50,
            provider_healthy=False,
        )
        request = PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=20,
            available_agents=50,
            agents_on_calls=0,
            calls_in_flight=0,
        )
        result = safety.evaluate(request, snapshot)
        # Should reduce to 1 (probe)
        assert result.approved_calls <= 1

    def test_agent_buffer_enforced(self):
        safety = SafetyController(SafetyConfig(
            min_agent_buffer=5, max_burst_size=100,
        ))
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(), available_agents=6,
        )
        request = PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=6,
            available_agents=6,
            agents_on_calls=0,
            calls_in_flight=0,
        )
        result = safety.evaluate(request, snapshot)
        assert result.approved_calls <= 1  # 6 - 5 buffer = 1

    def test_zero_agents_means_reject(self):
        safety = SafetyController()
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(), available_agents=0,
        )
        request = PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=0,
            available_agents=0,
            agents_on_calls=0,
            calls_in_flight=0,
        )
        result = safety.evaluate(request, snapshot)
        assert result.approved_calls == 0


# ================================================================== #
# 4. Idempotency Tests                                                #
# ================================================================== #

class TestIdempotency:
    """Duplicate events must not cause duplicate state transitions."""

    @pytest.mark.asyncio
    async def test_duplicate_event_ignored(self, store):
        event_id = uuid4()
        call_id = uuid4()

        first = await store.record_event(event_id, call_id, "ANSWERED")
        assert first is True

        second = await store.record_event(event_id, call_id, "ANSWERED")
        assert second is False

    @pytest.mark.asyncio
    async def test_different_events_both_recorded(self, store):
        call_id = uuid4()

        r1 = await store.record_event(uuid4(), call_id, "RINGING")
        r2 = await store.record_event(uuid4(), call_id, "ANSWERED")
        assert r1 is True
        assert r2 is True

    @pytest.mark.asyncio
    async def test_triple_duplicate_all_idempotent(self, store):
        """Provider sends ANSWERED 3 times — only first is processed."""
        event_id = uuid4()
        call_id = uuid4()

        results = []
        for _ in range(3):
            results.append(
                await store.record_event(event_id, call_id, "ANSWERED")
            )
        assert results == [True, False, False]


# ================================================================== #
# 5. Concurrency Tests                                                #
# ================================================================== #

class TestConcurrency:
    """
    Verify that concurrent workers cannot double-book agents or borrowers.
    """

    @pytest.mark.asyncio
    async def test_no_double_booking_agents(self, seeded_store, campaign_id):
        """10 concurrent workers each try to reserve an agent.
        We have 10 agents, so each should get exactly one."""
        async def reserve_one(worker_num: int):
            return await seeded_store.reserve_available_agent(
                campaign_id, f"worker-{worker_num}",
            )

        results = await asyncio.gather(
            *[reserve_one(i) for i in range(10)]
        )
        agents = [r for r in results if r is not None]
        agent_ids = [a.id for a in agents]

        # All 10 agents should be reserved
        assert len(agents) == 10
        # No duplicates
        assert len(set(agent_ids)) == 10

    @pytest.mark.asyncio
    async def test_excess_workers_get_none(self, seeded_store, campaign_id):
        """15 workers compete for 10 agents — 5 must get None."""
        async def reserve_one(worker_num: int):
            return await seeded_store.reserve_available_agent(
                campaign_id, f"worker-{worker_num}",
            )

        results = await asyncio.gather(
            *[reserve_one(i) for i in range(15)]
        )
        agents = [r for r in results if r is not None]
        nones = [r for r in results if r is None]

        assert len(agents) == 10
        assert len(nones) == 5
        assert len(set(a.id for a in agents)) == 10

    @pytest.mark.asyncio
    async def test_no_double_booking_borrowers(self, seeded_store, campaign_id):
        """Multiple workers reserving borrowers — no duplicates."""
        async def reserve_one(worker_num: int):
            return await seeded_store.reserve_next_borrower(
                campaign_id, f"worker-{worker_num}",
            )

        results = await asyncio.gather(
            *[reserve_one(i) for i in range(20)]
        )
        borrowers = [r for r in results if r is not None]
        borrower_ids = [b.id for b in borrowers]

        assert len(set(borrower_ids)) == len(borrower_ids)  # no dupes

    @pytest.mark.asyncio
    async def test_concurrent_state_transitions(self, seeded_store, campaign_id):
        """Two workers try to transition the same agent — only one succeeds."""
        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        assert agent is not None

        # Both try to move to DIALING with the same version
        r1 = await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, agent.version,
        )
        r2 = await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, agent.version,
        )

        # First succeeds, second fails (version already bumped)
        assert r1 is True
        assert r2 is False


# ================================================================== #
# 6. Pacing Engine Tests                                               #
# ================================================================== #

class TestProgressivePacing:
    @pytest.mark.asyncio
    async def test_requests_one_per_agent(self):
        engine = ProgressivePacingEngine()
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=10,
            calls_initiated=0,
            calls_ringing=0,
        )
        result = await engine.compute(snapshot)
        assert result.requested_calls == 10
        assert result.mode == "PROGRESSIVE"

    @pytest.mark.asyncio
    async def test_subtracts_in_flight(self):
        engine = ProgressivePacingEngine()
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=10,
            calls_initiated=3,
            calls_ringing=2,
        )
        result = await engine.compute(snapshot)
        assert result.requested_calls == 5  # 10 - 3 - 2

    @pytest.mark.asyncio
    async def test_zero_agents(self):
        engine = ProgressivePacingEngine()
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=0,
        )
        result = await engine.compute(snapshot)
        assert result.requested_calls == 0


class TestPredictivePacing:
    @pytest.mark.asyncio
    async def test_overdialing_with_low_answer_rate(self):
        engine = PredictivePacingEngine(PacingConfig(
            safety_margin=0.85, min_sample_size=0,
        ))
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=10,
            recent_answer_rate=0.20,  # low answer rate
            calls_initiated=0,
            calls_ringing=0,
            wrap_up_agents=0,
            provider_healthy=True,
        )
        result = await engine.compute(snapshot)
        # With 20% answer rate, should request significantly more than 10
        assert result.requested_calls > 10
        assert result.mode == "PREDICTIVE"

    @pytest.mark.asyncio
    async def test_conservative_with_high_answer_rate(self):
        engine = PredictivePacingEngine(PacingConfig(
            safety_margin=0.85, min_sample_size=0,
        ))
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=10,
            recent_answer_rate=0.90,
            calls_initiated=0,
            calls_ringing=0,
            wrap_up_agents=0,
            provider_healthy=True,
        )
        result = await engine.compute(snapshot)
        # With 90% answer rate, should request close to 10
        assert result.requested_calls <= 15

    @pytest.mark.asyncio
    async def test_ramp_up_phase(self):
        engine = PredictivePacingEngine(PacingConfig(
            min_sample_size=50, ramp_up_factor=0.5,
        ))
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=10,
            recent_answer_rate=0.50,
            recent_calls_placed=5,  # low sample count
        )
        result = await engine.compute(snapshot)
        # Should be conservative during ramp-up
        full_engine = PredictivePacingEngine(PacingConfig(
            min_sample_size=0,
        ))
        full_result = await full_engine.compute(snapshot)
        assert result.requested_calls <= full_result.requested_calls


# ================================================================== #
# 7. Failure Scenario Tests                                            #
# ================================================================== #

class TestFailureScenarios:
    """Test recovery from various failure modes."""

    @pytest.mark.asyncio
    async def test_crash_recovery_stale_reservation(self, seeded_store, campaign_id):
        """Worker crashes after reserving an agent — recovery frees it."""
        agent = await seeded_store.reserve_available_agent(campaign_id, "crashed-worker")
        assert agent is not None
        assert agent.state == AgentState.RESERVED

        # Simulate time passing (make the reservation stale)
        async with seeded_store._lock:
            seeded_store._agents[agent.id].state_changed_at = (
                datetime.now(timezone.utc) - timedelta(seconds=60)
            )

        from smart_dialer.config import TimeoutConfig
        recovery = CrashRecovery(seeded_store, TimeoutConfig(max_reserve_secs=10))
        summary = await recovery.sweep()

        assert summary["stale_reserved_agents"] >= 1

        # Agent should be AVAILABLE again
        recovered = await seeded_store.get_agent(agent.id)
        assert recovered.state == AgentState.AVAILABLE

    @pytest.mark.asyncio
    async def test_crash_recovery_skips_active_ringing_call_within_ring_timeout(
        self, seeded_store, campaign_id,
    ):
        """A DIALING agent with a live RINGING call within max_ring_secs is not killed early."""
        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        call = CallRecord.new(campaign_id, uuid4(), agent.id)
        call.state = CallState.RINGING
        call.initiated_at = datetime.now(timezone.utc) - timedelta(seconds=12)
        call.version = 1
        await seeded_store.create_call(call)

        await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, agent.version, current_call_id=call.id,
        )
        # Age the agent past max_reserve_secs (10s), but call is still < max_ring_secs (30s)
        async with seeded_store._lock:
            seeded_store._agents[agent.id].state_changed_at = (
                datetime.now(timezone.utc) - timedelta(seconds=12)
            )

        from smart_dialer.config import TimeoutConfig
        recovery = CrashRecovery(
            seeded_store, TimeoutConfig(max_reserve_secs=10, max_ring_secs=30),
        )
        summary = await recovery.sweep()

        assert summary["stale_reserved_agents"] == 0
        assert summary["stale_ringing_calls"] == 0
        still_dialing = await seeded_store.get_agent(agent.id)
        assert still_dialing.state == AgentState.DIALING
        still_ringing = await seeded_store.get_call(call.id)
        assert still_ringing.state == CallState.RINGING

    @pytest.mark.asyncio
    async def test_out_of_order_events_handled(self, seeded_store, campaign_id):
        """COMPLETED arrives before ANSWERED — call ends cleanly."""
        # Create a call in RINGING state
        call = CallRecord.new(campaign_id, uuid4())
        call.state = CallState.RINGING
        call.version = 1
        await seeded_store.create_call(call)

        # Transition directly to COMPLETED (skipping ANSWERED)
        result = await seeded_store.transition_call(
            call.id, CallState.COMPLETED, call.version,
        )
        assert result is True

        updated = await seeded_store.get_call(call.id)
        assert updated.state == CallState.COMPLETED

    @pytest.mark.asyncio
    async def test_event_after_terminal_ignored(self, seeded_store, campaign_id):
        """ANSWERED arriving after COMPLETED is silently ignored."""
        call = CallRecord.new(campaign_id, uuid4())
        call.state = CallState.COMPLETED
        call.version = 2
        await seeded_store.create_call(call)

        result = await seeded_store.transition_call(
            call.id, CallState.ANSWERED, call.version,
        )
        assert result is False

        unchanged = await seeded_store.get_call(call.id)
        assert unchanged.state == CallState.COMPLETED

    @pytest.mark.asyncio
    async def test_duplicate_provider_events(self, seeded_store, campaign_id):
        """Same ANSWERED event delivered 3 times — only processed once."""
        handler = WebhookHandler(seeded_store)

        # Create a call
        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        call = CallRecord.new(campaign_id, uuid4(), agent.id)
        call.state = CallState.RINGING
        call.version = 1
        call.provider_name = "reliable"
        call.provider_call_id = "test-123"
        await seeded_store.create_call(call)

        # Same event 3 times
        event = ProviderEvent.make(
            "reliable", "test-123", call.id,
            ProviderEventType.ANSWERED,
        )
        await handler.handle_event(event)
        await handler.handle_event(event)
        await handler.handle_event(event)

        assert handler.stats["processed"] == 1
        assert handler.stats["duplicates"] == 2

    @pytest.mark.asyncio
    async def test_provider_outage_reduces_pacing(self):
        """When the provider is down, safety controller reduces to probe."""
        safety = SafetyController(SafetyConfig())
        snapshot = CampaignSnapshot(
            campaign_id=uuid4(),
            available_agents=50,
            provider_healthy=False,
        )
        request = PacingDecision(
            campaign_id=snapshot.campaign_id,
            requested_calls=30,
            available_agents=50,
            agents_on_calls=0,
            calls_in_flight=0,
        )
        result = safety.evaluate(request, snapshot)
        assert result.approved_calls <= 1

    @pytest.mark.asyncio
    async def test_agent_dropout_reflected_in_snapshot(self, seeded_store, campaign_id):
        """When agents go OFFLINE, snapshot shows reduced availability."""
        # Start: 10 available agents
        snapshot1 = await seeded_store.campaign_snapshot(campaign_id)
        assert snapshot1.available_agents == 10

        # Offline 5 agents
        agents = list(seeded_store._agents.values())[:5]
        for a in agents:
            if a.campaign_id == campaign_id:
                async with seeded_store._lock:
                    a.state = AgentState.OFFLINE
                    a.version += 1

        snapshot2 = await seeded_store.campaign_snapshot(campaign_id)
        assert snapshot2.available_agents == 5


# ================================================================== #
# 8. Webhook Handler Tests                                            #
# ================================================================== #

class TestWebhookHandler:
    @pytest.mark.asyncio
    async def test_ringing_event_transitions_call(self, seeded_store, campaign_id):
        handler = WebhookHandler(seeded_store)

        call = CallRecord.new(campaign_id, uuid4())
        call.state = CallState.INITIATED
        call.version = 1
        await seeded_store.create_call(call)

        event = ProviderEvent.make(
            "reliable", "prov-123", call.id,
            ProviderEventType.RINGING,
        )
        await handler.handle_event(event)

        updated = await seeded_store.get_call(call.id)
        assert updated.state == CallState.RINGING

    @pytest.mark.asyncio
    async def test_answered_event_connects_agent(self, seeded_store, campaign_id):
        handler = WebhookHandler(seeded_store)

        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, agent.version,
        )
        agent = await seeded_store.get_agent(agent.id)

        call = CallRecord.new(campaign_id, uuid4(), agent.id)
        call.state = CallState.RINGING
        call.version = 1
        await seeded_store.create_call(call)

        event = ProviderEvent.make(
            "reliable", "prov-456", call.id,
            ProviderEventType.ANSWERED,
        )
        await handler.handle_event(event)

        updated_call = await seeded_store.get_call(call.id)
        # After ANSWERED, the handler auto-bridges to CONNECTED
        assert updated_call.state == CallState.CONNECTED

        updated_agent = await seeded_store.get_agent(agent.id)
        assert updated_agent.state == AgentState.CONNECTED

    @pytest.mark.asyncio
    async def test_completed_event_wraps_up_agent(self, seeded_store, campaign_id):
        handler = WebhookHandler(seeded_store)

        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        # Move through states
        await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, agent.version,
        )
        agent = await seeded_store.get_agent(agent.id)
        await seeded_store.transition_agent(
            agent.id, AgentState.CONNECTED, agent.version,
        )
        agent = await seeded_store.get_agent(agent.id)

        call = CallRecord.new(campaign_id, uuid4(), agent.id)
        call.state = CallState.CONNECTED
        call.version = 1
        call.borrower_id = uuid4()
        await seeded_store.create_call(call)

        event = ProviderEvent.make(
            "reliable", "prov-789", call.id,
            ProviderEventType.COMPLETED,
        )
        await handler.handle_event(event)

        updated_agent = await seeded_store.get_agent(agent.id)
        assert updated_agent.state == AgentState.WRAP_UP

    @pytest.mark.asyncio
    async def test_failed_call_releases_agent_from_dialing(self, seeded_store, campaign_id):
        """When a call FAILS while agent is DIALING, agent returns to AVAILABLE and clears call ref."""
        handler = WebhookHandler(seeded_store)

        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        call = CallRecord.new(campaign_id, uuid4(), agent.id)
        call.state = CallState.RINGING
        call.version = 1
        call.provider_name = "reliable"
        call.provider_call_id = "prov-fail-1"
        await seeded_store.create_call(call)

        await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, agent.version,
            current_call_id=call.id, worker_id="w1",
        )

        event = ProviderEvent.make(
            "reliable", "prov-fail-1", call.id,
            ProviderEventType.FAILED, reason="NO_ANSWER",
        )
        await handler.handle_event(event)

        updated_agent = await seeded_store.get_agent(agent.id)
        assert updated_agent.state == AgentState.AVAILABLE
        assert updated_agent.current_call_id is None
        assert updated_agent.worker_id is None

        updated_call = await seeded_store.get_call(call.id)
        assert updated_call.state == CallState.FAILED

    @pytest.mark.asyncio
    async def test_failed_call_releases_agent_from_reserved(self, seeded_store, campaign_id):
        """When NO_ANSWER/FAILED arrives while agent is RESERVED, agent returns to AVAILABLE."""
        handler = WebhookHandler(seeded_store)

        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        call = CallRecord.new(campaign_id, uuid4(), agent.id)
        call.state = CallState.INITIATED
        call.version = 1
        call.provider_name = "reliable"
        call.provider_call_id = "prov-fail-res"
        await seeded_store.create_call(call)

        event = ProviderEvent.make(
            "reliable", "prov-fail-res", call.id,
            ProviderEventType.NO_ANSWER,
        )
        await handler.handle_event(event)

        updated_agent = await seeded_store.get_agent(agent.id)
        assert updated_agent.state == AgentState.AVAILABLE
        assert updated_agent.current_call_id is None
        assert updated_agent.worker_id is None

        updated_call = await seeded_store.get_call(call.id)
        assert updated_call.state == CallState.FAILED

    @pytest.mark.asyncio
    async def test_cancelled_call_releases_agent_from_reserved_and_dialing(
        self, seeded_store, campaign_id,
    ):
        """CANCELLED webhook events immediately release RESERVED or DIALING agents to AVAILABLE."""
        handler = WebhookHandler(seeded_store)

        # Case 1: Agent in RESERVED
        agent_reserved = await seeded_store.reserve_available_agent(campaign_id, "w1")
        call1 = CallRecord.new(campaign_id, uuid4(), agent_reserved.id)
        call1.state = CallState.RESERVED
        call1.version = 0
        call1.provider_name = "reliable"
        call1.provider_call_id = "prov-cancel-res"
        await seeded_store.create_call(call1)

        await handler.handle_event(ProviderEvent.make(
            "reliable", "prov-cancel-res", call1.id,
            ProviderEventType.CANCELLED, reason="agent_timeout",
        ))

        updated_call1 = await seeded_store.get_call(call1.id)
        assert updated_call1.state == CallState.CANCELLED
        updated_agent1 = await seeded_store.get_agent(agent_reserved.id)
        assert updated_agent1.state == AgentState.AVAILABLE
        assert updated_agent1.current_call_id is None

        # Case 2: Agent in DIALING
        agent_dialing = await seeded_store.reserve_available_agent(campaign_id, "w1")
        call2 = CallRecord.new(campaign_id, uuid4(), agent_dialing.id)
        call2.state = CallState.RINGING
        call2.version = 1
        call2.provider_name = "reliable"
        call2.provider_call_id = "prov-cancel-dial"
        await seeded_store.create_call(call2)

        await seeded_store.transition_agent(
            agent_dialing.id, AgentState.DIALING, agent_dialing.version,
            current_call_id=call2.id, worker_id="w1",
        )

        await handler.handle_event(ProviderEvent.make(
            "reliable", "prov-cancel-dial", call2.id,
            ProviderEventType.CANCELLED, reason="campaign_stopped",
        ))

        updated_call2 = await seeded_store.get_call(call2.id)
        assert updated_call2.state == CallState.CANCELLED
        updated_agent2 = await seeded_store.get_agent(agent_dialing.id)
        assert updated_agent2.state == AgentState.AVAILABLE
        assert updated_agent2.current_call_id is None
        assert updated_agent2.worker_id is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("inactive_state", [
        AgentState.OFFLINE,
        AgentState.PAUSED,
        AgentState.AVAILABLE,
    ])
    async def test_failed_and_cancelled_do_not_overwrite_inactive_or_available_agent(
        self, seeded_store, campaign_id, inactive_state,
    ):
        """If agent is already OFFLINE, PAUSED, or AVAILABLE, terminal events must not throw or overwrite."""
        handler = WebhookHandler(seeded_store)

        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        if inactive_state == AgentState.PAUSED:
            await seeded_store.transition_agent(agent.id, AgentState.AVAILABLE, agent.version)
            agent = await seeded_store.get_agent(agent.id)
            await seeded_store.transition_agent(agent.id, AgentState.PAUSED, agent.version)
        elif inactive_state == AgentState.AVAILABLE:
            await seeded_store.transition_agent(agent.id, AgentState.AVAILABLE, agent.version)
        elif inactive_state == AgentState.OFFLINE:
            await seeded_store.transition_agent(agent.id, AgentState.OFFLINE, agent.version)

        agent = await seeded_store.get_agent(agent.id)
        assert agent.state == inactive_state
        initial_version = agent.version

        call = CallRecord.new(campaign_id, uuid4(), agent.id)
        call.state = CallState.RINGING
        call.version = 1
        call.provider_name = "reliable"
        call.provider_call_id = f"prov-safe-{inactive_state.value}"
        await seeded_store.create_call(call)

        event_type = (
            ProviderEventType.CANCELLED
            if inactive_state == AgentState.PAUSED
            else ProviderEventType.FAILED
        )
        await handler.handle_event(ProviderEvent.make(
            "reliable", call.provider_call_id, call.id, event_type,
        ))

        updated_agent = await seeded_store.get_agent(agent.id)
        assert updated_agent.state == inactive_state
        assert updated_agent.version == initial_version

    @pytest.mark.asyncio
    async def test_delayed_failure_for_old_call_does_not_release_agent_on_new_call(
        self, seeded_store, campaign_id,
    ):
        """Delayed FAILED for old call must not release an agent now dialing a newer call."""
        handler = WebhookHandler(seeded_store)

        agent = await seeded_store.reserve_available_agent(campaign_id, "w1")
        new_call = CallRecord.new(campaign_id, uuid4(), agent.id)
        new_call.state = CallState.RINGING
        await seeded_store.create_call(new_call)

        await seeded_store.transition_agent(
            agent.id, AgentState.DIALING, agent.version,
            current_call_id=new_call.id, worker_id="w1",
        )

        old_call = CallRecord.new(campaign_id, uuid4(), agent.id)
        old_call.state = CallState.RINGING
        old_call.version = 1
        await seeded_store.create_call(old_call)

        await handler.handle_event(ProviderEvent.make(
            "reliable", "prov-old", old_call.id, ProviderEventType.FAILED,
        ))

        updated_agent = await seeded_store.get_agent(agent.id)
        assert updated_agent.state == AgentState.DIALING
        assert updated_agent.current_call_id == new_call.id


# ================================================================== #
# 9. Integration Test                                                  #
# ================================================================== #

class TestIntegration:
    """End-to-end test: full pipeline with mock provider."""

    @pytest.mark.asyncio
    async def test_full_call_lifecycle(self, seeded_store, campaign_id):
        """One complete call: allocate → dial → ring → answer → complete."""
        provider = ReliableProvider(
            answer_rate=1.0,  # always answer
            avg_ring_secs=0.1,
            avg_talk_secs=0.5,
            failure_rate=0.0,
        )
        handler = WebhookHandler(seeded_store)
        await provider.start(handler.handle_event)

        allocator = CallAllocator(seeded_store, {"reliable": provider})

        result = await allocator.allocate_one(campaign_id, "reliable", "w1")
        assert result.success is True
        assert result.call_id is not None

        # Wait for the call to complete (provider min talk = 0.05s with 0.5s avg)
        for _ in range(50):
            await asyncio.sleep(0.1)
            call = await seeded_store.get_call(result.call_id)
            if call.state in TERMINAL_CALL_STATES:
                break

        call = await seeded_store.get_call(result.call_id)
        assert call.state in TERMINAL_CALL_STATES

        await provider.stop()
