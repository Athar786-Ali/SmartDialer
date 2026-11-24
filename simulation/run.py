"""
SmartDialer Simulation Runner.

Runs the full dialer pipeline in-memory with configurable scenarios.
No PostgreSQL required — uses InMemoryStore.

Demonstrates:
  - Progressive vs Predictive dialing
  - Safety Controller decisions
  - Provider failures, duplicates, and out-of-order events
  - Agent utilisation metrics
  - Crash recovery

Usage::

    python -m simulation.run                    # default scenario B
    python -m simulation.run --scenario A       # low answer rate
    python -m simulation.run --scenario C       # high answer rate
    python -m simulation.run --scenario D       # changing conditions
    python -m simulation.run --mode predictive  # predictive dialing
    python -m simulation.run --agents 100       # 100 agents
    python -m simulation.run --provider unreliable  # flaky provider
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from uuid import UUID

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from smart_dialer.config import Config, PacingConfig, SafetyConfig, TimeoutConfig
from smart_dialer.engine.progressive import ProgressivePacingEngine
from smart_dialer.engine.predictive import PredictivePacingEngine
from smart_dialer.models import (
    AgentRecord, AgentState,
    BorrowerRecord,
    CallRecord, CallState,
    CampaignRecord, TERMINAL_CALL_STATES,
)
from smart_dialer.providers.reliable import ReliableProvider
from smart_dialer.providers.unreliable import UnreliableProvider
from smart_dialer.safety.controller import SafetyController, SafetyVerdict
from smart_dialer.store.memory_store import InMemoryStore
from smart_dialer.webhooks.handler import WebhookHandler
from smart_dialer.worker.dialer_worker import DialerWorker


# ------------------------------------------------------------------ #
# Scenarios                                                           #
# ------------------------------------------------------------------ #

@dataclass
class Scenario:
    name: str
    description: str
    answer_rate: float
    avg_talk_secs: float
    num_agents: int = 50
    num_borrowers: int = 500
    provider_name: str = "reliable"
    mode: str = "PROGRESSIVE"
    duration_secs: float = 30.0
    num_workers: int = 2
    # Chaos parameters
    agent_dropout_at: float | None = None   # seconds into sim
    agent_dropout_count: int = 0
    provider_failure_rate: float = 0.02


SCENARIOS = {
    "A": Scenario(
        name="A", description="Low answer rate (20%), long talks (120s)",
        answer_rate=0.20, avg_talk_secs=120.0,
    ),
    "B": Scenario(
        name="B", description="Medium answer rate (50%), short talks (90s)",
        answer_rate=0.50, avg_talk_secs=90.0,
    ),
    "C": Scenario(
        name="C", description="High answer rate (70%), long talks (180s)",
        answer_rate=0.70, avg_talk_secs=180.0,
    ),
    "D": Scenario(
        name="D", description="Changing conditions — rate shifts mid-simulation",
        answer_rate=0.50, avg_talk_secs=120.0,
        agent_dropout_at=15.0, agent_dropout_count=20,
    ),
}


# ------------------------------------------------------------------ #
# Simulation dashboard (plain-text)                                   #
# ------------------------------------------------------------------ #

def print_header(scenario: Scenario, mode: str) -> None:
    print("\n" + "=" * 72)
    print(f"  SmartDialer Simulation")
    print(f"  Scenario {scenario.name}: {scenario.description}")
    print(f"  Mode: {mode} | Agents: {scenario.num_agents} | "
          f"Borrowers: {scenario.num_borrowers}")
    print(f"  Provider: {scenario.provider_name} | Workers: {scenario.num_workers}")
    print(f"  Duration: {scenario.duration_secs}s")
    print("=" * 72)


def print_tick_stats(
    tick: int,
    elapsed: float,
    store: InMemoryStore,
    campaign_id: UUID,
    workers: List[DialerWorker],
    safety: SafetyController,
) -> None:
    # Synchronously peek at store internals (safe for in-memory)
    agent_states: Dict[str, int] = {}
    for a in store._agents.values():
        if a.campaign_id == campaign_id:
            s = a.state.value
            agent_states[s] = agent_states.get(s, 0) + 1

    call_states: Dict[str, int] = {}
    for c in store._calls.values():
        if c.campaign_id == campaign_id:
            s = c.state.value
            call_states[s] = call_states.get(s, 0) + 1

    total_placed = sum(w.total_calls_placed for w in workers)
    total_failed = sum(w.total_calls_failed for w in workers)

    # Safety decisions summary
    recent = safety.recent_decisions[-5:]
    verdicts = [d.verdict.value for d in recent] if recent else ["—"]

    # Agent utilisation
    total_agents = sum(agent_states.values())
    busy = sum(
        agent_states.get(s, 0)
        for s in ("RESERVED", "DIALING", "CONNECTED", "WRAP_UP")
    )
    utilisation = (busy / total_agents * 100) if total_agents > 0 else 0

    # Webhook handler stats
    wh_stats = workers[0]._webhook_handler.stats if workers else {}

    print(
        f"\n[{elapsed:6.1f}s] Tick {tick:3d}"
        f" | Agents: AV={agent_states.get('AVAILABLE', 0):3d}"
        f" RS={agent_states.get('RESERVED', 0):2d}"
        f" DI={agent_states.get('DIALING', 0):2d}"
        f" CO={agent_states.get('CONNECTED', 0):2d}"
        f" WU={agent_states.get('WRAP_UP', 0):2d}"
        f" PA={agent_states.get('PAUSED', 0):2d}"
        f" | Util: {utilisation:4.1f}%"
    )
    print(
        f"         Calls: "
        f"INI={call_states.get('INITIATED', 0):2d}"
        f" RNG={call_states.get('RINGING', 0):2d}"
        f" ANS={call_states.get('ANSWERED', 0):2d}"
        f" CON={call_states.get('CONNECTED', 0):2d}"
        f" CMP={call_states.get('COMPLETED', 0):3d}"
        f" FAI={call_states.get('FAILED', 0):3d}"
        f" CAN={call_states.get('CANCELLED', 0):2d}"
        f" | Placed={total_placed:3d}"
    )
    print(
        f"         Events: proc={wh_stats.get('processed', 0)}"
        f" dup={wh_stats.get('duplicates', 0)}"
        f" rej={wh_stats.get('rejected', 0)}"
        f" | Safety: {', '.join(verdicts[-3:])}"
    )


def print_summary(
    scenario: Scenario,
    mode: str,
    elapsed: float,
    store: InMemoryStore,
    campaign_id: UUID,
    workers: List[DialerWorker],
    safety: SafetyController,
) -> None:
    total_placed = sum(w.total_calls_placed for w in workers)
    total_failed_alloc = sum(w.total_calls_failed for w in workers)

    # Call outcomes
    completed = sum(
        1 for c in store._calls.values()
        if c.campaign_id == campaign_id and c.state == CallState.COMPLETED
    )
    failed = sum(
        1 for c in store._calls.values()
        if c.campaign_id == campaign_id and c.state == CallState.FAILED
    )
    cancelled = sum(
        1 for c in store._calls.values()
        if c.campaign_id == campaign_id and c.state == CallState.CANCELLED
    )
    connected = sum(
        1 for c in store._calls.values()
        if c.campaign_id == campaign_id and c.connected_at is not None
    )
    answered = sum(
        1 for c in store._calls.values()
        if c.campaign_id == campaign_id and c.answered_at is not None
    )
    # Abandoned: answered but never connected
    abandoned = sum(
        1 for c in store._calls.values()
        if c.campaign_id == campaign_id
        and c.answered_at is not None
        and c.connected_at is None
        and c.state in TERMINAL_CALL_STATES
    )

    answer_rate = answered / total_placed if total_placed > 0 else 0
    abandon_rate = abandoned / max(answered, 1) if answered > 0 else 0
    connect_rate = connected / total_placed if total_placed > 0 else 0

    # Average durations
    durations = [
        c.duration_ms for c in store._calls.values()
        if c.campaign_id == campaign_id and c.duration_ms and c.duration_ms > 0
    ]
    avg_duration = (sum(durations) / len(durations) / 1000) if durations else 0

    # Safety controller stats
    safety_decisions = safety.recent_decisions
    approves = sum(1 for d in safety_decisions if d.verdict == SafetyVerdict.APPROVE)
    reduces = sum(1 for d in safety_decisions if d.verdict == SafetyVerdict.REDUCE)
    rejects = sum(1 for d in safety_decisions if d.verdict == SafetyVerdict.REJECT)
    fallbacks = sum(1 for d in safety_decisions if d.verdict == SafetyVerdict.FALLBACK)

    wh = workers[0]._webhook_handler.stats if workers else {}

    print("\n" + "=" * 72)
    print(f"  SIMULATION SUMMARY — Scenario {scenario.name} ({mode})")
    print("=" * 72)
    print(f"  Duration:          {elapsed:.1f}s")
    print(f"  Calls placed:      {total_placed}")
    print(f"  Calls completed:   {completed}")
    print(f"  Calls failed:      {failed}")
    print(f"  Calls cancelled:   {cancelled}")
    print(f"  Calls answered:    {answered}")
    print(f"  Calls connected:   {connected}")
    print(f"  Calls abandoned:   {abandoned}")
    print(f"  ---")
    print(f"  Answer rate:       {answer_rate:.1%}")
    print(f"  Connect rate:      {connect_rate:.1%}")
    print(f"  Abandon rate:      {abandon_rate:.1%} "
          f"{'✓ COMPLIANT' if abandon_rate <= 0.03 else '✗ VIOLATION'}")
    print(f"  Avg talk time:     {avg_duration:.1f}s")
    print(f"  ---")
    print(f"  Events processed:  {wh.get('processed', 0)}")
    print(f"  Duplicate events:  {wh.get('duplicates', 0)}")
    print(f"  Rejected events:   {wh.get('rejected', 0)}")
    print(f"  ---")
    print(f"  Safety decisions:  approve={approves} reduce={reduces} "
          f"reject={rejects} fallback={fallbacks}")
    print(f"  Alloc failures:    {total_failed_alloc}")
    print("=" * 72)


# ------------------------------------------------------------------ #
# Main simulation coroutine                                           #
# ------------------------------------------------------------------ #

async def run_simulation(scenario: Scenario, mode: str | None = None) -> None:
    if mode:
        scenario.mode = mode.upper()

    # --- Setup ---
    store = InMemoryStore()
    await store.initialise()

    # Create campaign
    campaign = CampaignRecord.new(
        name=f"Scenario-{scenario.name}",
        mode=scenario.mode,
        provider_name=scenario.provider_name,
        target_answer_rate=scenario.answer_rate,
        avg_talk_time_secs=int(scenario.avg_talk_secs),
    )
    await store.create_campaign(campaign)

    # Create agents
    for i in range(scenario.num_agents):
        agent = AgentRecord.new(f"Agent-{i:03d}", campaign.id)
        agent.state = AgentState.AVAILABLE
        agent.state_changed_at = datetime.now(timezone.utc)
        await store.create_agent(agent)

    # Create borrowers
    for i in range(scenario.num_borrowers):
        borrower = BorrowerRecord.new(
            campaign_id=campaign.id,
            name=f"Borrower-{i:04d}",
            phone=f"+1555{i:07d}",
            priority=i % 10,
        )
        await store.create_borrower(borrower)

    # Create provider
    if scenario.provider_name == "unreliable":
        provider = UnreliableProvider(
            answer_rate=scenario.answer_rate,
            avg_talk_secs=scenario.avg_talk_secs,
            failure_rate=scenario.provider_failure_rate,
        )
    else:
        provider = ReliableProvider(
            answer_rate=scenario.answer_rate,
            avg_talk_secs=scenario.avg_talk_secs,
            failure_rate=scenario.provider_failure_rate,
        )

    providers = {provider.name: provider}

    # Create pacing engine
    pacing_config = PacingConfig(tick_interval=0.5)
    if scenario.mode == "PREDICTIVE":
        engine = PredictivePacingEngine(pacing_config)
    else:
        engine = ProgressivePacingEngine()

    # Safety controller (never bypassed)
    safety_config = SafetyConfig(
        max_abandon_rate=0.03,
        max_burst_size=min(50, scenario.num_agents),
        min_agent_buffer=max(1, scenario.num_agents // 20),
    )
    safety = SafetyController(safety_config)

    # Create workers
    config = Config(
        pacing=pacing_config,
        safety=safety_config,
        timeouts=TimeoutConfig(
            max_reserve_secs=10,
            max_ring_secs=30,
            max_initiate_secs=15,
            default_wrap_up_secs=5,
            recovery_sweep_secs=5.0,
        ),
    )

    workers: List[DialerWorker] = []
    # Create a shared webhook handler so all provider events are
    # processed through a single handler with unified stats.
    shared_handler = WebhookHandler(store)
    for i in range(scenario.num_workers):
        worker = DialerWorker(
            store=store,
            pacing_engine=engine,
            safety_controller=safety,
            providers=providers,
            config=config,
            worker_id=f"worker-{i}",
        )
        # Override the per-worker handler with the shared one
        worker._webhook_handler = shared_handler
        workers.append(worker)

    # --- Print header ---
    print_header(scenario, scenario.mode)

    # --- Start providers ONCE with the shared handler ---
    for prov in providers.values():
        await prov.start(shared_handler.handle_event)

    # Start workers (but skip provider.start inside them)
    for worker in workers:
        # Mark as running without re-starting providers
        worker._running = True
        worker._campaign_ids = [campaign.id]

    # --- Run simulation with periodic stats ---
    start_time = time.monotonic()
    tick = 0
    stats_interval = 2.0  # print every 2 seconds
    next_stats = stats_interval

    # Auto-wrap-up task: move agents from WRAP_UP → AVAILABLE after delay
    async def auto_wrap_up_loop():
        while True:
            await asyncio.sleep(1.0)
            async with store._lock:
                for agent in store._agents.values():
                    if (
                        agent.state == AgentState.WRAP_UP
                        and agent.campaign_id == campaign.id
                    ):
                        from datetime import timedelta
                        if datetime.now(timezone.utc) - agent.state_changed_at > timedelta(seconds=3):
                            agent.state = AgentState.AVAILABLE
                            agent.current_call_id = None
                            agent.worker_id = None
                            agent.state_changed_at = datetime.now(timezone.utc)
                            agent.version += 1

    wrap_task = asyncio.create_task(auto_wrap_up_loop())

    # Scenario D: agent dropout
    async def agent_dropout():
        if scenario.agent_dropout_at:
            await asyncio.sleep(scenario.agent_dropout_at)
            print(f"\n  ⚡ CHAOS: {scenario.agent_dropout_count} agents going OFFLINE!")
            dropped = 0
            async with store._lock:
                for agent in store._agents.values():
                    if (
                        agent.campaign_id == campaign.id
                        and agent.state == AgentState.AVAILABLE
                        and dropped < scenario.agent_dropout_count
                    ):
                        agent.state = AgentState.OFFLINE
                        agent.state_changed_at = datetime.now(timezone.utc)
                        agent.version += 1
                        dropped += 1

    dropout_task = asyncio.create_task(agent_dropout())

    # Create worker tasks
    worker_tasks = [
        asyncio.create_task(worker.run())
        for worker in workers
    ]

    try:
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed >= scenario.duration_secs:
                break

            if elapsed >= next_stats:
                tick += 1
                print_tick_stats(
                    tick, elapsed, store, campaign.id, workers, safety,
                )
                next_stats += stats_interval

            await asyncio.sleep(0.1)
    finally:
        # Stop workers (just flag them, don't stop providers through workers)
        for worker in workers:
            worker._running = False
        for task in worker_tasks:
            task.cancel()
        wrap_task.cancel()
        dropout_task.cancel()
        # Stop providers
        for prov in providers.values():
            await prov.stop()

        # Wait for cancellation
        await asyncio.gather(*worker_tasks, wrap_task, dropout_task, return_exceptions=True)

    # --- Summary ---
    elapsed = time.monotonic() - start_time
    print_summary(scenario, scenario.mode, elapsed, store, campaign.id, workers, safety)


# ------------------------------------------------------------------ #
# CLI entry point                                                     #
# ------------------------------------------------------------------ #

def main() -> None:
    parser = argparse.ArgumentParser(description="SmartDialer Simulation")
    parser.add_argument(
        "--scenario", choices=list(SCENARIOS.keys()), default="B",
        help="Scenario to run (A/B/C/D)",
    )
    parser.add_argument(
        "--mode", choices=["progressive", "predictive"], default=None,
        help="Dialing mode (overrides scenario default)",
    )
    parser.add_argument(
        "--agents", type=int, default=None,
        help="Number of agents (overrides scenario default)",
    )
    parser.add_argument(
        "--borrowers", type=int, default=None,
        help="Number of borrowers",
    )
    parser.add_argument(
        "--duration", type=float, default=None,
        help="Simulation duration in seconds",
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help="Number of concurrent workers",
    )
    parser.add_argument(
        "--provider", choices=["reliable", "unreliable"], default=None,
        help="Telecom provider to use",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)-30s %(levelname)-5s %(message)s",
    )

    scenario = SCENARIOS[args.scenario]
    if args.agents:
        scenario.num_agents = args.agents
    if args.borrowers:
        scenario.num_borrowers = args.borrowers
    if args.duration:
        scenario.duration_secs = args.duration
    if args.workers:
        scenario.num_workers = args.workers
    if args.provider:
        scenario.provider_name = args.provider

    asyncio.run(run_simulation(scenario, args.mode))


if __name__ == "__main__":
    main()
