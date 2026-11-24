# SmartDialer — Intelligent Collections Dialing System

A high-concurrency outbound dialer for lending platform collections, supporting both **progressive** and **predictive** dialing modes with a **hard safety boundary** to prevent abandoned-call compliance violations.

## Quick Start

```bash
# No external dependencies required for simulation
# (PostgreSQL only needed for production mode)

# Install Python dependencies
pip install -r requirements.txt

# Run the test suite (45 tests, ~1 second)
python -m pytest tests/ -v

# Run simulations
python -m simulation.run --scenario B --mode progressive --duration 30
python -m simulation.run --scenario A --mode predictive --duration 30
python -m simulation.run --scenario C --mode predictive --provider unreliable
python -m simulation.run --scenario D --mode predictive --agents 50

# See all options
python -m simulation.run --help
```

### With PostgreSQL (production mode)

```bash
docker-compose up -d
# The schema auto-applies on first connection via PgStore.initialise()
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Dialer Worker(s)                         │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐    │
│  │ Campaign │───▶│ Pacing Engine│───▶│ Safety Controller  │    │
│  │ Snapshot │    │  (Prog/Pred) │    │  (APPROVE/REDUCE/  │    │
│  └──────────┘    └──────────────┘    │   REJECT/FALLBACK) │    │
│                                      └─────────┬──────────┘    │
│                                                │               │
│                                      ┌─────────▼──────────┐    │
│                                      │  Call Allocator     │    │
│                                      │  (SKIP LOCKED)      │    │
│                                      └─────────┬──────────┘    │
│                                                │               │
│                                      ┌─────────▼──────────┐    │
│                                      │  Telecom Provider   │    │
│                                      │  (Reliable / Flaky) │    │
│                                      └─────────┬──────────┘    │
│                                                │               │
│                                      ┌─────────▼──────────┐    │
│                                      │  Webhook Handler    │    │
│                                      │  (Idempotent)       │    │
│                                      └─────────┬──────────┘    │
│                                                │               │
│                                      ┌─────────▼──────────┐    │
│                                      │  Crash Recovery     │    │
│                                      │  (Periodic sweep)   │    │
│                                      └────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Key invariant:** The Pacing Engine *proposes* calls. The Safety Controller *decides* whether they're allowed. The engine cannot bypass, disable, or reconfigure the safety layer.

## State Machines

### Agent States

```
            ┌─────────┐
      ┌────▶│ OFFLINE │◀──── (from any state)
      │     └────┬────┘
      │          │ login
      │     ┌────▼─────┐    pause    ┌────────┐
      │     │ AVAILABLE │◀──────────▶│ PAUSED │
      │     └────┬──────┘            └────────┘
      │          │ reserve
      │     ┌────▼─────┐
      │     │ RESERVED │──── call failed ──▶ AVAILABLE
      │     └────┬──────┘
      │          │ dial
      │     ┌────▼─────┐
      │     │ DIALING  │──── call failed ──▶ AVAILABLE
      │     └────┬──────┘
      │          │ answered
      │     ┌────▼──────┐
      │     │ CONNECTED │
      │     └────┬──────┘
      │          │ call ended
      │     ┌────▼──────┐
      └─────│  WRAP_UP  │──────────────────▶ AVAILABLE
            └───────────┘
```

### Call States

```
  QUEUED ──▶ RESERVED ──▶ INITIATED ──▶ RINGING ──▶ ANSWERED ──▶ CONNECTED ──▶ COMPLETED
                │              │            │           │             │
                │              │            │           │             └──▶ FAILED
                │              ├──▶ ANSWERED (skip RINGING)
                │              │            │
                └──▶ CANCELLED ├──▶ FAILED  ├──▶ FAILED
                               └──▶ COMPLETED (out-of-order ─ terminal states always accepted)
```

**Terminal states** (COMPLETED, FAILED, CANCELLED) are absorbing — further events are silently ignored.

## Concurrency Model

### How double-booking is prevented

The critical operation is "reserve an available agent." If two workers see the same agent at the same time, only one must win.

**PostgreSQL solution:** `SELECT ... FOR UPDATE SKIP LOCKED`
```sql
SELECT id FROM agents
WHERE state = 'AVAILABLE' AND campaign_id = $1
ORDER BY state_changed_at
LIMIT 1
FOR UPDATE SKIP LOCKED;
```
Worker A locks row #1; Worker B's query *skips* it and grabs row #2. Zero contention, zero blocking, zero double-booking.

**In-memory solution:** `asyncio.Lock` guards all mutations with serializable-equivalent isolation. The lock is held only during the atomic operation, not across async boundaries.

### Optimistic concurrency for state transitions

Every agent and call row carries a `version` column. State transitions use compare-and-swap:

```sql
UPDATE agents SET state = $1, version = version + 1
WHERE id = $2 AND version = $3 AND state IN (...valid sources...)
```

If `version` has changed (another worker got there first), the UPDATE affects 0 rows and the caller retries or aborts.

## Predictive Pacing Algorithm

The engine uses a **ratio-based model with EMA-smoothed feedback**:

```
desired_ringing = effective_agents × (1 / answer_rate) × safety_margin

effective_agents = AVAILABLE agents + 50% of WRAP_UP agents
new_calls = desired_ringing − calls_already_in_flight
```

**Why this approach:**
- **Simpler than Erlang-C** — we control arrival rate (not Poisson), so queuing theory adds complexity without accuracy
- **EMA smoothing** — prevents whiplash from noisy short-term answer rates
- **Ramp-up guard** — conservative until enough samples are collected
- **Abandon rate feedback** — dynamically reduces over-dialing when abandons rise
- **Provider health** — throttles when the provider is failing

The Safety Controller then applies five hard checks in order: burst cap → abandon threshold → agent buffer → unmatched ringing → provider health.

## Failure Handling

| Scenario | What happens |
|----------|-------------|
| **Worker crash** (agent RESERVED, call INITIATED) | Crash recovery sweep detects stale RESERVED agents after `max_reserve_secs`, transitions them to AVAILABLE, cancels their calls, releases borrowers |
| **Provider outage** | `place_call` returns `success=False`. Allocator rolls back agent→AVAILABLE + borrower release. Safety controller limits to 1 probe call when provider is unhealthy |
| **40 of 100 agents drop** | Next pacing tick sees 60 available agents. Progressive adjusts immediately. Predictive's snapshot shows fewer agents → fewer calls. No configuration change needed |
| **Duplicate events** | `INSERT INTO processed_events ON CONFLICT DO NOTHING`. Second/third delivery returns `False` → skipped. Demonstrated in tests with triple-delivery |
| **Out-of-order events** | Call FSM allows any non-terminal → terminal transition. COMPLETED arriving before ANSWERED is handled cleanly. ANSWERED after COMPLETED is rejected (terminal is absorbing) |

## Mock Providers

| | Provider A ("reliable") | Provider B ("unreliable") |
|---|---|---|
| Latency | 50–150 ms setup | 150–600 ms setup |
| Failure rate | 2% | 15% |
| Duplicates | Never | 20% of events (sometimes triple) |
| Reordering | Never | 15% chance of event swap |
| Timeouts | Never | 5% (call hangs forever — recovered by sweep) |
| Outages | Never | 2% chance of full outage (10s duration) |

## Simulation Scenarios

| Scenario | Answer Rate | Avg Talk Time | Notes |
|----------|-------------|---------------|-------|
| A | 20% | 120s | Low answer rate — tests aggressive over-dialing |
| B | 50% | 90s | Balanced — baseline scenario |
| C | 70% | 180s | High answer rate — tests conservative pacing |
| D | 50% → changing | 120s | 20 agents drop out at t=15s |

## Scaling Analysis

### What breaks first at 10,000 agents

1. **Campaign snapshot query** — `COUNT(*) GROUP BY state` across 10K agent rows every tick. At 1 tick/sec, this is 10K row scans per second.
   - **Fix:** Maintain in-memory counters updated by state transitions (event-sourced), or use a Redis hash map of `campaign:state → count`. The snapshot becomes O(1).

2. **Borrower reservation** — `FOR UPDATE SKIP LOCKED` works well but scanning 100K+ callable borrowers sorted by priority gets expensive.
   - **Fix:** Partition borrowers by campaign. Use a materialized priority queue (Redis sorted set or a separate `call_queue` table with pre-sorted batches).

3. **Single-database bottleneck** — All workers hit one PostgreSQL. At 10K agents with 2 calls/agent/minute, that's ~330 calls/sec, each needing 3–5 DB round-trips.
   - **Fix:** Shard by campaign_id. Each shard handles a subset of campaigns. Workers connect to the shard for their assigned campaigns.

4. **Lock contention on the events table** — High INSERT rate for idempotency checks.
   - **Fix:** Partition by event_id hash or use a bloom filter for fast duplicate rejection with DB as fallback.

5. **Pacing engine synchronization** — Multiple workers running pacing independently can overshoot in aggregate.
   - **Fix:** Elect a pacing leader per campaign (via advisory lock or lease), or use a shared atomic counter for "calls remaining this tick."

## Technology Choices

| Choice | Why | What it makes harder |
|--------|-----|---------------------|
| **Python + asyncio** | Natural fit for I/O-bound concurrent workers; excellent for prototyping | CPU-bound work (not relevant here); GIL limits multi-core (solved by multi-process workers) |
| **asyncpg** | Fastest Python PostgreSQL driver; native async support; prepared statements | Requires PostgreSQL specifically (not portable to MySQL) |
| **PostgreSQL** | SKIP LOCKED is perfect for this workload; strong ACID; excellent indexing | Heavier than Redis for simple counters; scaling requires sharding |
| **In-memory store** | Zero-dependency simulation and testing | Not production-viable; single-process only |
| **No Kafka/Redis** | Complexity not justified at this scale; PostgreSQL handles everything | At very high scale (10K+ agents), would need Redis for counters and Kafka for event fan-out |

## Project Structure

```
smart_dialer/
├── config.py               # All tunables in one place
├── models/
│   ├── agent.py            # Agent FSM + AgentRecord
│   ├── call.py             # Call FSM + CallRecord
│   └── campaign.py         # Campaign + Borrower records
├── store/
│   ├── base.py             # Store ABC
│   ├── memory_store.py     # In-memory (simulation/tests)
│   ├── pg_store.py         # PostgreSQL (production)
│   └── schema.sql          # DDL with indexes
├── engine/
│   ├── base.py             # PacingEngine ABC + CampaignSnapshot
│   ├── progressive.py      # 1:1 agent-to-call
│   └── predictive.py       # Statistical over-dialing
├── safety/
│   └── controller.py       # Hard safety boundary
├── allocator/
│   └── call_allocator.py   # Agent→Borrower→Call→Provider
├── providers/
│   ├── base.py             # TelecomProvider ABC
│   ├── reliable.py         # Provider A
│   └── unreliable.py       # Provider B (chaos)
├── webhooks/
│   └── handler.py          # Idempotent event processing
├── worker/
│   └── dialer_worker.py    # Main loop
└── recovery/
    └── crash_recovery.py   # Stale state cleanup

simulation/
└── run.py                  # Configurable simulation runner

tests/
└── test_smart_dialer.py    # 45 tests across 9 categories
```

## Final Question

> *How would you build a SmartDialer that gets as much of the utilization benefit of predictive dialing as possible, while retaining the deterministic safety characteristics of progressive dialing?*

The key insight is **separation of concerns through a structural safety boundary**.

The pacing engine is allowed to be as aggressive as it wants — it can propose over-dialing based on answer rates, agent availability projections, wrap-up predictions, and historical patterns. This is where utilization gains come from.

But between the engine and the telecom provider sits a Safety Controller that the engine **cannot bypass, reconfigure, or disable**. It's not a flag; it's a separate component with its own invariants:

1. **Never exceed the agent buffer** — always keep N agents free as a safety net
2. **Cap unmatched ringing** — at most K calls can ring without a waiting agent
3. **Abandon rate circuit breaker** — if the recent abandon rate approaches the regulatory limit, automatically fall back to progressive (1:1) mode

This architecture means you can iterate on the pacing algorithm — make it smarter, add ML, incorporate time-of-day patterns — without ever risking compliance. The worst case is always "fall back to progressive," which is safe by construction.

The result is a system that acts like predictive when conditions are good (high utilization), and automatically degrades to progressive when conditions are risky (guaranteed safety). You get 80–90% of predictive's utilization benefit with 100% of progressive's safety guarantee.
