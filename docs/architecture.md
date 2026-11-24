# Architecture Decision Document

## 1. System Overview

SmartDialer is a campaign-based outbound dialer for collections. It supports N concurrent workers operating on the same campaigns, with strong guarantees against double-booking agents and abandoned calls.

### Design Principles

1. **Correctness over cleverness** — explicit state machines, atomic operations, idempotent event processing
2. **Structural safety** — the pacing engine cannot bypass the safety controller through any code path
3. **Fail-safe defaults** — unknown states, stale data, and crashed workers always resolve to safe states
4. **Simplicity** — no technology that doesn't solve a specific problem

---

## 2. Key Decisions

### 2.1 PostgreSQL with FOR UPDATE SKIP LOCKED

**Problem:** Multiple workers must reserve agents and borrowers without double-booking.

**Decision:** Use PostgreSQL's `FOR UPDATE SKIP LOCKED` for contention-free reservation.

**Why:** This is the optimal primitive for exactly this workload:
- Worker A locks row #1; Worker B's query *skips* it and takes row #2
- No blocking, no retries, no deadlocks
- Atomic within a transaction (SELECT + UPDATE in one round-trip)

**Alternatives considered:**
- **Redis distributed locks (SETNX/Redlock):** Adds infrastructure complexity. Redlock has known theoretical issues. We already need PostgreSQL for persistent state, so using it for locking avoids a second system.
- **Optimistic locking only (version-based retry):** Works but causes wasted work under high contention. With 10 workers and 50 agents, every tick would have ~20% retry rate.
- **Application-level partitioning:** Assign agent ranges to workers statically. Simple but inflexible — can't handle worker failures or dynamic load balancing.

**Trade-off:** Ties us to PostgreSQL specifically. MySQL's `SKIP LOCKED` has different semantics. Acceptable for this use case.

### 2.2 Separation of Pacing Engine and Safety Controller

**Problem:** Predictive dialing improves utilization but risks abandoned calls (compliance violation).

**Decision:** Split into two components with a one-way interface:
- Pacing engine outputs a `PacingDecision` (advisory)
- Safety controller outputs a `SafetyDecision` (binding)
- The pacing engine has no reference to the safety controller and cannot influence its decisions

**Why:** This is the core architectural decision. If the safety check is a flag (`if safety_enabled:`), eventually someone adds `--force` or `--admin-override`. By making safety a separate component that the pacing engine doesn't even import, bypass requires changing the architecture, not just a configuration.

**The fallback mechanism is key:** When conditions deteriorate, the safety controller doesn't just reduce — it falls back to progressive mode (1:1). This means the worst case is always "one call per agent," which is safe by construction.

### 2.3 In-Memory Store for Simulation

**Problem:** The system should be easy to run locally without infrastructure.

**Decision:** Define a `Store` ABC with two implementations:
- `PgStore` — production PostgreSQL with asyncpg
- `InMemoryStore` — zero-dependency, asyncio-based

**Why:** Reviewers can run `python -m simulation.run` immediately. Tests run in <1 second without Docker. The PgStore exists for production correctness; the InMemoryStore exists for developer experience.

**Trade-off:** The InMemoryStore uses a global `asyncio.Lock` which is *stronger* than SKIP LOCKED (serializable vs. row-level). This means simulation can't expose contention patterns that only appear with true row-level locking. Acceptable for a prototype.

### 2.4 Idempotent Event Processing

**Problem:** Telecom providers send duplicate webhooks. The same ANSWERED event might arrive 2-3 times.

**Decision:** Every event carries a UUID `event_id`. Before processing:
```sql
INSERT INTO processed_events (event_id, call_id, event_type)
VALUES ($1, $2, $3)
ON CONFLICT (event_id) DO NOTHING
```
If the INSERT affects 0 rows, it's a duplicate — skip processing.

**Why:** This is the standard at-most-once processing pattern. It's simple, correct, and works at scale. The `processed_events` table can be partitioned by time and old entries can be pruned.

**Alternative considered:** Rely on state machine rejection (e.g., RINGING→RINGING is a no-op). This works for same-state duplicates but fails for reprocessed events that caused side-effects (e.g., agent state transition). The event table is the safer choice.

### 2.5 Explicit State Machines with Whitelist Transitions

**Problem:** External systems send events in unexpected order. Workers crash mid-transition.

**Decision:** Define valid transitions as explicit `frozenset` of `(from_state, to_state)` pairs. Any transition not in the set is rejected. Terminal states are absorbing.

**Why:** An implicit state machine (switch/if-else) can silently accept invalid transitions. An explicit whitelist fails closed — if we forgot to add a transition, we get a rejection (safe) rather than data corruption (unsafe).

**Special rule:** Any non-terminal state can transition to a terminal state (COMPLETED, FAILED, CANCELLED). This handles out-of-order events gracefully: if COMPLETED arrives before ANSWERED, we skip straight to COMPLETED.

### 2.6 No Kafka, No Redis, No Microservices

**Problem:** The system needs to handle concurrent workers.

**Decision:** Use PostgreSQL as the single source of truth for all coordination. Workers are asyncio tasks (or processes) that connect to the same database.

**Why:** At the scale of this prototype (up to ~1,000 agents), PostgreSQL handles everything:
- Agent/borrower reservation → SKIP LOCKED
- Event deduplication → INSERT ON CONFLICT
- Crash recovery → periodic SELECT for stale state
- Metrics → aggregate queries on the calls table

Adding Kafka for event streaming would require:
- A Kafka cluster (3+ brokers)
- Consumer group management
- Exactly-once processing semantics (complex)
- Schema management for events
- Operational overhead (partition rebalancing, lag monitoring)

None of this is justified until we exceed PostgreSQL's throughput (~5,000-10,000 transactions/second on commodity hardware).

**When to add Redis:** Counter-based metrics (available agent count, calls-in-flight) that are queried every tick. At 10K agents, `SELECT COUNT(*) GROUP BY state` becomes expensive. A Redis hash map with atomic increments would eliminate this bottleneck.

**When to add Kafka:** Event fan-out (one event → multiple consumers: metrics, analytics, audit log, ML training). Also useful for decoupling webhook ingestion from processing.

---

## 3. Predictive Pacing: Why This Algorithm

The pacing algorithm uses a ratio-based model, not Erlang-C.

**Erlang-C** assumes:
- Poisson arrival process (random arrivals)
- Exponential service times
- No abandonment

None of these hold for outbound dialing:
- We *control* the arrival rate (it's not random)
- Talk times are not exponential (they're campaign-dependent)
- Abandonment is the thing we're trying to prevent

**Our model** directly calculates:
```
calls_to_place = available_agents / answer_rate × safety_margin − calls_in_flight
```

With feedback loops:
1. **EMA-smoothed answer rate** — α=0.3 provides responsive-but-stable estimates
2. **Abandon rate pullback** — linearly reduces over-dialing as abandons approach threshold
3. **Ramp-up guard** — conservative until min_sample_size calls are observed
4. **Provider health** — 0.3× throttle during outages

The Safety Controller then applies hard caps. The result is a system that adapts quickly to changing conditions while maintaining strict safety bounds.

---

## 4. What I'd Do Differently With Another Week

1. **Campaign-level pacing leader election** — Use PostgreSQL advisory locks to elect one pacing leader per campaign. Other workers only allocate, reducing snapshot query load.

2. **Real-time metrics dashboard** — WebSocket-based dashboard showing agent states, call flow, pacing decisions, and safety verdicts in real time.

3. **Call quality scoring** — Track per-borrower answer patterns to prioritize likely-to-answer borrowers, improving both utilization and customer experience.

4. **Graceful degradation tiers** — Instead of binary "predictive or progressive," define multiple degradation levels based on observed conditions.

5. **Property-based testing** — Use Hypothesis to generate random event sequences and verify that the system always reaches a consistent state.

6. **Actual PostgreSQL integration tests** — Use testcontainers to run the full PgStore test suite against a real database in CI.
