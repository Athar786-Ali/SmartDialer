"""
Provider B — Unreliable (Flaky).

Simulates a problematic telecom provider:
  - Slower call setup (500 ms – 2 s)
  - Occasional timeouts (call hangs without events)
  - **Duplicate events** (same event sent 2–3 times)
  - **Out-of-order events** (COMPLETED before ANSWERED, etc.)
  - Higher failure rate
  - Occasional complete outage periods

This provider is designed to stress-test the dialer's resilience.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from smart_dialer.providers.base import (
    EventCallback,
    PlaceCallResult,
    ProviderEvent,
    ProviderEventType,
    ProviderHealth,
    TelecomProvider,
)


class UnreliableProvider(TelecomProvider):
    """Flaky mock telecom provider — duplicates, reorders, and drops events."""

    def __init__(
        self,
        answer_rate: float = 0.40,
        avg_ring_secs: float = 6.0,
        avg_talk_secs: float = 90.0,
        failure_rate: float = 0.15,
        duplicate_rate: float = 0.20,
        reorder_rate: float = 0.15,
        timeout_rate: float = 0.05,
        outage_probability: float = 0.02,
        outage_duration_secs: float = 10.0,
    ) -> None:
        self._answer_rate = answer_rate
        self._avg_ring_secs = avg_ring_secs
        self._avg_talk_secs = avg_talk_secs
        self._failure_rate = failure_rate
        self._duplicate_rate = duplicate_rate
        self._reorder_rate = reorder_rate
        self._timeout_rate = timeout_rate
        self._outage_probability = outage_probability
        self._outage_duration_secs = outage_duration_secs
        self._callback: Optional[EventCallback] = None
        self._active_calls: Dict[str, asyncio.Task] = {}
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0
        self._is_in_outage = False
        self._last_failure_at: Optional[datetime] = None
        self._running = False

    @property
    def name(self) -> str:
        return "unreliable"

    async def start(self, callback: EventCallback) -> None:
        self._callback = callback
        self._running = True

    async def stop(self) -> None:
        self._running = False
        for task in self._active_calls.values():
            task.cancel()
        self._active_calls.clear()

    async def place_call(
        self, call_id: UUID, phone_number: str,
    ) -> PlaceCallResult:
        if not self._running:
            return PlaceCallResult(success=False, error="Provider stopped")

        # Simulate outage
        if self._is_in_outage:
            self._total_failures += 1
            self._last_failure_at = datetime.utcnow()
            return PlaceCallResult(success=False, error="Provider outage")

        # Random outage trigger
        if random.random() < self._outage_probability:
            self._is_in_outage = True
            asyncio.create_task(self._end_outage())
            self._total_failures += 1
            self._last_failure_at = datetime.utcnow()
            return PlaceCallResult(success=False, error="Provider outage")

        provider_call_id = f"flk-{uuid4().hex[:12]}"
        self._total_calls += 1

        # Slower setup latency (500ms – 2s)
        await asyncio.sleep(random.uniform(0.15, 0.6))

        # Timeout: call gets stuck, no events ever arrive
        if random.random() < self._timeout_rate:
            self._total_failures += 1
            self._last_failure_at = datetime.utcnow()
            # We return success but never send events — the dialer's
            # crash recovery must detect and clean this up
            return PlaceCallResult(
                success=True,
                provider_call_id=provider_call_id,
            )

        task = asyncio.create_task(
            self._simulate_call(call_id, provider_call_id)
        )
        self._active_calls[provider_call_id] = task

        return PlaceCallResult(
            success=True,
            provider_call_id=provider_call_id,
        )

    async def cancel_call(self, provider_call_id: str) -> bool:
        task = self._active_calls.pop(provider_call_id, None)
        if task and not task.done():
            task.cancel()
            return True
        return False

    def health(self) -> ProviderHealth:
        total = self._total_successes + self._total_failures
        return ProviderHealth(
            name=self.name,
            is_healthy=not self._is_in_outage,
            active_calls=len(self._active_calls),
            recent_failures=self._total_failures,
            recent_successes=self._total_successes,
            error_rate=(
                self._total_failures / total if total > 0 else 0.0
            ),
            last_failure_at=self._last_failure_at,
        )

    async def _end_outage(self) -> None:
        await asyncio.sleep(self._outage_duration_secs)
        self._is_in_outage = False

    async def _emit(
        self,
        call_id: UUID,
        provider_call_id: str,
        event_type: ProviderEventType,
        **meta,
    ) -> None:
        """Emit an event, possibly duplicating it."""
        assert self._callback is not None
        event = ProviderEvent.make(
            self.name, provider_call_id, call_id, event_type, **meta,
        )
        await self._callback(event)

        # Duplicate event?
        if random.random() < self._duplicate_rate:
            # Send the SAME event again (same event_id = idempotency test)
            await asyncio.sleep(random.uniform(0.01, 0.1))
            await self._callback(event)

            # Sometimes triple
            if random.random() < 0.3:
                await asyncio.sleep(random.uniform(0.01, 0.05))
                await self._callback(event)

    async def _simulate_call(
        self, call_id: UUID, provider_call_id: str,
    ) -> None:
        """Simulate the full call lifecycle with chaos."""
        try:
            assert self._callback is not None

            # Quick failure?
            if random.random() < self._failure_rate:
                self._total_failures += 1
                self._last_failure_at = datetime.utcnow()
                await asyncio.sleep(random.uniform(0.2, 0.8))
                await self._emit(
                    call_id, provider_call_id,
                    ProviderEventType.FAILED, reason="provider_error",
                )
                return

            # Build the event sequence
            events: List[tuple] = []  # (delay, event_type, meta)

            # RINGING
            events.append((
                random.uniform(0.3, 1.0),
                ProviderEventType.RINGING,
                {},
            ))

            ring_time = random.expovariate(1.0 / self._avg_ring_secs)
            ring_time = min(ring_time, 30.0)

            if random.random() < self._answer_rate:
                # ANSWERED
                events.append((
                    ring_time,
                    ProviderEventType.ANSWERED,
                    {},
                ))
                self._total_successes += 1

                talk_time = random.expovariate(1.0 / self._avg_talk_secs)
                min_talk = min(1.0, self._avg_talk_secs * 0.1)
                talk_time = max(min_talk, min(talk_time, 600.0))

                # COMPLETED
                events.append((
                    talk_time,
                    ProviderEventType.COMPLETED,
                    {},
                ))
            else:
                # No answer
                self._total_failures += 1
                self._last_failure_at = datetime.utcnow()
                events.append((
                    ring_time,
                    random.choice([
                        ProviderEventType.NO_ANSWER,
                        ProviderEventType.BUSY,
                        ProviderEventType.FAILED,
                    ]),
                    {},
                ))

            # Should we reorder events?
            if random.random() < self._reorder_rate and len(events) >= 2:
                # Swap last two events (e.g., COMPLETED before ANSWERED)
                # but keep delays roughly the same
                i, j = len(events) - 2, len(events) - 1
                # Swap event types but keep delays in original order
                e_i = events[i]
                e_j = events[j]
                events[i] = (e_i[0], e_j[1], e_j[2])
                events[j] = (e_j[0], e_i[1], e_i[2])

            # Emit events with delays
            for delay, event_type, meta in events:
                await asyncio.sleep(delay)
                await self._emit(
                    call_id, provider_call_id, event_type, **meta,
                )

        except asyncio.CancelledError:
            if self._callback:
                await self._emit(
                    call_id, provider_call_id,
                    ProviderEventType.COMPLETED, reason="cancelled",
                )
        finally:
            self._active_calls.pop(provider_call_id, None)
