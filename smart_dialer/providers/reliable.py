"""
Provider A — Reliable.

Simulates a high-quality telecom provider:
  - Fast call setup (200–500 ms)
  - High answer rate (~configurable)
  - Clean event ordering (RINGING → ANSWERED → COMPLETED)
  - No duplicate events
  - Very low failure rate
"""

from __future__ import annotations

import asyncio
import random
from typing import Dict, Optional
from uuid import UUID, uuid4

from smart_dialer.providers.base import (
    EventCallback,
    PlaceCallResult,
    ProviderEvent,
    ProviderEventType,
    ProviderHealth,
    TelecomProvider,
)


class ReliableProvider(TelecomProvider):
    """Fast, reliable mock telecom provider."""

    def __init__(
        self,
        answer_rate: float = 0.50,
        avg_ring_secs: float = 3.0,
        avg_talk_secs: float = 120.0,
        failure_rate: float = 0.02,
    ) -> None:
        self._answer_rate = answer_rate
        self._avg_ring_secs = avg_ring_secs
        self._avg_talk_secs = avg_talk_secs
        self._failure_rate = failure_rate
        self._callback: Optional[EventCallback] = None
        self._active_calls: Dict[str, asyncio.Task] = {}
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0
        self._running = False

    @property
    def name(self) -> str:
        return "reliable"

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

        provider_call_id = f"rel-{uuid4().hex[:12]}"
        self._total_calls += 1

        # Simulate small setup latency
        await asyncio.sleep(random.uniform(0.05, 0.15))

        # Schedule the call lifecycle as a background task
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
            is_healthy=True,
            active_calls=len(self._active_calls),
            recent_failures=self._total_failures,
            recent_successes=self._total_successes,
            error_rate=(
                self._total_failures / total if total > 0 else 0.0
            ),
        )

    async def _simulate_call(
        self, call_id: UUID, provider_call_id: str,
    ) -> None:
        """Simulate the full call lifecycle with realistic timing."""
        try:
            assert self._callback is not None

            # Quick provider-side failure?
            if random.random() < self._failure_rate:
                self._total_failures += 1
                await asyncio.sleep(random.uniform(0.1, 0.3))
                await self._callback(ProviderEvent.make(
                    self.name, provider_call_id, call_id,
                    ProviderEventType.FAILED,
                    reason="provider_error",
                ))
                return

            # RINGING
            await asyncio.sleep(random.uniform(0.2, 0.5))
            await self._callback(ProviderEvent.make(
                self.name, provider_call_id, call_id,
                ProviderEventType.RINGING,
            ))

            # Wait for answer
            ring_time = random.expovariate(1.0 / self._avg_ring_secs)
            ring_time = min(ring_time, 30.0)  # cap at 30s
            await asyncio.sleep(ring_time)

            # Did borrower answer?
            if random.random() < self._answer_rate:
                # ANSWERED
                await self._callback(ProviderEvent.make(
                    self.name, provider_call_id, call_id,
                    ProviderEventType.ANSWERED,
                ))
                self._total_successes += 1

                # Talk time
                talk_time = random.expovariate(1.0 / self._avg_talk_secs)
                min_talk = min(1.0, self._avg_talk_secs * 0.1)
                talk_time = max(min_talk, min(talk_time, 600.0))
                await asyncio.sleep(talk_time)

                # COMPLETED
                await self._callback(ProviderEvent.make(
                    self.name, provider_call_id, call_id,
                    ProviderEventType.COMPLETED,
                ))
            else:
                # No answer / busy
                event_type = random.choice([
                    ProviderEventType.NO_ANSWER,
                    ProviderEventType.BUSY,
                    ProviderEventType.FAILED,
                ])
                self._total_failures += 1
                await self._callback(ProviderEvent.make(
                    self.name, provider_call_id, call_id,
                    event_type,
                ))
        except asyncio.CancelledError:
            # Call was cancelled — send COMPLETED
            if self._callback:
                await self._callback(ProviderEvent.make(
                    self.name, provider_call_id, call_id,
                    ProviderEventType.COMPLETED,
                    reason="cancelled",
                ))
        finally:
            self._active_calls.pop(provider_call_id, None)
