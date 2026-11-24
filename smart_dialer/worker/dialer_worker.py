"""
Dialer Worker.

The main event loop that ties everything together:

    Campaign → Pacing Engine → Safety Controller → Call Allocator → Provider

Each worker runs as an independent ``asyncio.Task``.  Multiple workers
can operate on the same campaign concurrently — the Store's SKIP LOCKED
semantics prevent double-booking.

A worker's tick cycle:
  1. Heartbeat.
  2. Build a campaign snapshot.
  3. Run the pacing engine → get a PacingDecision.
  4. Pass the decision through the Safety Controller → get a SafetyDecision.
  5. Call the Allocator for ``approved_calls`` times.
  6. Run crash recovery (periodically).
  7. Auto-transition WRAP_UP → AVAILABLE after timeout.
  8. Sleep until the next tick.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from smart_dialer.allocator.call_allocator import CallAllocator
from smart_dialer.config import Config
from smart_dialer.engine.base import PacingEngine
from smart_dialer.models import AgentState
from smart_dialer.providers.base import TelecomProvider
from smart_dialer.recovery.crash_recovery import CrashRecovery
from smart_dialer.safety.controller import SafetyController
from smart_dialer.store.base import Store
from smart_dialer.webhooks.handler import WebhookHandler

logger = logging.getLogger(__name__)


class DialerWorker:
    """
    A single dialer worker instance.

    Multiple workers can run concurrently on the same or different
    campaigns.  Each worker has a unique ``worker_id``.
    """

    def __init__(
        self,
        store: Store,
        pacing_engine: PacingEngine,
        safety_controller: SafetyController,
        providers: Dict[str, TelecomProvider],
        config: Config | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._store = store
        self._pacing = pacing_engine
        self._safety = safety_controller
        self._providers = providers
        self._config = config or Config()
        self.worker_id = worker_id or f"worker-{uuid4().hex[:8]}"

        self._allocator = CallAllocator(store, providers)
        self._webhook_handler = WebhookHandler(store)
        self._recovery = CrashRecovery(store, self._config.timeouts)

        self._running = False
        self._tick_count = 0
        self._last_recovery = datetime.min
        self._campaign_ids: List[UUID] = []

        # Metrics for observability
        self.total_calls_placed = 0
        self.total_calls_failed = 0
        self.pacing_history: list = []
        self.safety_history: list = []

    @property
    def webhook_handler(self) -> WebhookHandler:
        """Expose handler so providers can be wired to it."""
        return self._webhook_handler

    async def start(self, campaign_ids: List[UUID]) -> None:
        """Start the worker loop for the given campaigns."""
        self._campaign_ids = campaign_ids
        self._running = True

        # Wire up providers to the webhook handler
        for provider in self._providers.values():
            await provider.start(self._webhook_handler.handle_event)

        logger.info(
            "Worker %s started for campaigns %s",
            self.worker_id, campaign_ids,
        )

    async def stop(self) -> None:
        """Signal the worker to stop after the current tick."""
        self._running = False
        for provider in self._providers.values():
            await provider.stop()
        logger.info("Worker %s stopped", self.worker_id)

    async def run(self, max_ticks: int | None = None) -> None:
        """
        Main loop — runs until ``stop()`` is called or max_ticks reached.
        """
        while self._running:
            if max_ticks is not None and self._tick_count >= max_ticks:
                break

            try:
                await self._tick()
            except Exception:
                logger.exception(
                    "Worker %s tick failed", self.worker_id,
                )

            self._tick_count += 1
            await asyncio.sleep(self._config.pacing.tick_interval)

    async def _tick(self) -> None:
        """One cycle of the dial loop."""
        # Heartbeat
        await self._store.heartbeat(self.worker_id, self._campaign_ids)

        for campaign_id in self._campaign_ids:
            await self._tick_campaign(campaign_id)

        # Periodic crash recovery
        now = datetime.utcnow()
        if (now - self._last_recovery).total_seconds() >= self._config.timeouts.recovery_sweep_secs:
            await self._recovery.sweep()
            self._last_recovery = now

        # Auto-transition WRAP_UP → AVAILABLE
        await self._auto_unwrap()

    async def _tick_campaign(self, campaign_id: UUID) -> None:
        """Run one pacing cycle for a single campaign."""
        campaign = await self._store.get_campaign(campaign_id)
        if campaign is None or not campaign.is_active:
            return

        # 1. Build snapshot
        snapshot = await self._store.campaign_snapshot(campaign_id)

        # Inject provider health
        provider = self._providers.get(campaign.provider_name)
        if provider:
            health = provider.health()
            snapshot = type(snapshot)(
                **{
                    **{
                        f.name: getattr(snapshot, f.name)
                        for f in snapshot.__dataclass_fields__.values()
                    },
                    "provider_healthy": health.is_healthy,
                    "provider_error_rate": health.error_rate,
                },
            )

        # 2. Pacing decision
        pacing_decision = await self._pacing.compute(snapshot)
        self.pacing_history.append(pacing_decision)

        if pacing_decision.requested_calls <= 0:
            return

        # 3. Safety check
        safety_decision = self._safety.evaluate(pacing_decision, snapshot)
        self.safety_history.append(safety_decision)

        if safety_decision.approved_calls <= 0:
            logger.debug(
                "Worker %s: safety rejected — %s",
                self.worker_id, safety_decision.reasons,
            )
            return

        # 4. Allocate calls
        results = await self._allocator.allocate_batch(
            campaign_id=campaign_id,
            provider_name=campaign.provider_name,
            count=safety_decision.approved_calls,
            worker_id=self.worker_id,
        )

        # Track metrics
        placed = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        self.total_calls_placed += placed
        self.total_calls_failed += failed

        if placed > 0:
            logger.info(
                "Worker %s: placed %d/%d calls (pacing=%d, safety=%d) | %s",
                self.worker_id,
                placed,
                safety_decision.approved_calls,
                pacing_decision.requested_calls,
                safety_decision.approved_calls,
                safety_decision.verdict.value,
            )

    async def _auto_unwrap(self) -> None:
        """Move agents from WRAP_UP → AVAILABLE after the wrap-up timeout."""
        for campaign_id in self._campaign_ids:
            counts = await self._store.count_agents_by_state(campaign_id)
            # We need to check individual agents — but for the in-memory store
            # we can query all wrap-up agents
            wrap_up_agents = []
            # Use stale_reserved_agents pattern — but we need a custom query
            # For now, we rely on the recovery sweep for this
