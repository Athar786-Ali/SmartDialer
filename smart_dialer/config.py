"""
Central configuration for the SmartDialer system.

All tunables live here so they can be overridden from environment
variables, CLI flags, or test fixtures without touching business logic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL connection parameters."""
    host: str = "localhost"
    port: int = 5432
    database: str = "smartdialer"
    user: str = "dialer"
    password: str = "dialer_pass"
    min_pool_size: int = 5
    max_pool_size: int = 20
    statement_cache_size: int = 100

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class SafetyConfig:
    """Hard limits enforced by the Safety Controller."""
    # Maximum allowable abandon rate (FCC/TCPA typically requires ≤3 %)
    max_abandon_rate: float = 0.03
    # Maximum calls that can ring simultaneously without an agent
    max_unmatched_ringing: int = 5
    # Minimum agents that must remain AVAILABLE as a buffer
    min_agent_buffer: int = 2
    # If abandon rate exceeds this, fall back to progressive
    fallback_threshold: float = 0.05
    # Maximum burst size in a single pacing tick
    max_burst_size: int = 50
    # Sliding window for abandon-rate calculation (seconds)
    metrics_window_secs: int = 300


@dataclass(frozen=True)
class PacingConfig:
    """Knobs for the pacing engines."""
    # Progressive: how often the dial loop ticks (seconds)
    tick_interval: float = 1.0
    # Predictive: safety margin applied to the over-dial ratio
    safety_margin: float = 0.85
    # Predictive: exponential-moving-average decay for answer-rate
    ema_alpha: float = 0.3
    # Predictive: minimum sample size before trusting the EMA
    min_sample_size: int = 20
    # Predictive: ramp-up factor when starting a new campaign
    ramp_up_factor: float = 0.5


@dataclass(frozen=True)
class TimeoutConfig:
    """Timeouts for detecting stale / crashed state."""
    # How long an agent can sit in RESERVED before we reclaim it
    max_reserve_secs: int = 15
    # Maximum time a call can ring before we consider it failed
    max_ring_secs: int = 45
    # Maximum time an INITIATED call can wait for provider ACK
    max_initiate_secs: int = 20
    # Default wrap-up duration before auto-transitioning to AVAILABLE
    default_wrap_up_secs: int = 30
    # How often the crash-recovery sweep runs
    recovery_sweep_secs: float = 10.0


@dataclass(frozen=True)
class ProviderConfig:
    """Per-provider tuning."""
    # Circuit-breaker: failures in window to trip
    circuit_breaker_threshold: int = 5
    # Circuit-breaker: window length (seconds)
    circuit_breaker_window_secs: int = 60
    # Circuit-breaker: cooldown before half-open (seconds)
    circuit_breaker_cooldown_secs: int = 30
    # Maximum concurrent calls to a single provider
    max_concurrent_calls: int = 500


@dataclass(frozen=True)
class Config:
    """Top-level configuration container."""
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    pacing: PacingConfig = field(default_factory=PacingConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    provider: ProviderConfig = field(default_factory=ProviderConfig)

    @classmethod
    def from_env(cls) -> "Config":
        """Build config from environment variables (falls back to defaults)."""
        return cls(
            db=DatabaseConfig(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", "5432")),
                database=os.getenv("DB_NAME", "smartdialer"),
                user=os.getenv("DB_USER", "dialer"),
                password=os.getenv("DB_PASSWORD", "dialer_pass"),
            ),
            safety=SafetyConfig(
                max_abandon_rate=float(
                    os.getenv("SAFETY_MAX_ABANDON_RATE", "0.03")
                ),
            ),
        )
