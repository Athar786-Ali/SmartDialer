"""Public re-exports for the store package."""
from .base import Store
from .memory_store import InMemoryStore

# PgStore requires asyncpg — import lazily so the in-memory path
# (simulation / tests) works without it.
def __getattr__(name: str):
    if name == "PgStore":
        from .pg_store import PgStore
        return PgStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ["Store", "InMemoryStore", "PgStore"]
