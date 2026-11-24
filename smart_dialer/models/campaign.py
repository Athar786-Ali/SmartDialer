"""
Campaign and borrower models.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4


@dataclass
class CampaignRecord:
    """Snapshot of a campaign row."""
    id: UUID
    name: str
    mode: str = "PROGRESSIVE"           # PROGRESSIVE | PREDICTIVE
    provider_name: str = "reliable"
    max_abandon_rate: float = 0.03
    target_answer_rate: float = 0.50
    avg_talk_time_secs: int = 120
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)

    @staticmethod
    def new(
        name: str,
        mode: str = "PROGRESSIVE",
        provider_name: str = "reliable",
        target_answer_rate: float = 0.50,
        avg_talk_time_secs: int = 120,
    ) -> "CampaignRecord":
        return CampaignRecord(
            id=uuid4(),
            name=name,
            mode=mode,
            provider_name=provider_name,
            target_answer_rate=target_answer_rate,
            avg_talk_time_secs=avg_talk_time_secs,
        )


@dataclass
class BorrowerRecord:
    """Snapshot of a borrower row."""
    id: UUID
    campaign_id: UUID
    name: str
    phone: str
    priority: int = 0
    last_called_at: Optional[datetime] = None
    call_attempts: int = 0
    max_attempts: int = 5
    is_callable: bool = True
    locked_by: Optional[str] = None
    locked_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

    @staticmethod
    def new(
        campaign_id: UUID,
        name: str,
        phone: str,
        priority: int = 0,
    ) -> "BorrowerRecord":
        return BorrowerRecord(
            id=uuid4(),
            campaign_id=campaign_id,
            name=name,
            phone=phone,
            priority=priority,
        )
