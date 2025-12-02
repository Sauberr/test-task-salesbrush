from datetime import date
from decimal import Decimal

from pydantic import BaseModel


class MergedRecord(BaseModel):
    """Объединённая запись расходов и конверсий"""

    date: date
    campaign_id: str
    spend: Decimal
    conversions: int
    cpa: Decimal | None
