from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator


class SpendRecord(BaseModel):
    """Модель записи расходов из fb_spend.json"""

    date: date
    campaign_id: str
    spend: Decimal = Field(ge=0)

    @field_validator("spend", mode="before")
    @classmethod
    def convert_to_decimal(cls, v: Any) -> Decimal:
        """Конвертация числа в Decimal"""

        return Decimal(str(v))
