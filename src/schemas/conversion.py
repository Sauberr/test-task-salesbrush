from datetime import date

from pydantic import BaseModel, Field


class ConversionRecord(BaseModel):
    """Модель записи конверсий из network_conv.json"""

    date: date
    campaign_id: str
    conversions: int = Field(ge=0)
