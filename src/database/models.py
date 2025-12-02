from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column  # type: ignore[attr-defined]


class Base(DeclarativeBase):
    pass


class DailyStats(Base):
    __tablename__ = "daily_stats"

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    campaign_id: Mapped[str] = mapped_column(String(50), primary_key=True)

    spend: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    conversions: Mapped[int] = mapped_column(nullable=False)
    cpa: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)

    def __repr__(self) -> str:
        return (
            f"DailyStats(date={self.date}, campaign_id={self.campaign_id}, "
            f"spend={self.spend}, conversions={self.conversions}, cpa={self.cpa})"
        )
