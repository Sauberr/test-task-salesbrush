from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.schemas import ConversionRecord, MergedRecord, SpendRecord


class TestSpendRecord:
    """Тесты для схемы SpendRecord"""

    def test_spend_record_valid(self):
        """Тест создания валидной записи расходов"""

        record = SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", spend=37.50)

        assert record.date == date(2025, 6, 4)
        assert record.campaign_id == "CAMP-123"
        assert record.spend == Decimal("37.50")
        assert isinstance(record.spend, Decimal)

    def test_spend_record_decimal_conversion(self):
        """Тест автоматической конвертации в Decimal"""

        record = SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", spend=100)

        assert isinstance(record.spend, Decimal)
        assert record.spend == Decimal("100")

    def test_spend_record_negative_spend(self):
        """Тест валидации - spend не может быть отрицательным"""

        with pytest.raises(ValidationError):
            SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", spend=-10.00)


class TestConversionRecord:
    """Тесты для схемы ConversionRecord"""

    def test_conversion_record_valid(self):
        """Тест создания валидной записи конверсий"""

        record = ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", conversions=14)

        assert record.date == date(2025, 6, 4)
        assert record.campaign_id == "CAMP-123"
        assert record.conversions == 14

    def test_conversion_record_zero_conversions(self):
        """Тест что conversions может быть 0"""

        record = ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", conversions=0)

        assert record.conversions == 0

    def test_conversion_record_negative_conversions(self):
        """Тест валидации - conversions не может быть отрицательным"""

        with pytest.raises(ValidationError):
            ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", conversions=-5)


class TestMergedRecord:
    """Тесты для схемы MergedRecord"""

    def test_merged_record_with_cpa(self):
        """Тест создания объединённой записи с CPA"""

        record = MergedRecord(
            date=date(2025, 6, 4), campaign_id="CAMP-123", spend=Decimal("37.50"), conversions=14, cpa=Decimal("2.68")
        )

        assert record.date == date(2025, 6, 4)
        assert record.campaign_id == "CAMP-123"
        assert record.spend == Decimal("37.50")
        assert record.conversions == 14
        assert record.cpa == Decimal("2.68")

    def test_merged_record_without_cpa(self):
        """Тест создания объединённой записи без CPA (None)"""

        record = MergedRecord(
            date=date(2025, 6, 5), campaign_id="CAMP-789", spend=Decimal("11.00"), conversions=0, cpa=None
        )

        assert record.conversions == 0
        assert record.cpa is None
