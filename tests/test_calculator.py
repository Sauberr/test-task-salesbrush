from datetime import date
from decimal import Decimal

from src.schemas import ConversionRecord, MergedRecord, SpendRecord
from src.services.calculator import CPACalculator


class TestCPACalculator:
    """Тесты для класса CPACalculator"""

    def test_calculate_cpa_with_conversions(self):
        """Тест расчёта CPA при наличии конверсий"""

        spend = Decimal("100.00")
        conversions = 10

        result = CPACalculator.calculate_cpa(spend, conversions)

        assert result == Decimal("10.00")
        assert isinstance(result, Decimal)

    def test_calculate_cpa_zero_conversions(self):
        """Тест расчёта CPA при нулевых конверсиях - должен вернуть None"""

        spend = Decimal("50.00")
        conversions = 0

        result = CPACalculator.calculate_cpa(spend, conversions)

        assert result is None

    def test_calculate_cpa_rounding(self):
        """Тест округления CPA до 2 знаков"""

        spend = Decimal("37.50")
        conversions = 14

        result = CPACalculator.calculate_cpa(spend, conversions)

        assert result == Decimal("2.68")

    def test_merge_data_basic(self):
        """Тест базового слияния данных по date + campaign_id"""

        spend_records = [
            SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", spend=Decimal("37.50")),
            SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-456", spend=Decimal("19.90")),
        ]

        conversion_records = [
            ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", conversions=14),
            ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-456", conversions=3),
        ]

        result = CPACalculator.merge_data(spend_records, conversion_records)

        assert len(result) == 2
        assert all(isinstance(r, MergedRecord) for r in result)

        camp_123 = next(r for r in result if r.campaign_id == "CAMP-123")
        assert camp_123.spend == Decimal("37.50")
        assert camp_123.conversions == 14
        assert camp_123.cpa == Decimal("2.68")

        camp_456 = next(r for r in result if r.campaign_id == "CAMP-456")
        assert camp_456.spend == Decimal("19.90")
        assert camp_456.conversions == 3
        assert camp_456.cpa == Decimal("6.63")

    def test_merge_data_missing_spend(self):
        """Тест слияния когда есть конверсии но нет расходов"""

        spend_records = []
        conversion_records = [
            ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", conversions=10),
        ]

        result = CPACalculator.merge_data(spend_records, conversion_records)

        assert len(result) == 1
        assert result[0].spend == Decimal("0")
        assert result[0].conversions == 10
        assert result[0].cpa == Decimal("0.00")

    def test_merge_data_missing_conversions(self):
        """Тест слияния когда есть расходы но нет конверсий"""

        spend_records = [
            SpendRecord(date=date(2025, 6, 5), campaign_id="CAMP-789", spend=Decimal("11.00")),
        ]
        conversion_records = []

        result = CPACalculator.merge_data(spend_records, conversion_records)

        assert len(result) == 1
        assert result[0].spend == Decimal("11.00")
        assert result[0].conversions == 0
        assert result[0].cpa is None

    def test_merge_data_multiple_dates(self):
        """Тест слияния данных с несколькими датами"""

        spend_records = [
            SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", spend=Decimal("37.50")),
            SpendRecord(date=date(2025, 6, 5), campaign_id="CAMP-123", spend=Decimal("42.10")),
        ]

        conversion_records = [
            ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", conversions=14),
            ConversionRecord(date=date(2025, 6, 5), campaign_id="CAMP-123", conversions=10),
        ]

        result = CPACalculator.merge_data(spend_records, conversion_records)

        assert len(result) == 2
        assert result[0].date == date(2025, 6, 4)
        assert result[1].date == date(2025, 6, 5)

    def test_merge_data_sorted_by_date_and_campaign(self):
        """Тест что результат отсортирован по date и campaign_id"""

        spend_records = [
            SpendRecord(date=date(2025, 6, 5), campaign_id="CAMP-789", spend=Decimal("11.00")),
            SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-456", spend=Decimal("19.90")),
            SpendRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", spend=Decimal("37.50")),
        ]

        conversion_records = [
            ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-123", conversions=14),
            ConversionRecord(date=date(2025, 6, 5), campaign_id="CAMP-789", conversions=5),
            ConversionRecord(date=date(2025, 6, 4), campaign_id="CAMP-456", conversions=3),
        ]

        result = CPACalculator.merge_data(spend_records, conversion_records)

        assert result[0].date == date(2025, 6, 4)
        assert result[0].campaign_id == "CAMP-123"
        assert result[1].date == date(2025, 6, 4)
        assert result[1].campaign_id == "CAMP-456"
        assert result[2].date == date(2025, 6, 5)
        assert result[2].campaign_id == "CAMP-789"

    def test_filter_by_date_range_both_dates(self):
        """Тест фильтрации по начальной и конечной дате"""

        records = [
            MergedRecord(
                date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("10"), conversions=1, cpa=Decimal("10")
            ),
            MergedRecord(
                date=date(2025, 6, 5), campaign_id="C2", spend=Decimal("20"), conversions=2, cpa=Decimal("10")
            ),
            MergedRecord(
                date=date(2025, 6, 6), campaign_id="C3", spend=Decimal("30"), conversions=3, cpa=Decimal("10")
            ),
        ]

        result = CPACalculator.filter_by_date_range(records, start_date=date(2025, 6, 4), end_date=date(2025, 6, 5))

        assert len(result) == 2
        assert result[0].date == date(2025, 6, 4)
        assert result[1].date == date(2025, 6, 5)

    def test_filter_by_date_range_only_start(self):
        """Тест фильтрации только по начальной дате"""

        records = [
            MergedRecord(
                date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("10"), conversions=1, cpa=Decimal("10")
            ),
            MergedRecord(
                date=date(2025, 6, 5), campaign_id="C2", spend=Decimal("20"), conversions=2, cpa=Decimal("10")
            ),
            MergedRecord(
                date=date(2025, 6, 6), campaign_id="C3", spend=Decimal("30"), conversions=3, cpa=Decimal("10")
            ),
        ]

        result = CPACalculator.filter_by_date_range(records, start_date=date(2025, 6, 5))

        assert len(result) == 2
        assert all(r.date >= date(2025, 6, 5) for r in result)

    def test_filter_by_date_range_only_end(self):
        """Тест фильтрации только по конечной дате"""

        records = [
            MergedRecord(
                date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("10"), conversions=1, cpa=Decimal("10")
            ),
            MergedRecord(
                date=date(2025, 6, 5), campaign_id="C2", spend=Decimal("20"), conversions=2, cpa=Decimal("10")
            ),
            MergedRecord(
                date=date(2025, 6, 6), campaign_id="C3", spend=Decimal("30"), conversions=3, cpa=Decimal("10")
            ),
        ]

        result = CPACalculator.filter_by_date_range(records, end_date=date(2025, 6, 5))

        assert len(result) == 2
        assert all(r.date <= date(2025, 6, 5) for r in result)

    def test_filter_by_date_range_no_filters(self):
        """Тест фильтрации без параметров - должен вернуть все записи"""

        records = [
            MergedRecord(
                date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("10"), conversions=1, cpa=Decimal("10")
            ),
            MergedRecord(
                date=date(2025, 6, 5), campaign_id="C2", spend=Decimal("20"), conversions=2, cpa=Decimal("10")
            ),
        ]

        result = CPACalculator.filter_by_date_range(records)

        assert len(result) == 2
        assert result == records
