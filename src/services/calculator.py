from datetime import date
from decimal import Decimal

from src.schemas import ConversionRecord, MergedRecord, SpendRecord


class CPACalculator:
    """Класс для расчёта CPA и объединения данных"""

    @staticmethod
    def calculate_cpa(spend: Decimal, conversions: int) -> Decimal | None:
        """
        Расчёт CPA (Cost Per Acquisition).
        """

        if conversions == 0:
            return None

        cpa = spend / Decimal(conversions)
        return cpa.quantize(Decimal("0.01"))

    @staticmethod
    def merge_data(
        spend_records: list[SpendRecord],
        conversion_records: list[ConversionRecord],
    ) -> list[MergedRecord]:
        """
        Объединение данных по date + campaign_id и расчёт CPA.
        """

        spend_dict: dict[tuple[date, str], Decimal] = {
            (record.date, record.campaign_id): record.spend for record in spend_records
        }

        conversion_dict: dict[tuple[date, str], int] = {
            (record.date, record.campaign_id): record.conversions for record in conversion_records
        }

        all_keys = set(spend_dict.keys()) | set(conversion_dict.keys())

        merged_records: list[MergedRecord] = []

        for key in all_keys:
            record_date, campaign_id = key
            spend = spend_dict.get(key, Decimal("0"))
            conversions = conversion_dict.get(key, 0)

            cpa = CPACalculator.calculate_cpa(spend, conversions)

            merged_records.append(
                MergedRecord(
                    date=record_date,
                    campaign_id=campaign_id,
                    spend=spend,
                    conversions=conversions,
                    cpa=cpa,
                )
            )

        return sorted(merged_records, key=lambda x: (x.date, x.campaign_id))

    @staticmethod
    def filter_by_date_range(
        records: list[MergedRecord],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MergedRecord]:
        """
        Фильтрация записей по диапазону дат.
        """

        filtered = records

        if start_date:
            filtered = [r for r in filtered if r.date >= start_date]

        if end_date:
            filtered = [r for r in filtered if r.date <= end_date]

        return filtered
