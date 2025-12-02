from datetime import date

from src.database.db import Database
from src.schemas import MergedRecord
from src.services.calculator import CPACalculator
from src.services.data_loader import DataLoader
from src.settings.api import api_config


class ETLService:
    """Сервис для ETL процесса: Extract, Transform, Load"""

    def __init__(self, database: Database) -> None:
        """
        Инициализация ETL сервиса.
        """

        self.database = database
        self.data_loader = DataLoader()
        self.calculator = CPACalculator()

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MergedRecord]:
        """
        Запуск полного ETL процесса.
        """

        spend_records = self.data_loader.load_spend_data(api_config.fb_spend_path)
        conversion_records = self.data_loader.load_conversion_data(api_config.network_conv_path)

        merged_records = self.calculator.merge_data(spend_records, conversion_records)

        if start_date or end_date:
            merged_records = self.calculator.filter_by_date_range(merged_records, start_date, end_date)

        self._save_to_database(merged_records)

        return merged_records

    def _save_to_database(self, records: list[MergedRecord]) -> None:
        """
        Сохранение записей в базу данных (bulk upsert).
        """

        if not records:
            return

        stats_list = [
            {
                "date": record.date,
                "campaign_id": record.campaign_id,
                "spend": record.spend,
                "conversions": record.conversions,
                "cpa": record.cpa,
            }
            for record in records
        ]

        self.database.bulk_upsert_stats(stats_list)

    def print_summary(self, records: list[MergedRecord]) -> None:
        """
        Вывод краткого резюме в консоль.
        """

        if not records:
            print("📭 Нет данных для отображения")
            return

        total_spend = sum(r.spend for r in records)
        total_conversions = sum(r.conversions for r in records)
        records_with_cpa = [r for r in records if r.cpa is not None]

        avg_cpa_line = ""
        if records_with_cpa:
            avg_cpa = sum(r.cpa for r in records_with_cpa if r.cpa is not None) / len(records_with_cpa)
            avg_cpa_line = f"\n💵 Средний CPA: ${avg_cpa:,.2f}"

        print(
            f"\n{'=' * 80}\n"
            f"📊 РЕЗЮМЕ ОБРАБОТКИ ДАННЫХ\n"
            f"{'=' * 80}\n\n"
            f"✅ Обработано записей: {len(records)}\n"
            f"💰 Общие расходы: ${total_spend:,.2f}\n"
            f"🎯 Общие конверсии: {total_conversions}\n"
            f"📈 Записей c CPA: {len(records_with_cpa)}"
            f"{avg_cpa_line}\n\n"
            f"{'-' * 80}\n"
            f"{'Дата':<12} {'Campaign ID':<15} {'Spend':<12} {'Conv':<8} {'CPA':<12}\n"
            f"{'-' * 80}"
        )

        for record in records:
            cpa_str = f"${record.cpa:,.2f}" if record.cpa else "N/A"
            print(
                f"{record.date} {record.campaign_id:<15} ${record.spend:<11,.2f} {record.conversions:<8} {cpa_str:<12}"
            )

        print(f"{'=' * 80}\n")
