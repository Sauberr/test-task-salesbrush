from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.database import Database
from src.schemas import ConversionRecord, MergedRecord, SpendRecord
from src.services import ETLService


class TestETLService:
    """Тесты для ETLService"""

    @pytest.fixture
    def mock_database(self):
        """Мок базы данных"""

        return MagicMock(spec=Database)

    @pytest.fixture
    def etl_service(self, mock_database):
        """Создаёт ETLService с моком БД"""

        return ETLService(database=mock_database)

    @patch("src.services.data_loader.DataLoader.load_conversion_data")
    @patch("src.services.data_loader.DataLoader.load_spend_data")
    def test_run_without_dates(self, mock_load_spend, mock_load_conv, etl_service, mock_database):
        """Тест запуска ETL без фильтрации по датам"""

        mock_load_spend.return_value = [
            SpendRecord(date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("100")),
        ]
        mock_load_conv.return_value = [
            ConversionRecord(date=date(2025, 6, 4), campaign_id="C1", conversions=10),
        ]

        results = etl_service.run()

        assert len(results) > 0
        mock_database.bulk_upsert_stats.assert_called_once()

    @patch("src.services.data_loader.DataLoader.load_conversion_data")
    @patch("src.services.data_loader.DataLoader.load_spend_data")
    def test_run_with_dates(self, mock_load_spend, mock_load_conv, etl_service, mock_database):
        """Тест запуска ETL с фильтрацией по датам"""

        mock_load_spend.return_value = [
            SpendRecord(date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("100")),
            SpendRecord(date=date(2025, 6, 6), campaign_id="C2", spend=Decimal("200")),
        ]
        mock_load_conv.return_value = [
            ConversionRecord(date=date(2025, 6, 4), campaign_id="C1", conversions=10),
            ConversionRecord(date=date(2025, 6, 6), campaign_id="C2", conversions=20),
        ]

        results = etl_service.run(start_date=date(2025, 6, 4), end_date=date(2025, 6, 5))

        assert len(results) == 1
        assert results[0].date == date(2025, 6, 4)

    def test_save_to_database_with_records(self, etl_service, mock_database):
        """Тест сохранения записей в БД"""

        records = [
            MergedRecord(
                date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("100"), conversions=10, cpa=Decimal("10")
            ),
        ]

        etl_service._save_to_database(records)

        mock_database.bulk_upsert_stats.assert_called_once()
        call_args = mock_database.bulk_upsert_stats.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0]["campaign_id"] == "C1"

    def test_save_to_database_empty_records(self, etl_service, mock_database):
        """Тест сохранения пустого списка"""

        etl_service._save_to_database([])

        mock_database.bulk_upsert_stats.assert_not_called()

    def test_print_summary_with_records(self, etl_service, capsys):
        """Тест вывода резюме с данными"""

        records = [
            MergedRecord(
                date=date(2025, 6, 4), campaign_id="C1", spend=Decimal("100"), conversions=10, cpa=Decimal("10")
            ),
        ]

        etl_service.print_summary(records)

        captured = capsys.readouterr()
        assert "РЕЗЮМЕ ОБРАБОТКИ ДАННЫХ" in captured.out
        assert "Обработано записей: 1" in captured.out
        assert "$100.00" in captured.out

    def test_print_summary_empty_records(self, etl_service, capsys):
        """Тест вывода резюме без данных"""

        etl_service.print_summary([])

        captured = capsys.readouterr()
        assert "Нет данных для отображения" in captured.out
