import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from src.schemas import ConversionRecord, SpendRecord
from src.services.data_loader import DataLoader


class TestDataLoader:
    """Тесты для класса DataLoader"""

    @pytest.fixture
    def temp_spend_file(self, tmp_path):
        """Создаёт временный файл с данными расходов"""

        data = [
            {"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": 37.50},
            {"date": "2025-06-04", "campaign_id": "CAMP-456", "spend": 19.90},
        ]
        file_path = tmp_path / "test_spend.json"
        with open(file_path, "w") as f:
            json.dump(data, f)
        return file_path

    @pytest.fixture
    def temp_conversion_file(self, tmp_path):
        """Создаёт временный файл с данными конверсий"""

        data = [
            {"date": "2025-06-04", "campaign_id": "CAMP-123", "conversions": 14},
            {"date": "2025-06-04", "campaign_id": "CAMP-456", "conversions": 3},
        ]
        file_path = tmp_path / "test_conversions.json"
        with open(file_path, "w") as f:
            json.dump(data, f)
        return file_path

    def test_load_spend_data(self, temp_spend_file):
        """Тест загрузки данных расходов"""

        result = DataLoader.load_spend_data(temp_spend_file)

        assert len(result) == 2
        assert all(isinstance(r, SpendRecord) for r in result)

        assert result[0].date == date(2025, 6, 4)
        assert result[0].campaign_id == "CAMP-123"
        assert result[0].spend == Decimal("37.50")

        assert result[1].campaign_id == "CAMP-456"
        assert result[1].spend == Decimal("19.90")

    def test_load_conversion_data(self, temp_conversion_file):
        """Тест загрузки данных конверсий"""

        result = DataLoader.load_conversion_data(temp_conversion_file)

        assert len(result) == 2
        assert all(isinstance(r, ConversionRecord) for r in result)

        assert result[0].date == date(2025, 6, 4)
        assert result[0].campaign_id == "CAMP-123"
        assert result[0].conversions == 14

        assert result[1].campaign_id == "CAMP-456"
        assert result[1].conversions == 3

    def test_load_spend_data_empty_file(self, tmp_path):
        """Тест загрузки пустого файла расходов"""

        file_path = tmp_path / "empty.json"
        with open(file_path, "w") as f:
            json.dump([], f)

        result = DataLoader.load_spend_data(file_path)

        assert result == []

    def test_load_spend_data_file_not_found(self):
        """Тест обработки отсутствующего файла"""

        file_path = Path("/non/existent/file.json")

        with pytest.raises(FileNotFoundError):
            DataLoader.load_spend_data(file_path)

    def test_load_spend_data_invalid_json(self, tmp_path):
        """Тест обработки невалидного JSON"""

        file_path = tmp_path / "invalid.json"
        with open(file_path, "w") as f:
            f.write("invalid json content")

        with pytest.raises(json.JSONDecodeError):
            DataLoader.load_spend_data(file_path)

    def test_load_spend_data_validation(self, tmp_path):
        """Тест валидации данных через Pydantic"""

        data = [
            {"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": -10.00},
        ]
        file_path = tmp_path / "invalid_data.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        with pytest.raises(Exception):
            DataLoader.load_spend_data(file_path)

    def test_load_conversion_data_validation(self, tmp_path):
        """Тест валидации конверсий через Pydantic"""

        data = [
            {"date": "2025-06-04", "campaign_id": "CAMP-123", "conversions": -5},
        ]
        file_path = tmp_path / "invalid_conv.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        with pytest.raises(Exception):
            DataLoader.load_conversion_data(file_path)
