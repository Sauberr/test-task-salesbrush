import json
from pathlib import Path

from src.schemas import ConversionRecord, SpendRecord


class DataLoader:
    """Класс для загрузки и валидации данных из JSON файлов"""

    @staticmethod
    def load_spend_data(file_path: Path) -> list[SpendRecord]:
        """
        Загрузка данных o расходах из JSON файла.
        """

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        return [SpendRecord(**record) for record in data]

    @staticmethod
    def load_conversion_data(file_path: Path) -> list[ConversionRecord]:
        """
        Загрузка данных o конверсиях из JSON файла.
        """

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        return [ConversionRecord(**record) for record in data]
