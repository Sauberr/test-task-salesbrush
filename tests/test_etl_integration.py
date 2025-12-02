from datetime import date

import pytest
from sqlalchemy import func, select

from src.database import Database
from src.database.models import DailyStats
from src.schemas import MergedRecord
from src.services import ETLService


class TestETLIntegration:
    """Интеграционные тесты для ETL процесса"""

    @pytest.fixture
    def database(self):
        """Создаёт тестовую базу данных"""
        db = Database()
        db.init_db()
        yield db
        db.close()

    def test_etl_full_process(self, database):
        """Тест полного ETL процесса end-to-end"""

        etl = ETLService(database=database)

        results = etl.run()

        assert len(results) > 0
        assert all(isinstance(r, MergedRecord) for r in results)

        with database.get_session() as session:
            count = session.execute(select(func.count()).select_from(DailyStats)).scalar()
            assert count == len(results)

    def test_etl_with_date_filter(self, database):
        """Тест ETL с фильтрацией по датам"""

        etl = ETLService(database=database)
        start_date = date(2025, 6, 4)
        end_date = date(2025, 6, 5)

        results = etl.run(start_date=start_date, end_date=end_date)

        assert all(start_date <= r.date <= end_date for r in results)

    def test_etl_upsert(self, database):
        """Тест UPSERT - повторная загрузка не создаёт дубликаты"""

        etl = ETLService(database=database)

        results1 = etl.run()
        count1 = len(results1)

        results2 = etl.run()
        count2 = len(results2)

        assert count1 == count2

        with database.get_session() as session:
            total = session.execute(select(func.count()).select_from(DailyStats)).scalar()
            assert total == count1
