from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.database.models import Base, DailyStats
from src.settings.database import db_config


class Database:
    """Класс для работы c базой данных"""

    def __init__(self) -> None:
        """Инициализация подключения к БД"""

        self.engine: Engine = create_engine(
            db_config.database_url,
            echo=False,
            pool_pre_ping=True,
        )
        self._session_factory = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

    def init_db(self) -> None:
        """Создание всех таблиц в базе данных"""
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager для работы c сессией БД.
        """

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_stats(
        self,
        date: date,
        campaign_id: str,
        spend: Decimal,
        conversions: int,
        cpa: Decimal | None,
    ) -> None:
        """
        Upsert (INSERT or UPDATE) статистики в таблицу daily_stats.
        """

        with self.get_session() as session:
            stmt = pg_insert(DailyStats).values(
                date=date,
                campaign_id=campaign_id,
                spend=spend,
                conversions=conversions,
                cpa=cpa,
            )

            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "campaign_id"],
                set_={
                    "spend": stmt.excluded.spend,
                    "conversions": stmt.excluded.conversions,
                    "cpa": stmt.excluded.cpa,
                },
            )

            session.execute(stmt)

    def bulk_upsert_stats(self, stats_list: list[dict[str, Any]]) -> None:
        """
        Массовый upsert статистики.
        """

        if not stats_list:
            return

        with self.get_session() as session:
            stmt = pg_insert(DailyStats).values(stats_list)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "campaign_id"],
                set_={
                    "spend": stmt.excluded.spend,
                    "conversions": stmt.excluded.conversions,
                    "cpa": stmt.excluded.cpa,
                },
            )
            session.execute(stmt)

    def get_stats_by_date_range(self, start_date: date | None, end_date: date | None) -> list[DailyStats]:
        """
        Получить статистику за период.
        """

        with self.get_session() as session:
            query = select(DailyStats)

            if start_date is not None:
                query = query.where(DailyStats.date >= start_date)  # type: ignore[operator]
            if end_date is not None:
                query = query.where(DailyStats.date <= end_date)  # type: ignore[operator]

            query = query.order_by(DailyStats.date, DailyStats.campaign_id)
            result = session.execute(query)
            return list(result.scalars().all())  # type: ignore[no-untyped-call]

    def close(self) -> None:
        """Закрытие подключения к БД"""

        self.engine.dispose()
