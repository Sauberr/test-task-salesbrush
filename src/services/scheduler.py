from datetime import date, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from src.database import Database
from src.services.etl_service import ETLService
from src.services.rate_limiter import RateLimiter
from src.settings.scheduler import scheduler_config


class SchedulerService:
    """
    Сервис для планирования автоматических обновлений данных.
    """

    def __init__(self, database: Database) -> None:
        """
        Инициализация планировщика.
        """

        self.database = database
        self.etl_service = ETLService(database=database)
        self.rate_limiter = RateLimiter()
        self.scheduler = BackgroundScheduler()
        self.is_running = False

    def start(self) -> None:
        """Запуск планировщика"""
        if self.is_running:
            logger.warning("Планировщик уже запущен")
            return

        logger.info("🚀 Запуск планировщика ETL процессов...")

        self.scheduler.add_job(
            func=self._run_etl_job,
            trigger=IntervalTrigger(minutes=scheduler_config.UPDATE_INTERVAL_MINUTES),
            id="etl_job",
            name="ETL процесс c учётом лимитов API",
            replace_existing=True,
        )

        self.scheduler.start()
        self.is_running = True

        logger.info(f"✅ Планировщик запущен. Интервал обновления: {scheduler_config.UPDATE_INTERVAL_MINUTES} мин")

    def stop(self) -> None:
        """Остановка планировщика"""

        if not self.is_running:
            return

        logger.info("🛑 Остановка планировщика...")
        self.scheduler.shutdown()
        self.is_running = False
        logger.info("✅ Планировщик остановлен")

    def _run_etl_job(self) -> None:
        """
        Основная задача ETL c проверкой лимитов и умной загрузкой.
        """

        logger.info("=" * 80)
        logger.info("📊 Запуск плановой задачи ETL")

        if not self.rate_limiter.can_make_request():
            stats = self.rate_limiter.get_stats()
            next_time = self.rate_limiter.get_next_available_time()
            logger.warning(f"⚠️ Достигнут лимит API: {stats['used']}/{stats['total']} ({stats['usage_percent']}%)")
            logger.warning(f"⏳ Следующий доступный слот: {next_time}")
            return

        try:
            dates_to_load = self._get_dates_to_load()

            if not dates_to_load:
                logger.info("✅ Bce данные актуальны, загрузка не требуется")
                return

            logger.info(f"📅 Найдено дат для загрузки: {len(dates_to_load)}")

            for check_date in dates_to_load:
                if not self.rate_limiter.can_make_request():
                    logger.warning("⚠️ Лимит API исчерпан, прерываем загрузку")
                    break

                logger.info(f"📥 Загрузка данных за {check_date}...")

                results = self.etl_service.run(
                    start_date=check_date,
                    end_date=check_date,
                )

                self.rate_limiter.record_request()

                logger.info(f"✅ Загружено записей: {len(results)}")

            stats = self.rate_limiter.get_stats()
            logger.info(f"📊 Использовано API запросов: {stats['used']}/{stats['total']} ({stats['usage_percent']}%)")
            logger.info(f"💚 Доступно запросов: {stats['available']}")

        except Exception as e:
            logger.error(f"❌ Ошибка при выполнении ETL задачи: {e}", exc_info=True)

        logger.info("=" * 80)

    def _get_dates_to_load(self) -> list[date]:
        """
        Определить даты которые нужно загрузить.
        """

        dates_to_check = []
        today = date.today()

        for days_ago in range(7):
            check_date = today - timedelta(days=days_ago)
            dates_to_check.append(check_date)

        dates_to_load = []

        for check_date in dates_to_check:
            if not self._date_has_data(check_date):
                dates_to_load.append(check_date)

        return sorted(dates_to_load)

    def _date_has_data(self, check_date: date) -> bool:
        """
        Проверить есть ли данные для указанной даты в БД.
        """

        with self.database.get_session() as session:
            from sqlalchemy import select

            from src.database.models import DailyStats

            result = session.execute(select(DailyStats).where(DailyStats.date == check_date).limit(1))
            return result.scalar_one_or_none() is not None

    def run_manual_update(self, start_date: date | None = None, end_date: date | None = None) -> None:
        """
        Ручной запуск обновления данных (вне планировщика).
        """

        logger.info("🔧 Ручной запуск обновления данных...")

        if not self.rate_limiter.can_make_request():
            stats = self.rate_limiter.get_stats()
            logger.error(f"❌ Невозможно выполнить обновление. Лимит API: {stats['used']}/{stats['total']}")
            return

        results = self.etl_service.run(start_date=start_date, end_date=end_date)
        self.rate_limiter.record_request()

        stats = self.rate_limiter.get_stats()
        logger.info(f"✅ Обновление завершено. Обработано записей: {len(results)}")
        logger.info(f"📊 API запросов: {stats['used']}/{stats['total']} ({stats['usage_percent']}%)")
