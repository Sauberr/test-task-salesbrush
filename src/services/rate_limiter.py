from datetime import datetime, timedelta

from src.settings.api import api_config


class RateLimiter:
    """
    Класс для контроля лимитов API запросов.
    """

    def __init__(self) -> None:
        self.requests_log: list[datetime] = []
        self.max_requests = api_config.API_MAX_REQUESTS_PER_DAY  # 80

    def can_make_request(self) -> bool:
        """
        Проверка возможности сделать запрос.
        """

        self._cleanup_old_requests()
        return len(self.requests_log) < self.max_requests

    def record_request(self) -> None:
        """Записать новый запрос в лог"""

        self.requests_log.append(datetime.now())

    def get_available_requests(self) -> int:
        """
        Получить количество доступных запросов.
        """

        self._cleanup_old_requests()
        return self.max_requests - len(self.requests_log)

    def get_next_available_time(self) -> datetime | None:
        """
        Получить время когда будет доступен следующий запрос.
        """

        self._cleanup_old_requests()

        if len(self.requests_log) < self.max_requests:
            return None

        oldest_request = min(self.requests_log)
        return oldest_request + timedelta(days=1, seconds=1)

    def _cleanup_old_requests(self) -> None:
        """Удалить запросы старше 24 часов"""

        now = datetime.now()
        cutoff_time = now - timedelta(days=1)
        self.requests_log = [req for req in self.requests_log if req > cutoff_time]

    def get_stats(self) -> dict[str, int | float]:
        """
        Получить статистику использования лимитов.
        """

        self._cleanup_old_requests()
        used = len(self.requests_log)
        available = self.max_requests - used
        usage_percent = (used / self.max_requests * 100) if self.max_requests > 0 else 0

        return {
            "used": used,
            "available": available,
            "total": self.max_requests,
            "usage_percent": round(usage_percent, 2),
        }
