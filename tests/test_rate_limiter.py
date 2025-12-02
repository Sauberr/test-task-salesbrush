from datetime import datetime, timedelta

from src.services.rate_limiter import RateLimiter


class TestRateLimiter:
    """Тесты для класса RateLimiter"""

    def test_initial_state(self):
        """Тест начального состояния RateLimiter"""

        limiter = RateLimiter()

        assert limiter.can_make_request() is True
        assert limiter.get_available_requests() == 80
        assert len(limiter.requests_log) == 0

    def test_record_request(self):
        """Тест записи запроса"""

        limiter = RateLimiter()

        limiter.record_request()

        assert len(limiter.requests_log) == 1
        assert limiter.get_available_requests() == 79

    def test_can_make_request_within_limit(self):
        """Тест что можно делать запросы в пределах лимита"""

        limiter = RateLimiter()

        for _ in range(50):
            limiter.record_request()

        assert limiter.can_make_request() is True
        assert limiter.get_available_requests() == 30

    def test_can_make_request_at_limit(self):
        """Тест что нельзя делать запросы при достижении лимита"""

        limiter = RateLimiter()

        for _ in range(80):
            limiter.record_request()

        assert limiter.can_make_request() is False
        assert limiter.get_available_requests() == 0

    def test_cleanup_old_requests(self):
        """Тест очистки старых запросов (старше 24 часов)"""

        limiter = RateLimiter()

        old_time = datetime.now() - timedelta(days=1, hours=1)
        limiter.requests_log = [old_time, old_time, old_time]

        limiter.record_request()

        limiter._cleanup_old_requests()

        assert len(limiter.requests_log) == 1
        assert limiter.get_available_requests() == 79

    def test_get_stats(self):
        """Тест получения статистики использования"""

        limiter = RateLimiter()
        for _ in range(20):
            limiter.record_request()

        stats = limiter.get_stats()

        assert stats["used"] == 20
        assert stats["available"] == 60
        assert stats["total"] == 80
        assert stats["usage_percent"] == 25.0

    def test_get_next_available_time_with_space(self):
        """Тест что next_available_time = None когда есть свободные слоты"""

        limiter = RateLimiter()
        limiter.record_request()

        next_time = limiter.get_next_available_time()

        assert next_time is None

    def test_get_next_available_time_at_limit(self):
        """Тест расчёта времени следующего доступного слота"""

        limiter = RateLimiter()

        for _ in range(80):
            limiter.record_request()

        next_time = limiter.get_next_available_time()

        assert next_time is not None
        assert isinstance(next_time, datetime)
        assert next_time > datetime.now()
