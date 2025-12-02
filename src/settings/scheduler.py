from pydantic_settings import BaseSettings, SettingsConfigDict


class SchedulerConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    UPDATE_INTERVAL_MINUTES: int = 30

    MAX_UPDATES_PER_DAY: int = 80

    MAX_RETRIES: int = 3
    RETRY_DELAY_SECONDS: int = 60


scheduler_config = SchedulerConfig()
