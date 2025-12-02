from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class APIConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    DATA_DIR: Path = Path(__file__).parent.parent.parent / "data"
    FB_SPEND_FILE: str = "fb_spend.json"
    NETWORK_CONV_FILE: str = "network_conv.json"

    API_DAILY_LIMIT: int = 100
    API_SAFETY_MARGIN: float = 0.2
    API_MAX_REQUESTS_PER_DAY: int = int(API_DAILY_LIMIT * (1 - API_SAFETY_MARGIN))

    @property
    def fb_spend_path(self) -> Path:
        return self.DATA_DIR / self.FB_SPEND_FILE

    @property
    def network_conv_path(self) -> Path:
        return self.DATA_DIR / self.NETWORK_CONV_FILE


api_config = APIConfig()
