import sys
from pathlib import Path

from loguru import logger


def setup_logger(log_file: str = "etl.log", level: str = "DEBUG") -> None:
    """
    Настройка логирования c помощью loguru.
    """

    logger.remove()

    logger.add(
        sys.stdout,
        colorize=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
        ),
        level=level,
    )

    log_path = Path(__file__).parent.parent.parent / "logs" / log_file
    log_path.parent.mkdir(exist_ok=True)

    logger.add(
        log_path,
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
        level=level,
    )


__all__ = ["logger", "setup_logger"]
