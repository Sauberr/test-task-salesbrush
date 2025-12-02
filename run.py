import signal
import sys
import time
from datetime import date

import typer
from loguru import logger
from rich.console import Console

from src.database import Database
from src.services import ETLService
from src.services.scheduler import SchedulerService
from src.utils import setup_logger

setup_logger()

app = typer.Typer(help="ETL скрипт для расчёта CPA и синхронизации данных")
console = Console()


def signal_handler(signum: int, frame) -> None:  # type: ignore
    """Обработчик сигналов для graceful shutdown планировщика"""

    logger.info("\n🛑 Получен сигнал остановки...")
    sys.exit(0)


def run_scheduler() -> None:
    """Запуск планировщика c автоматическим обновлением данных"""

    console.print("\n[bold blue]🚀 Запуск планировщика ETL процессов...[/bold blue]\n")
    logger.info("=" * 80)
    logger.info("🚀 Запуск планировщика ETL процессов")
    logger.info("=" * 80)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    db = Database()
    db.init_db()
    logger.info("✅ База данных инициализирована")
    console.print("[green]✅ База данных инициализирована[/green]")

    scheduler_service = SchedulerService(database=db)

    try:
        scheduler_service.start()

        console.print("\n[bold cyan]📊 Планировщик работает.[/bold cyan]")
        console.print("[yellow]⏰ Интервал обновления: 30 минут[/yellow]")
        console.print("[yellow]📊 Лимит API: 80 запросов/день (20% резерв)[/yellow]")
        console.print("\n[dim]Нажмите Ctrl+C для остановки...[/dim]\n")
        logger.info("📊 Планировщик работает. Нажмите Ctrl+C для остановки.")
        logger.info("=" * 80)

        while True:
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        logger.info("\n🛑 Остановка планировщика...")
        console.print("\n[yellow]🛑 Остановка планировщика...[/yellow]")
    finally:
        scheduler_service.stop()
        db.close()
        logger.info("✅ Планировщик остановлен")
        console.print("[green]✅ Планировщик остановлен[/green]\n")


@app.command()
def main(
    start_date: str | None = typer.Option(
        None,
        "--start-date",
        help="Начальная дата в формате ISO (YYYY-MM-DD)",
    ),
    end_date: str | None = typer.Option(
        None,
        "--end-date",
        help="Конечная дата в формате ISO (YYYY-MM-DD)",
    ),
    scheduler: bool = typer.Option(
        False,
        "--scheduler",
        "-s",
        help="Запустить планировщик для автоматического обновления данных",
    ),
) -> None:
    """
    Запуск ETL процесса для расчёта CPA и загрузки данных в БД.

    Режимы работы:
    - Без флагов: разовая загрузка всех данных
    - C --start-date/--end-date: загрузка за период
    - C --scheduler: запуск автоматического планировщика (работает постоянно)
    """

    if scheduler:
        run_scheduler()
        return

    console.print("\n[bold blue]🚀 Запуск ETL процесса...[/bold blue]\n")

    parsed_start_date: date | None = None
    parsed_end_date: date | None = None

    if start_date:
        try:
            parsed_start_date = date.fromisoformat(start_date)
            console.print(f"📅 Начальная дата: [green]{parsed_start_date}[/green]")
        except ValueError as err:
            console.print(f"[red]❌ Ошибка: неверный формат даты '{start_date}'. Используйте YYYY-MM-DD[/red]")
            raise typer.Exit(code=1) from err

    if end_date:
        try:
            parsed_end_date = date.fromisoformat(end_date)
            console.print(f"📅 Конечная дата: [green]{parsed_end_date}[/green]")
        except ValueError as err:
            console.print(f"[red]❌ Ошибка: неверный формат даты '{end_date}'. Используйте YYYY-MM-DD[/red]")
            raise typer.Exit(code=1) from err

    try:
        logger.info("🔧 Инициализация базы данных...")
        console.print("\n[cyan]🔧 Инициализация базы данных...[/cyan]")
        db = Database()
        db.init_db()
        console.print("[green]✅ База данных готова[/green]")
        logger.success("База данных инициализирована")

        logger.info("📊 Запуск ETL процесса...")
        console.print("\n[cyan]📊 Загрузка и обработка данных...[/cyan]")
        etl_service = ETLService(database=db)
        results = etl_service.run(
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        etl_service.print_summary(results)

        console.print("[bold green]✨ ETL процесс завершён успешно![/bold green]\n")
        logger.success("ETL процесс завершён успешно")

    except FileNotFoundError as e:
        logger.error(f"Файл не найден: {e}")
        console.print(f"[red]❌ Ошибка: файл не найден - {e}[/red]")
        raise typer.Exit(code=1) from e
    except Exception as e:
        logger.exception(f"Ошибка при выполнении ETL процесса: {e}")
        console.print(f"[red]❌ Ошибка при выполнении ETL процесса: {e}[/red]")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
