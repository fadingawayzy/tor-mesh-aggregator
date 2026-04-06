import asyncio
import os
import json
import urllib.request
import aiosqlite
from datetime import datetime
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.console import Console
from rich.align import Align
from database import SQLiteRepository, BaseRepository

console = Console()
DB_PATH = os.getenv("DATABASE_PATH", "./data/aggregator.db")

def get_tor_status():
    """Стучится в API Circuit Manager'а для получения статуса нод."""
    try:
        req = urllib.request.Request("http://circuit-manager:9999/api/status", method="GET")
        with urllib.request.urlopen(req, timeout=1) as response:
            return json.loads(response.read())
    except Exception:
        return None

async def get_db_metrics():
    """Собирает метрики и последние логи из SQLite."""
    stats = {"total": 0, "alive": 0, "dead": 0, "unknown": 0, "tasks": 0, "posts": 0}
    logs = []
    
    if not os.path.exists(DB_PATH):
        return stats, logs

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # 1. Статистика ссылок
        async with db.execute("SELECT status, COUNT(*) as cnt FROM CollectedLink GROUP BY status") as cursor:
            async for row in cursor:
                stats[row["status"]] = row["cnt"]
                stats["total"] += row["cnt"]
                
        # 2. Статистика задач
        async with db.execute("SELECT COUNT(*) as cnt FROM CrawlTask") as cursor:
            stats["tasks"] = (await cursor.fetchone())["cnt"]

        # 2.5 Статистика постов
        async with db.execute("SELECT COUNT(*) as cnt FROM ParsedPost") as cursor:
            stats["posts"] = (await cursor.fetchone())["cnt"]

        # 3. Формирование Live Log (Последние действия Health Checker-а)
        async with db.execute("SELECT id, normalized_url, status, last_checked_at FROM CollectedLink ORDER BY last_checked_at DESC LIMIT 6") as cursor:
            async for row in cursor:
                url = row['normalized_url']
                short_url = url[:45] + "..." if len(url) > 45 else url
                status = row['status']
                dt = row['last_checked_at'] or "Не проверялась"
                
                if status == 'dead':
                    logs.append(f"[{dt[11:19]}] [bold red]DEAD[/bold red]   | Ссылка {short_url} умерла -> Триггер поиска замены")
                elif status == 'alive':
                    logs.append(f"[{dt[11:19]}] [bold green]ALIVE[/bold green]  | Успешный пинг {short_url}")
                elif status == 'unknown':
                    logs.append(f"[{dt[11:19]}] [bold yellow]WARN[/bold yellow]   | Ссылка {short_url} помечена как unknown (в очереди на чек)")

    return stats, logs

def generate_layout(tor_data, stats, logs) -> Layout:
    """Отрисовывает TUI интерфейс."""
    layout = Layout()
    layout.split_column(
        Layout(name="top", size=10),
        Layout(name="bottom")
    )
    layout["top"].split_row(
        Layout(name="tor_status", ratio=1),
        Layout(name="db_stats", ratio=1)
    )

    # Tor Status
    tor_table = Table(expand=True, box=None, show_header=False)
    tor_table.add_column("Node", style="cyan", width=10)
    tor_table.add_column("Status", justify="left")
    
    if tor_data and "nodes" in tor_data:
        for node_id, state in tor_data["nodes"].items():
            status_color = "green" if state.get("connected") and state.get("bootstrapped") else "red"
            circuits = state.get('circuit_count', 0)
            tor_table.add_row(
                f"[bold]{node_id.upper()}[/bold]", 
                f"[{status_color}]● ONLINE[/{status_color}] | Цепей: {circuits} | Ротаций: {state.get('rotation_count', 0)}"
            )
    else:
        tor_table.add_row("API", "[bold red]● OFFLINE[/bold red] (Circuit Manager недоступен)")

    layout["tor_status"].update(Panel(tor_table, title="[bold blue]🧅 Tor Mesh Status[/bold blue]", border_style="blue"))

    # База Данных
    stats_table = Table(expand=True, box=None, show_header=False)
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="bold white", justify="right")
    
    stats_table.add_row("Всего собрано ссылок:", str(stats["total"]))
    stats_table.add_row("🟢 Живых (Alive):", f"[green]{stats['alive']}[/green]")
    stats_table.add_row("🔴 Мертвых (Dead):", f"[red]{stats['dead']}[/red]")
    stats_table.add_row("🟡 Требуют проверки:", f"[yellow]{stats['unknown']}[/yellow]")
    stats_table.add_row("Всего задач парсинга:", str(stats["tasks"]))
    stats_table.add_row("Выкачано постов (Deep Crawl):", f"[bold cyan]{stats['posts']}[/bold cyan]")

    layout["db_stats"].update(Panel(stats_table, title="[bold magenta]📊 System Metrics[/bold magenta]", border_style="magenta"))

    # Live Log
    log_text = "\n".join(logs) if logs else "Ожидание первых данных..."
    layout["bottom"].update(Panel(log_text, title="[bold green]⚡ Live Action Log (Health Checker)[/bold green]", border_style="green"))

    return layout

async def main():
    console.clear()
    
    # Используем репозиторий для дашборда (только для чтения)
    repo: BaseRepository = SQLiteRepository(DB_PATH)
    
    with Live(refresh_per_second=2, screen=True) as live:
        while True:
            tor_data = get_tor_status()
            stats, logs = await repo.get_dashboard_metrics()
            live.update(generate_layout(tor_data, stats, logs))
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold red]Dashboard closed.[/bold red]")