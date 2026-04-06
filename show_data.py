import asyncio
import os
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from database import SQLiteRepository

console = Console()
DB_PATH = os.getenv("DATABASE_PATH", "./data/aggregator.db")

async def show_data():
    if not os.path.exists(DB_PATH):
        console.print("[bold red]Ошибка: База данных не найдена. Сначала запустите парсер.[/bold red]")
        return

    repo = SQLiteRepository(DB_PATH)
    posts = await repo.get_latest_posts(limit=5)

    if not posts:
        console.print("[yellow]В базе пока нет извлеченных постов. Подождите завершения цикла Deep Crawl.[/yellow]")
        return

    console.print(Panel("[bold cyan]ПОСЛЕДНИЕ ИЗВЛЕЧЕННЫЕ ДАННЫЕ (DEEP CRAWL)[/bold cyan]", expand=False))

    for post in posts:
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_row("[bold magenta]Автор:[/bold magenta]", post['author'])
        table.add_row("[bold magenta]Источник:[/bold magenta]", post['thread_url'][:70] + "...")
        table.add_row("[bold magenta]Дата поста:[/bold magenta]", post['published_at'] or "N/A")
        table.add_row("[bold magenta]Собрано:[/bold magenta]", post['scraped_at'])
        
        content_preview = post['content'][:300].replace('\n', ' ') + "..."
        
        console.print(Panel(
            content_preview,
            title=f"[bold green]Сообщение от {post['author']}[/bold green]",
            border_style="green",
            subtitle="[italic]Используйте SQL-клиент для просмотра RAW HTML[/italic]"
        ))
        console.print(table)
        console.print("-" * 50)

if __name__ == "__main__":
    asyncio.run(show_data())