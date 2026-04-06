import sqlite3
import sys
from rich.console import Console
from rich.table import Table

# Берем лимит из аргументов командной строки (по умолчанию 20)
limit = 20
if len(sys.argv) > 1:
    try:
        limit = int(sys.argv[1])
    except ValueError:
        pass

conn = sqlite3.connect('/app/data/aggregator.db')
console = Console()

table = Table(title='[bold yellow] НАЙДЕННЫЕ ССЫЛКИ[/bold yellow]')

table.add_column('ID', style='cyan', no_wrap=True)
table.add_column('Тип', justify='center')
table.add_column('Заголовок (Anchor)', style='white')
table.add_column('URL (Ссылка)', style='blue', overflow='ellipsize')
table.add_column('Статус', justify='center')

# Если лимит больше 0, добавляем его в запрос. Если 0 — выводим всё.
limit_sql = f" LIMIT {limit}" if limit > 0 else ""
query = f'''
    SELECT id, anchor_text, normalized_url, status 
    FROM CollectedLink 
    ORDER BY id DESC{limit_sql}
'''

for r in conn.execute(query):
    url = r[2]
    link_type = '📝' if '/threads/' in url.lower() or '/t/' in url.lower() else '📁'
    status_color = 'green' if r[3] == 'alive' else 'yellow'
    
    table.add_row(
        str(r[0]),
        link_type,
        (r[1] or 'N/A')[:40],
        url,
        f'[{status_color}]{r[3]}[/{status_color}]'
    )

console.print(table)
conn.close()
