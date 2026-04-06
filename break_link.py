import asyncio
import os
import aiosqlite

async def break_link():
    # Используем тот же путь к БД, что и весь проект
    db_path = os.getenv("DATABASE_PATH", "./data/aggregator.db")
    
    if not os.path.exists(db_path):
        print(f"Ошибка: База данных не найдена по пути {db_path}")
        return

    async with aiosqlite.connect(db_path) as db:
        # 1. Ищем одну любую живую ссылку
        async with db.execute(
            "SELECT id, normalized_url FROM CollectedLink WHERE status = 'alive' ORDER BY RANDOM() LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            print("Нет живых ссылок для поломки. Подождите пару минут, пока парсер соберет первые данные.")
            return

        link_id, old_url = row
        
        # 2. Искусственно ломаем URL
        broken_url = old_url + ".broken.onion"

        # 3. Меняем URL, ставим статус 'unknown' и откидываем дату проверки в 1970 год.
        await db.execute(
            """
            UPDATE CollectedLink 
            SET normalized_url = ?, 
                raw_url = ?, 
                status = 'unknown', 
                last_checked_at = '1970-01-01 00:00:00' 
            WHERE id = ?
            """,
            (broken_url, broken_url, link_id)
        )
        await db.commit()

        print(f"ID ссылки: {link_id}")
        print(f"Старый URL (рабочий): {old_url}")
        print(f"Новый URL (сломанный): {broken_url}")
        print("Health Checker подхватит её на следующем цикле")
        print("и автоматически пометит как dead, после чего запустится поиск замены.\n")

if __name__ == "__main__":
    asyncio.run(break_link())