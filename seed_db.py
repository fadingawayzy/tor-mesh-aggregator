import asyncio
import os
import json
import aiosqlite

from database import init_db, DatabaseWriter
from schemas import ParserTemplate, LinkExtractionConfig, AuthConfig

async def seed():
    db_path = os.getenv("DATABASE_PATH", "./data/aggregator.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    await init_db(db_path)
    writer = DatabaseWriter(db_path)
    await writer.start()

    forums_file = "forums.json"
    if not os.path.exists(forums_file):
        print(f"Ошибка: Файл конфигурации {forums_file} не найден!")
        await writer.stop()
        return

    with open(forums_file, "r", encoding="utf-8") as f:
        forums_data = json.load(f)

    for f_data in forums_data:
        print(f"Инициализация источника: {f_data['name']}...")
        
        # 1. Добавляем Форум
        forum_id = await writer.execute_write(
            "INSERT OR IGNORE INTO Forum (slug, display_name) VALUES (?, ?) RETURNING id",
            (f_data["slug"], f_data["name"])
        )
        if not forum_id:
            async with aiosqlite.connect(db_path) as db:
                async with db.execute("SELECT id FROM Forum WHERE slug=?", (f_data["slug"],)) as cursor:
                    forum_id = (await cursor.fetchone())[0]

        # 2. Добавляем Зеркало
        mirrors = f_data.get("mirrors", [f_data.get("url")])
        first_mirror_id = None
        
        for idx, mirror_url in enumerate(mirrors):
            if not mirror_url: 
                continue
            
            # Первое зеркало имеет приоритет 100, второе 90, и т.д.
            priority = 100 - (idx * 10) 
            
            mirror_id = await writer.execute_write(
                "INSERT OR IGNORE INTO Mirror (forum_id, url, priority, health_status) VALUES (?, ?, ?, 'online') RETURNING id",
                (forum_id, mirror_url, priority)
            )
            if not mirror_id:
                async with aiosqlite.connect(db_path) as db:
                    async with db.execute("SELECT id FROM Mirror WHERE url=?", (mirror_url,)) as cursor:
                        mirror_id = (await cursor.fetchone())[0]
            
            if idx == 0:
                first_mirror_id = mirror_id
        # 3. Безопасная сборка AuthConfig
        auth_data = f_data.get("auth", {"enabled": False})
        if auth_data.get("enabled"):
            user_val = auth_data.get("username", "")
            pass_val = auth_data.get("password", "")
            if user_val.startswith("ENV_"):
                auth_data["username"] = os.getenv(user_val[4:], "")
            if pass_val.startswith("ENV_"):
                auth_data["password"] = os.getenv(pass_val[4:], "")
                
            if "cookies" in auth_data:
                resolved_cookies = {}
                for cookie_name, cookie_val in auth_data["cookies"].items():
                    if cookie_val.startswith("ENV_"):
                        resolved_cookies[cookie_name] = os.getenv(cookie_val[4:], "")
                    else:
                        resolved_cookies[cookie_name] = cookie_val
                auth_data["cookies"] = resolved_cookies

        # 4. Конфигурация парсера
        template = ParserTemplate.model_validate(f_data)
        
        async with aiosqlite.connect(db_path) as db:
            async with db.execute("SELECT id FROM ParserConfig WHERE forum_id = ? AND page_type = 'thread_list'", (forum_id,)) as cursor:
                config_exists = await cursor.fetchone()

        if config_exists:
            await writer.execute_write(
                "UPDATE ParserConfig SET config_json = ?, version = version + 1 WHERE forum_id = ? AND page_type = 'thread_list'",
                (template.model_dump_json(), forum_id)
            )
            print(f"Конфигурация обновлена (forum_id: {forum_id})")
        else:
            await writer.execute_write(
                "INSERT INTO ParserConfig (forum_id, page_type, config_json) VALUES (?, 'thread_list', ?)",
                (forum_id, template.model_dump_json())
            )
            print(f"Конфигурация добавлена (forum_id: {forum_id})")
            
        # 5. Ставим задачу в очередь
        if first_mirror_id:
            await writer.execute_write(
                "INSERT OR IGNORE INTO CrawlTask (mirror_id, url, page_type, status) VALUES (?, ?, 'thread_list', 'pending')",
                (first_mirror_id, mirrors[0])
            )

    await writer.stop()
    print(f"База данных успешно инициализирована ({len(forums_data)} источников)!")

if __name__ == "__main__":
    asyncio.run(seed())