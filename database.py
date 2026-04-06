import asyncio
import sqlite3
import time
import traceback
import os
from abc import ABC, abstractmethod
from typing import Any, Tuple, Optional, List, Dict
import aiosqlite
import structlog
logger = structlog.get_logger(__name__)

INITIAL_MIGRATION_DDL = """
CREATE TABLE IF NOT EXISTS Forum (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    status TEXT DEFAULT 'active' CHECK(status IN ('active', 'paused', 'archived')),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Mirror (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    forum_id INTEGER NOT NULL,
    url TEXT NOT NULL UNIQUE,
    priority INTEGER DEFAULT 100,
    health_status TEXT DEFAULT 'unknown' CHECK(health_status IN ('online', 'offline', 'degraded', 'unknown')),
    consecutive_failures INTEGER DEFAULT 0,
    last_checked_at DATETIME,
    last_online_at DATETIME,
    failure_reason TEXT,
    FOREIGN KEY(forum_id) REFERENCES Forum(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS MirrorGraph (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    src_mirror_id INTEGER NOT NULL,
    dst_mirror_id INTEGER NOT NULL,
    edge_weight INTEGER DEFAULT 1,
    FOREIGN KEY(src_mirror_id) REFERENCES Mirror(id) ON DELETE CASCADE,
    FOREIGN KEY(dst_mirror_id) REFERENCES Mirror(id) ON DELETE CASCADE,
    UNIQUE(src_mirror_id, dst_mirror_id)
);

CREATE TABLE IF NOT EXISTS ParserConfig (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    forum_id INTEGER NOT NULL,
    page_type TEXT NOT NULL CHECK(page_type IN ('thread_list', 'thread_page', 'post_page', 'search_results')),
    version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT 1,
    config_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(forum_id) REFERENCES Forum(id) ON DELETE CASCADE,
    UNIQUE(forum_id, page_type, version)
);

CREATE TABLE IF NOT EXISTS CrawlTask (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mirror_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    page_type TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'done', 'failed', 'migrated')),
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    scheduled_at DATETIME,
    started_at DATETIME,
    finished_at DATETIME,
    error_message TEXT,
    parent_task_id INTEGER,
    FOREIGN KEY(mirror_id) REFERENCES Mirror(id) ON DELETE CASCADE,
    FOREIGN KEY(parent_task_id) REFERENCES CrawlTask(id) ON DELETE SET NULL,
    UNIQUE(mirror_id, url, page_type)
);

CREATE TABLE IF NOT EXISTS CollectedLink (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    forum_id INTEGER NOT NULL,
    raw_url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    url_hash TEXT NOT NULL UNIQUE,
    anchor_text TEXT,
    context_snippet TEXT,
    link_type TEXT DEFAULT 'unknown' CHECK(link_type IN ('onion', 'clearnet', 'i2p', 'unknown')),
    status TEXT DEFAULT 'alive' CHECK(status IN ('alive', 'dead', 'unknown')), 
    consecutive_failures INTEGER DEFAULT 0,
    last_checked_at DATETIME, 
    first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    seen_count INTEGER DEFAULT 1,
    FOREIGN KEY(task_id) REFERENCES CrawlTask(id) ON DELETE CASCADE,
    FOREIGN KEY(forum_id) REFERENCES Forum(id) ON DELETE CASCADE
);

/* --- НОВАЯ ТАБЛИЦА ДЛЯ ПОСТОВ (Deep Crawl) --- */
CREATE TABLE IF NOT EXISTS ParsedPost (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    forum_id INTEGER NOT NULL,
    thread_url TEXT NOT NULL,
    post_hash TEXT NOT NULL UNIQUE,
    author TEXT,
    content TEXT,
    published_at TEXT,
    scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(task_id) REFERENCES CrawlTask(id) ON DELETE CASCADE,
    FOREIGN KEY(forum_id) REFERENCES Forum(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_crawltask_status ON CrawlTask(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_collectedlink_hash ON CollectedLink(url_hash);
CREATE INDEX IF NOT EXISTS idx_parsedpost_hash ON ParsedPost(post_hash);
"""

async def init_db(db_path: str) -> None:
    logger.info("Starting DB initialization", marker="[DB_INIT_START]", db_path=db_path)
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("PRAGMA foreign_keys=ON;")
            await db.execute("PRAGMA busy_timeout=5000;")
        
            statements = INITIAL_MIGRATION_DDL.strip().split(';')
            statements_count = 0
        
            for statement in statements:
                if statement.strip():
                    await db.execute(statement)
                    statements_count += 1
                
            await db.commit()
        
            logger.info("Migration applied", marker="[DB_MIGRATION_APPLIED]", version="initial", statements_count=statements_count)
        logger.info("DB initialized successfully", marker="[DB_INIT_SUCCESS]", version="1.0")
    
    except Exception as e:
        logger.error("Failed to initialize database", marker="[SYS_CONFIG_ERROR]", error_type=type(e).__name__, traceback=traceback.format_exc())
        raise

class DatabaseWriter:
    """Единственная точка входа для операций записи (INSERT/UPDATE/DELETE)."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._queue: asyncio.Queue[Tuple[str, tuple, asyncio.Future]] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if not self._worker_task:
            self._stop_event.clear()
            self._worker_task = asyncio.create_task(self._process_queue())
            logger.info("DatabaseWriter started", marker="[SYS_STARTUP]", feature="database_writer")

    async def stop(self) -> None:
        if self._worker_task:
            self._stop_event.set()
            await self._queue.join()
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    async def execute_write(self, query: str, parameters: tuple = ()) -> Any:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((query, parameters, future))
        
        logger.debug("Write operation enqueued", marker="[DB_WRITE_ENQUEUED]", query_prefix=query[:50].replace('\n', ' '))
        return await future

    async def execute_write_many(self, query: str, parameters_list: list[tuple]) -> int:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((query, parameters_list, future))
        return await future

    async def _process_queue(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("PRAGMA foreign_keys=ON;")
            await db.execute("PRAGMA busy_timeout=5000;")
            
            while not self._stop_event.is_set() or not self._queue.empty():
                try:
                    queue_item = await asyncio.wait_for(self._queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                
                query, parameters, future = queue_item
                start_time = time.perf_counter()
                
                retry_count = 0
                max_retries = 3
                success = False

                while not success and retry_count < max_retries:
                    try:
                        if isinstance(parameters, list):
                            cursor = await db.executemany(query, parameters)
                            result = cursor.rowcount
                        else:
                            cursor = await db.execute(query, parameters)
                            if "RETURNING" in query.upper():
                                row = await cursor.fetchone()
                                result = row[0] if row else cursor.rowcount
                            else:
                                result = cursor.lastrowid if cursor.lastrowid else cursor.rowcount
                            
                        await db.commit()
                        success = True
                        
                        duration_ms = int((time.perf_counter() - start_time) * 1000)
                        logger.debug("Write operation successful", marker="[DB_WRITE_SUCCESS]", duration_ms=duration_ms, affected_rows=cursor.rowcount)
                        
                        if not future.done():
                            future.set_result(result)
                            
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e) or "busy" in str(e):
                            retry_count += 1
                            logger.warning("Database busy, retrying", marker="[DB_BUSY_RETRY]", attempt=retry_count)
                            await asyncio.sleep(0.1 * (2 ** retry_count))
                        else:
                            await db.rollback()
                            self._handle_db_error(e, future)
                            break
                            
                    except Exception as e:
                        await db.rollback()
                        self._handle_db_error(e, future)
                        break
                        
                if not success and not future.done():
                    future.set_exception(RuntimeError("Max retries exceeded for DB write"))
                    
                self._queue.task_done()

    def _handle_db_error(self, error: Exception, future: asyncio.Future) -> None:
        logger.error("Write operation failed", marker="[DB_WRITE_FAILED]", error_type=type(error).__name__, traceback=traceback.format_exc())
        if not future.done():
            future.set_exception(error)

class BaseRepository(ABC):
    @abstractmethod
    async def get_pending_tasks(self, limit: int = 50) -> List[Dict[str, Any]]: pass
    @abstractmethod
    async def lock_tasks(self, task_ids: List[int]) -> None: pass
    @abstractmethod
    async def get_parser_config(self, page_type: str, mirror_id: int) -> Tuple[int, str, int]: pass
    @abstractmethod
    async def mark_task_failed(self, task_id: int, error_msg: str, current_retry: int = 0, max_retries: int = 0) -> None: pass
    @abstractmethod
    async def mark_task_done(self, task_id: int) -> None: pass
    @abstractmethod
    async def save_link(self, task_id: int, forum_id: int, raw_url: str, norm_url: str, url_hash: str, anchor_text: Optional[str], context_snippet: Optional[str]) -> Optional[int]: pass
    @abstractmethod
    async def get_links_for_verification(self, limit: int = 20) -> List[Dict[str, Any]]: pass
    @abstractmethod
    async def update_link_status(self, link_id: int, status: str, consecutive_failures: int = 0) -> None: pass
    @abstractmethod
    async def trigger_recovery_crawl(self, forum_id: int) -> None: pass
    @abstractmethod
    async def get_dashboard_metrics(self) -> Tuple[Dict[str, int], List[str]]: pass
    @abstractmethod
    async def save_post(self, task_id: int, forum_id: int, thread_url: str, post_hash: str, author: Optional[str], content: Optional[str], published_at: Optional[str]) -> None: pass
    @abstractmethod
    async def enqueue_thread_task(self, mirror_id: int, url: str) -> None: pass
    @abstractmethod
    async def reschedule_task(self, task_id: int, minutes: int) -> None: pass
    @abstractmethod
    async def get_latest_posts(self, limit: int = 5) -> List[Dict[str, Any]]: pass

class SQLiteRepository(BaseRepository):
    def __init__(self, db_path: str, writer: Optional[DatabaseWriter] = None):
        self.db_path = db_path
        self.writer = writer

    async def get_pending_tasks(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = "SELECT * FROM CrawlTask WHERE status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP) ORDER BY page_type DESC, id ASC LIMIT ?"
            async with db.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            
    async def lock_tasks(self, task_ids: List[int]) -> None:
        if not task_ids:
            return
        placeholders = ','.join(['?'] * len(task_ids))
        query = f"UPDATE CrawlTask SET status = 'running', started_at = CURRENT_TIMESTAMP WHERE id IN ({placeholders})"
        await self.writer.execute_write(query, tuple(task_ids))

    async def get_parser_config(self, page_type: str, mirror_id: int) -> Tuple[int, str, int]:
        async with aiosqlite.connect(self.db_path) as db:
            query = """
                SELECT f.id, pc.config_json, pc.version
                FROM Mirror m
                JOIN Forum f ON m.forum_id = f.id
                JOIN ParserConfig pc ON pc.forum_id = f.id AND pc.page_type = ?
                WHERE m.id = ? AND pc.is_active = 1
                ORDER BY pc.version DESC LIMIT 1
            """
            async with db.execute(query, (page_type, mirror_id)) as cursor:
                row = await cursor.fetchone()
                return row if row else (0, "{}", 0)

    async def mark_task_failed(self, task_id: int, error_msg: str, current_retry: int = 0, max_retries: int = 0) -> None:
        if current_retry < max_retries:
            query = "UPDATE CrawlTask SET status = 'pending', retry_count = retry_count + 1, error_message = ? WHERE id = ?"
            await self.writer.execute_write(query, (error_msg, task_id))
        else:
            query = "UPDATE CrawlTask SET status = 'failed', error_message = ?, finished_at = CURRENT_TIMESTAMP WHERE id = ?"
            await self.writer.execute_write(query, (error_msg, task_id))

    async def mark_task_done(self, task_id: int) -> None:
        query = "UPDATE CrawlTask SET status = 'done', finished_at = CURRENT_TIMESTAMP WHERE id = ?"
        await self.writer.execute_write(query, (task_id,))
    
    async def reschedule_task(self, task_id: int, minutes: int) -> None:
        query = f"UPDATE CrawlTask SET status = 'pending', scheduled_at = datetime(CURRENT_TIMESTAMP, '+{minutes} minutes') WHERE id = ?"
        await self.writer.execute_write(query, (task_id,))

    async def save_link(self, task_id: int, forum_id: int, raw_url: str, norm_url: str, url_hash: str, anchor_text: Optional[str], context_snippet: Optional[str]) -> Optional[int]:
        insert_query = """
            INSERT INTO CollectedLink (task_id, forum_id, raw_url, normalized_url, url_hash, anchor_text, context_snippet, link_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'unknown')
            ON CONFLICT(url_hash) DO UPDATE SET 
                last_seen_at = CURRENT_TIMESTAMP,
                seen_count = seen_count + 1
            RETURNING seen_count
        """
        return await self.writer.execute_write(insert_query, (task_id, forum_id, raw_url, norm_url, url_hash, anchor_text, context_snippet))
    
    async def get_links_for_verification(self, limit: int = 20) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT id, normalized_url, forum_id, IFNULL(consecutive_failures, 0) as consecutive_failures 
                FROM CollectedLink 
                WHERE status IN ('alive', 'unknown') 
                ORDER BY IFNULL(last_checked_at, '1970-01-01') ASC 
                LIMIT ?
            """
            async with db.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def update_link_status(self, link_id: int, status: str, consecutive_failures: int = 0) -> None:
        query = "UPDATE CollectedLink SET status = ?, consecutive_failures = ?, last_checked_at = CURRENT_TIMESTAMP WHERE id = ?"
        await self.writer.execute_write(query, (status, consecutive_failures, link_id))

    async def trigger_recovery_crawl(self, forum_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT id, url FROM Mirror WHERE forum_id = ? AND health_status != 'offline' ORDER BY priority DESC LIMIT 1", (forum_id,)) as cursor:
                mirror = await cursor.fetchone()

        if not mirror:
            logger.warning("No active mirror found for recovery", forum_id=forum_id)
            return

        mirror_id, mirror_url = mirror

        query = """
            INSERT INTO CrawlTask (mirror_id, url, page_type, status) 
            VALUES (?, ?, 'thread_list', 'pending')
            ON CONFLICT(mirror_id, url, page_type) DO UPDATE SET 
                status = 'pending', 
                retry_count = 0,
                scheduled_at = CURRENT_TIMESTAMP
        """
        await self.writer.execute_write(query, (mirror_id, mirror_url))
        logger.info("Recovery crawl task UPSERTED", marker="[RECOVERY_TASK_CREATED]", mirror_id=mirror_id, url=mirror_url)

    async def save_post(self, task_id: int, forum_id: int, thread_url: str, post_hash: str, author: Optional[str], content: Optional[str], published_at: Optional[str]) -> None:
        insert_query = """
            INSERT OR IGNORE INTO ParsedPost (task_id, forum_id, thread_url, post_hash, author, content, published_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        await self.writer.execute_write(insert_query, (task_id, forum_id, thread_url, post_hash, author, content, published_at))
        
    async def enqueue_thread_task(self, mirror_id: int, url: str) -> None:
        query = "INSERT OR IGNORE INTO CrawlTask (mirror_id, url, page_type, status) VALUES (?, ?, 'thread_page', 'pending')"
        await self.writer.execute_write(query, (mirror_id, url))
        logger.debug("Enqueued deep crawl task", marker="[TASK_ENQUEUED]", url=url)

    async def get_dashboard_metrics(self) -> Tuple[Dict[str, int], List[str]]:
        stats = {"total": 0, "alive": 0, "dead": 0, "unknown": 0, "tasks": 0, "posts": 0}
        logs = []
        if not os.path.exists(self.db_path):
            return stats, logs

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT status, COUNT(*) as cnt FROM CollectedLink GROUP BY status") as cursor:
                async for row in cursor:
                    stats[row["status"]] = row["cnt"]
                    stats["total"] += row["cnt"]
            async with db.execute("SELECT COUNT(*) as cnt FROM CrawlTask") as cursor:
                stats["tasks"] = (await cursor.fetchone())["cnt"]
            async with db.execute("SELECT COUNT(*) as cnt FROM ParsedPost") as cursor:
                stats["posts"] = (await cursor.fetchone())["cnt"]
                
            async with db.execute("SELECT id, normalized_url, status, last_checked_at FROM CollectedLink ORDER BY last_checked_at DESC LIMIT 6") as cursor:
                async for row in cursor:
                    url = row['normalized_url']
                    short_url = url[:45] + "..." if len(url) > 45 else url
                    dt = row['last_checked_at'] or "Не проверялась"
                    if row['status'] == 'dead':
                        logs.append(f"[{dt[11:19]}] [bold red]DEAD[/bold red]   | Ссылка {short_url} умерла -> Триггер поиска замены")
                    elif row['status'] == 'alive':
                        logs.append(f"[{dt[11:19]}] [bold green]ALIVE[/bold green]  | Успешный пинг {short_url}")
                    elif row['status'] == 'unknown':
                        logs.append(f"[{dt[11:19]}] [bold yellow]WARN[/bold yellow]   | Ссылка {short_url} помечена как unknown")
        return stats, logs
    
    async def get_latest_posts(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Извлекает последние n постов для демонстрации результатов."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT author, thread_url, content, published_at, scraped_at 
                FROM ParsedPost 
                ORDER BY scraped_at DESC LIMIT ?
            """
            async with db.execute(query, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]