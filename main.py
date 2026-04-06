import asyncio
import hashlib
import json
import os
import signal
import time
import traceback
import logging
import structlog
from typing import Dict, Any
import urllib.parse
from extractor import UniversalExtractor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from yarl import URL
from pydantic import ValidationError
from schemas import ParserTemplate
from database import DatabaseWriter, init_db, BaseRepository, SQLiteRepository
from parser import UniversalParser

logger = structlog.get_logger(__name__)

def normalize_url(raw_url: str) -> str:
    """
    Нормализует URL.
    Использует yarl для безопасного парсинга.
    """
    try:
        u = URL(raw_url)
        scheme = u.scheme.lower()
        
        host = u.host
        if host and not host.endswith('.onion'):
            host = host.lower()
         
        u = u.with_scheme(scheme).with_host(host).with_fragment(None)
    
        if (scheme == 'http' and u.port == 80) or (scheme == 'https' and u.port == 443):
            u = u.with_port(None)
        
        if u.query:
            sorted_query = sorted(u.query.items())
            u = u.with_query(sorted_query)
        
        norm_str = str(u)
        if norm_str.endswith('/') and u.path == '/':
            norm_str = norm_str[:-1]
        
        return norm_str
    except Exception as e:
        logger.warning(
            "URL normalization failed", 
            marker="[LINK_NORMALIZATION_FAILED]", 
            raw_url=raw_url, 
            error_type=type(e).__name__
        )
        return raw_url

def generate_url_hash(normalized_url: str) -> str:
    """Генерация SHA-256 хэша для быстрого поиска."""
    return hashlib.sha256(normalized_url.encode('utf-8')).hexdigest()

async def process_task(
    task: Dict[str, Any],
    parser: UniversalParser,
    extractor: UniversalExtractor,
    repo: BaseRepository,
    semaphore: asyncio.Semaphore
) -> None:
    task_id = task['id']
    url = task['url']
    mirror_id = task['mirror_id']
    page_type = task['page_type'] # thread_list или thread_page
    
    # Достаем счетчики для Retry механизма
    current_retry = task.get('retry_count', 0)
    max_retries = task.get('max_retries', 3)

    async with semaphore:
        start_time = time.perf_counter()
        logger.info("Starting task crawl", marker="[CRAWL_TASK_START]", task_id=task_id, page_type=page_type, url=url, attempt=current_retry+1)
    
        try:
            forum_id, config_json, version = await repo.get_parser_config(page_type, mirror_id)
            config_template = ParserTemplate()
            
            if forum_id and config_json:
                try:
                    config_template = ParserTemplate.model_validate_json(config_json)
                except ValidationError as e:
                    logger.error("Invalid config", marker="[PARSER_CONFIG_INVALID]")
                    # Фатальная ошибка (кривой JSON конфигурации), ретраить бесполезно, ставим max_retries
                    await repo.mark_task_failed(task_id, "Invalid parser configuration", max_retries, max_retries)
                    return

            html_content = await parser.fetch(
                url, forum_id=forum_id, auth_config=config_template.auth.model_dump(), task_id=task_id
            )
            
            if not html_content:
                # Ошибка сети (Tor отвалился и т.д.) - используем Retry механизм
                await repo.mark_task_failed(task_id, "Failed to fetch", current_retry, max_retries)
                return

            if page_type == 'thread_list':
                extracted_links = extractor.extract_links(html_content, config_template)
                for link_data in extracted_links:
                    raw_url = urllib.parse.urljoin(url, link_data['raw_url'])
                    norm_url = normalize_url(raw_url)
                    url_hash = generate_url_hash(norm_url)
                    
                    result = await repo.save_link(
                        task_id, forum_id, raw_url, norm_url, url_hash, 
                        link_data.get('anchor_text'), link_data.get('context_snippet')
                    )
                    
                    if result == 1 or result is None:
                        logger.info("New link saved", marker="[LINK_SAVED_NEW]", url_hash=url_hash)
                        
                        url_lower = norm_url.lower()
                        
                        # 1. ПРОВЕРКА НА ТЕМУ: Если это конкретный топик -> отправляем на выкачивание
                        if '/threads/' in url_lower or '/t/' in url_lower or 'topic' in url_lower:
                            # Проверяем включена ли эта функция
                            if os.getenv("ENABLE_DEEP_CRAWL", "false").lower() == "true":
                                await repo.enqueue_thread_task(mirror_id, norm_url)
                            else:
                                logger.debug("Deep crawl disabled by config", marker="[ROUTER_SKIP_DEEP]", url=norm_url)
                            
                        # 2. ПРОВЕРКА НА РАЗДЕЛ: Если это подфорум -> отправляем искать в нем темы
                        elif '/forums/' in url_lower or '/f/' in url_lower or 'board' in url_lower:
                            await repo.writer.execute_write(
                                "INSERT OR IGNORE INTO CrawlTask (mirror_id, url, page_type, status) VALUES (?, ?, 'thread_list', 'pending')",
                                (mirror_id, norm_url)
                            )
                            
                        # 3. ИГНОР МУСОРА: Всё остальное (пользователи, логины, FAQ) пропускаем
                        else:
                            logger.debug("Smart-Router ignored irrelevant link", marker="[ROUTER_SKIP]", url=norm_url)

            elif page_type == 'thread_page':
                extracted_posts = extractor.extract_posts(html_content, config_template, url)
                for post in extracted_posts:
                    await repo.save_post(
                        task_id, forum_id, url, post['post_hash'], 
                        post['author'], post['content'], post['published_at']
                    )
                logger.info("Posts extracted", marker="[POSTS_SAVED]", task_id=task_id, count=len(extracted_posts))
                
                next_page_raw = extractor.extract_next_page(html_content, config_template)
                if next_page_raw:
                    next_url = normalize_url(urllib.parse.urljoin(url, next_page_raw))
                    await repo.enqueue_thread_task(mirror_id, next_url)
                    logger.info("Next page enqueued", marker="[NEXT_PAGE_ENQUEUED]", url=next_url)

            if page_type == 'thread_list':
                # Fast Track: Проверили раздел -> Откладываем проверку на 15 минут
                await repo.reschedule_task(task_id, minutes=15)
                logger.info("Fast Track task rescheduled", marker="[FAST_TRACK_RESCHEDULED]", task_id=task_id)
            else:
                # Deep Crawl: Страницу темы выкачиваем один раз и забываем
                await repo.mark_task_done(task_id)
            
        except Exception as e:
            logger.error("Task failed", marker="[CRAWL_TASK_FAILED]", task_id=task_id, error_type=type(e).__name__)
            # Перехват любых исключений с передачей в систему Retry
            await repo.mark_task_failed(task_id, str(e), current_retry, max_retries)

async def crawl_job(repo: BaseRepository, parser: UniversalParser, extractor: UniversalExtractor, semaphore: asyncio.Semaphore) -> None:
    """
    Основная джоба планировщика (BR-004).
    Выбирает batch задач из БД, проставляет статус 'running' и запускает worker-ы.
    """
    job_id = "periodic_crawl"
    start_time = time.perf_counter()

    logger.info("Scheduler job started", marker="[SCHED_JOB_START]", job_id=job_id, scheduled_at=time.time(), tasks_pending_count=0)

    try:
        tasks = await repo.get_pending_tasks(limit=50)
            
        if not tasks:
            logger.info("No tasks to process", marker="[SCHED_JOB_SUCCESS]", job_id=job_id, duration_ms=0, tasks_processed=0, tasks_failed=0)
            return
        
        logger.info(f"Fetched {len(tasks)} tasks", marker="[SCHED_JOB_START]", job_id=job_id, scheduled_at=time.time(), tasks_pending_count=len(tasks))
    
        # Мгновенно лочим задачи в 'running' через очередь записи
        task_ids = [t['id'] for t in tasks]
        await repo.lock_tasks(task_ids)
    
        logger.info("Worker pool started", marker="[WORKER_POOL_START]")
    
        # Запуск задач с использованием ГЛОБАЛЬНОГО семафора
        coroutines =[process_task(task, parser, extractor, repo, semaphore) for task in tasks]
        results = await asyncio.gather(*coroutines, return_exceptions=True)
    
        tasks_failed = sum(1 for r in results if isinstance(r, Exception))
        tasks_processed = len(tasks)
    
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        logger.info(
            "Scheduler job completed", 
            marker="[SCHED_JOB_SUCCESS]", 
            job_id=job_id, 
            duration_ms=duration_ms, 
            tasks_processed=tasks_processed, 
            tasks_failed=tasks_failed
        )
    
    except Exception as e:
        logger.error(
            "Scheduler job failed critically", 
            marker="[SCHED_JOB_FAILED]", 
            error_type=type(e).__name__, 
            traceback=traceback.format_exc()
        )

async def verify_single_link(link: Dict[str, Any], parser: UniversalParser, repo: BaseRepository, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        link_id = link['id']
        url = link['normalized_url']
        forum_id = link['forum_id']
        failures = link.get('consecutive_failures', 0)
        
        logger.debug("Verifying link", marker="[LINK_VERIFY_START]", link_id=link_id, url=url)
        
        is_alive = await parser.verify_url(url, forum_id)
        
        if is_alive:
            # Ссылка жива - обнуляем счетчик падений
            new_status = 'alive'
            new_failures = 0
            await repo.update_link_status(link_id, new_status, new_failures)
            logger.debug("Link is alive", marker="[LINK_ALIVE]", link_id=link_id, url=url)
        else:
            # Ссылка не ответила - увеличиваем счетчик
            new_failures = failures + 1
            if new_failures >= 3:
                # 3 падения подряд — объявляем ссылку мертвой и ищем зеркало
                new_status = 'dead'
                await repo.update_link_status(link_id, new_status, new_failures)
                logger.warning("Link is DEAD", marker="[LINK_DEAD]", link_id=link_id, url=url, failures=new_failures)
                logger.info("Triggering recovery crawl", marker="[RECOVERY_CRAWL_TRIGGERED]", forum_id=forum_id)
                
                # Запускаем механизм Self-Healing
                await repo.trigger_recovery_crawl(forum_id)
            else:
                # Оставляем статус unknown, чтобы перепроверить в следующем цикле
                new_status = 'unknown' 
                await repo.update_link_status(link_id, new_status, new_failures)
                logger.warning("Link verification failed (retrying later)", marker="[LINK_VERIFY_RETRY]", link_id=link_id, url=url, failures=new_failures)

async def verify_links_job(repo: BaseRepository, parser: UniversalParser, semaphore: asyncio.Semaphore) -> None:
    """Фоновая джоба проверки здоровья собранных ссылок."""
    job_id = "verify_links"
    start_time = time.perf_counter()
    
    logger.info("Starting link verification job", marker="[SCHED_JOB_START]", job_id=job_id)
    
    try:
        links = await repo.get_links_for_verification(limit=20)
        if not links:
            return
            
        # Запуск задач с использованием ГЛОБАЛЬНОГО семафора
        coroutines =[verify_single_link(link, parser, repo, semaphore) for link in links]
        
        await asyncio.gather(*coroutines, return_exceptions=True)
        
        duration_ms = int((time.perf_counter() - start_time) * 1000)

        stats, _ = await repo.get_dashboard_metrics()
        logger.info(
            "CYCLE SUMMARY", 
            marker="[BUSINESS_METRICS]",
            собранных_ссылок=stats['total'],
            живых_ссылок=stats['alive'],
            мертвых_заменено=stats['dead']
        )

        logger.info("Verification job completed", marker="[SCHED_JOB_SUCCESS]", job_id=job_id, processed=len(links), duration_ms=duration_ms)
        
    except Exception as e:
        logger.error("Verification job failed", marker="[SCHED_JOB_FAILED]", job_id=job_id, error_type=type(e).__name__)

async def main():
    from datetime import datetime
    """Точка входа приложения. Настраивает зависимости, шедулер и обрабатывает сигналы."""
    db_path = os.getenv("DATABASE_PATH", "./data/aggregator.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    max_concurrent_tasks = int(os.getenv("MAX_CONCURRENT_TASKS", "5"))
    crawl_interval_minutes = int(os.getenv("CRAWL_INTERVAL_MINUTES", "60"))

    logger.info("Application starting", marker="[SYS_STARTUP]", version="0.1.0-prototype", db_path=db_path)
    logger.debug("Configuration loaded", marker="[SYS_CONFIG_LOADED]")

    await init_db(db_path)

    db_writer = DatabaseWriter(db_path)
    await db_writer.start()
    
    repo: BaseRepository = SQLiteRepository(db_path, db_writer)
    parser = UniversalParser(proxy_pool=os.getenv("PROXY_POOL").split(","))
    extractor = UniversalExtractor()

    scheduler = AsyncIOScheduler()

    # СОЗДАЕМ ЕДИНЫЙ ГЛОБАЛЬНЫЙ СЕМАФОР
    global_semaphore = asyncio.Semaphore(max_concurrent_tasks)

    scheduler.add_job(
        crawl_job,
        'interval',
        minutes=crawl_interval_minutes,
        args=[repo, parser, extractor, global_semaphore], # Передаем семафор
        id="periodic_crawl",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        next_run_time=datetime.now()
    )

    # Регистрация джобы верификации (запускаем каждые 10 минут)
    scheduler.add_job(
        verify_links_job,
        'interval',
        minutes=10,
        args=[repo, parser, global_semaphore], # Передаем семафор
        id="verify_links",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        # Запускаем через 1 минуту после старта, чтобы дать парсеру время собрать первые ссылки
        next_run_time=datetime.now() + __import__('datetime').timedelta(minutes=1) 
    )

    logger.info(
        "Job registered in APScheduler", 
        marker="[SCHED_JOB_REGISTERED]", 
        job_id="periodic_crawl", 
        interval_minutes=crawl_interval_minutes
    )

    scheduler.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def shutdown_handler(sig, frame):
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: shutdown_handler(s, None))

    await stop_event.wait()

    logger.info("Shutting down scheduler...")
    scheduler.shutdown(wait=True)

    logger.info("Shutting down database writer...")
    await db_writer.stop()

    logger.info("Closing parser sessions...")
    await parser.close()

    logger.info("Application shutdown complete", marker="[SYS_SHUTDOWN]")

if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(colors=True)
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
