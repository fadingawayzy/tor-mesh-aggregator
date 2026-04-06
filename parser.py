import asyncio
import random
import time
import structlog
import urllib.parse
import urllib.request
from typing import Optional, Dict, List
from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError
import parsel

logger = structlog.get_logger(__name__)

class UniversalParser:
    def __init__(self, proxy_pool: List[str]):
        self.proxy_pool = proxy_pool
        self.sessions: Dict[int, requests.AsyncSession] = {}
        self._auth_locks: Dict[int, asyncio.Lock] = {}
        self.proxy_penalties: Dict[str, float] = {p: 0.0 for p in proxy_pool}
        logger.info("UniversalParser initialized", marker="[SYS_STARTUP]", proxy_count=len(proxy_pool))
    
    def _get_best_proxy(self, attempted_proxies: set) -> str:
        now = time.time()
        available =[p for p in self.proxy_pool if p not in attempted_proxies and now >= self.proxy_penalties.get(p, 0)]
        if available:
            return random.choice(available)
        unattempted =[p for p in self.proxy_pool if p not in attempted_proxies]
        if unattempted:
            return random.choice(unattempted)
        return random.choice(self.proxy_pool)
    
    def _get_session(self, forum_id: int) -> requests.AsyncSession:
        if forum_id not in self.sessions:
            # Имитируем Chrome 120 + добавляем реалистичные дефолтные заголовки против Cloudflare
            default_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"'
            }
            session = requests.AsyncSession(
                impersonate="chrome120", 
                verify=False, 
                timeout=90,
                headers=default_headers
            )
            setattr(session, 'is_authenticated', False)
            self.sessions[forum_id] = session
        return self.sessions[forum_id]

    async def verify_url(self, url: str, forum_id: int) -> bool:
        try:
            resp = await self._request_with_retry("GET", url, forum_id, task_id=0)
            if resp is None or resp.status_code in (404, 502, 503):
                return False
            return True
        except Exception as e:
            logger.error("Verify URL exception", url=url, error_type=type(e).__name__)
            return False

    def _get_lock(self, forum_id: int) -> asyncio.Lock:
        if forum_id not in self._auth_locks:
            self._auth_locks[forum_id] = asyncio.Lock()
        return self._auth_locks[forum_id]

    async def close(self):
        for forum_id, session in self.sessions.items():
            await session.close()
        logger.debug("All sessions closed", marker="[SYS_SHUTDOWN]")

    def _trigger_tor_rotation(self):
        """Отправляет сигнал в Circuit Manager на смену IP-адресов при блокировке."""
        try:
            req = urllib.request.Request("http://circuit-manager:9999/api/rotate", method="POST")
            with urllib.request.urlopen(req, timeout=2) as response:
                logger.warning("Tor rotation triggered successfully", marker="[TOR_ROTATION_TRIGGERED]")
        except Exception as e:
            logger.error("Failed to trigger Tor rotation", error_type=type(e).__name__)

    async def _request_with_retry(self, method: str, url: str, forum_id: int, task_id: int, **kwargs) -> Optional[requests.Response]:
        max_retries = 3
        base_delay = 2.0
        session = self._get_session(forum_id)
        attempted_proxies = set() 
        
        for attempt in range(1, max_retries + 1):
            proxy = self._get_best_proxy(attempted_proxies)
            attempted_proxies.add(proxy) 
            proxies = {"http": proxy, "https": proxy}
            
            if attempt == 1:
                jitter_ms = int(random.uniform(1.5, 4.5) * 1000)
                await asyncio.sleep(jitter_ms / 1000.0)
            
            start_time = time.perf_counter()
            try:
                if method.upper() == "GET":
                    response = await session.get(url, proxies=proxies, **kwargs)
                else:
                    response = await session.post(url, proxies=proxies, **kwargs)
                    
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                
                # Защита от Cloudflare / 403
                if response.status_code in (403, 429):
                    logger.error(f"HTTP {response.status_code} detected! Captcha/Ban.", url=url)
                    self._trigger_tor_rotation() # <--- СРАЗУ МЕНЯЕМ IP!
                    raise RequestsError(f"HTTP {response.status_code}")
                    
                if response.status_code >= 500:
                    raise RequestsError(f"HTTP {response.status_code}")
                    
                logger.debug("Request successful", marker="[HTTP_REQUEST_SUCCESS]", status_code=response.status_code, proxy=proxy)
                self.proxy_penalties[proxy] = 0.0 
                return response
                
            except Exception as e:
                self.proxy_penalties[proxy] = time.time() + 60.0
                if attempt < max_retries:
                    backoff = min(base_delay * (2 ** attempt) + random.uniform(0, 1), 60.0)
                    await asyncio.sleep(backoff)
                else:
                    logger.error("Exhausted retries", marker="[CRAWL_TASK_EXHAUSTED]", task_id=task_id, url=url)
        return None

    async def authenticate_if_needed(self, forum_id: int, auth_config: dict, task_id: int, current_target_url: str) -> bool:
        """Передаем current_target_url, чтобы знать, для какого зеркала ставить куки."""
        if not auth_config or not auth_config.get("enabled"):
            return True
            
        lock = self._get_lock(forum_id)
        async with lock:
            session = self._get_session(forum_id)
            if getattr(session, 'is_authenticated', False):
                return True
                
            # Динамически вычисляем домен текущего зеркала
            current_domain = urllib.parse.urlparse(current_target_url).hostname
            
            # Если login_url относительный (начинается с /), клеим его к текущему домену
            login_url = auth_config.get("login_url", "")
            if login_url.startswith("/"):
                base_scheme = urllib.parse.urlparse(current_target_url).scheme
                login_url = f"{base_scheme}://{current_domain}{login_url}"
                
            logger.info("Starting auth flow", marker="[HTTP_AUTH_START]", forum_id=forum_id, domain=current_domain)
            
            if auth_config.get("cookies"):
                if current_domain:
                    for c_name, c_val in auth_config["cookies"].items():
                        # КУКИ ПРИВЯЗЫВАЮТСЯ К ТЕКУЩЕМУ АКТИВНОМУ ЗЕРКАЛУ
                        session.cookies.set(c_name, c_val, domain=f".{current_domain}")
                
                logger.info("Session cookies injected dynamically", marker="[HTTP_AUTH_COOKIE_INJECT]", domain=current_domain)
                setattr(session, 'is_authenticated', True)
                return True
                
            resp = await self._request_with_retry("GET", login_url, forum_id, task_id)
            if not resp: return False
                
            payload = {}
            if auth_config.get("username_field") and auth_config.get("password_field"):
                payload[auth_config.get("username_field")] = auth_config.get("username", "")
                payload[auth_config.get("password_field")] = auth_config.get("password", "")
            
            if auth_config.get("csrf_selector"):
                sel = parsel.Selector(text=resp.text)
                csrf_token = sel.css(auth_config["csrf_selector"]).get()
                if csrf_token:
                    payload[auth_config.get("csrf_field_name", "_xfToken")] = csrf_token

            headers = {"Referer": login_url, "Content-Type": "application/x-www-form-urlencoded"}
            
            post_url = auth_config.get("post_url") or login_url
            if post_url.startswith("/"):
                base_scheme = urllib.parse.urlparse(current_target_url).scheme
                post_url = f"{base_scheme}://{current_domain}{post_url}"

            auth_resp = await self._request_with_retry("POST", post_url, forum_id, task_id, data=payload, headers=headers)
            
            if auth_resp:
                cookies_dict = session.cookies.get_dict()
                has_auth_cookie = any('user' in k.lower() or 'session' in k.lower() or 'member' in k.lower() for k in cookies_dict.keys())
                if has_auth_cookie or auth_resp.status_code in (302, 303):
                    setattr(session, 'is_authenticated', True)
                    return True
            
            return False

    async def fetch(self, url: str, forum_id: int, auth_config: dict, task_id: int = 0) -> Optional[str]:
        # Передаем url (целевую страницу) в авторизацию, чтобы вычислить зеркало
        is_auth = await self.authenticate_if_needed(forum_id, auth_config, task_id, current_target_url=url)
        if not is_auth:
            return None
            
        resp = await self._request_with_retry("GET", url, forum_id, task_id)
        
        # Если несмотря на смену IP нас забанили на конкретной странице
        if resp and resp.status_code in (403, 429):
            session = self._get_session(forum_id)
            setattr(session, 'is_authenticated', False)
            raise RequestsError(f"HTTP {resp.status_code} on {url} - Auth dropped")
            
        return resp.text if resp else None