import parsel
import structlog
import hashlib
import re
from typing import Dict, Any, List, Optional
from schemas import ParserTemplate

logger = structlog.get_logger(__name__)

class UniversalExtractor:
    def __init__(self):
        # СИГНАТУРЫ ДВИЖКОВ
        self.engines = {
            "xenforo": {
                "fingerprint": "html#XF",
                "post_container": ".message, .message-cell",
                "author": ".message-name, .username, .userText",
                "content": ".message-userContent, .message-content, .bbWrapper",
                "date": ".u-dt, .message-date",
                "link_pattern": r"(threads/|/t/|\.\d+/)"
            },
            "vbulletin": {
                "fingerprint": "meta[name='generator'][content*='vBulletin']",
                "post_container": ".postcontainer, .postbit",
                "author": ".username, .postuser",
                "content": ".postcontent, .post_message",
                "date": ".postdate",
                "link_pattern": r"(showthread\.php\?t=|-t\d+\.html)"
            },
            "ipb": {
                "fingerprint": "body#ipboard_body, meta[content*='Invision Power Board']",
                "post_container": ".post_block, .cPost",
                "author": ".author, .cAuthorPane_author",
                "content": ".post_body, .cPost_contentWrap",
                "date": ".posted_info, .ipsType_light",
                "link_pattern": r"(topic/)"
            }
        }

    def detect_engine(self, selector: parsel.Selector) -> str:
        for engine, data in self.engines.items():
            if selector.css(data["fingerprint"]):
                return engine
        return "unknown"

    @staticmethod
    def _query(sel_obj, query: str, engine: str):
        if not query: 
            return [sel_obj]
        if engine == "xpath" or (engine == "auto" and ("//" in query or "@" in query)): 
            return sel_obj.xpath(query)
        return sel_obj.css(query)

    def extract_links(self, html: str, config: ParserTemplate) -> List[Dict[str, Any]]:
        selector = parsel.Selector(text=html)
        results = []

        # КАСКАД 1: СТРОГИЙ КОНФИГ
        if config.link_extraction and config.link_extraction.link_selector:
            logger.debug("Using STRICT link extraction", marker="[EXTRACT_STRICT]")
            containers = self._query(selector, config.link_extraction.container_selector, config.selector_engine) if config.link_extraction.container_selector else [selector]
            for container in containers:
                links = self._query(container, config.link_extraction.link_selector, config.selector_engine)
                for link in links:
                    raw_url = link.xpath(f"@{config.link_extraction.url_attribute}").get()
                    if raw_url:
                        anchor = " ".join([t.strip() for t in link.xpath(".//text()").getall() if t.strip()])
                        results.append({"raw_url": raw_url, "anchor_text": anchor[:100], "context_snippet": None})
            return results

        # КАСКАД 2 & 3: ЭВРИСТИКА
        engine = self.detect_engine(selector)
        logger.info(f"Using HEURISTIC link extraction. Engine: {engine}", marker="[EXTRACT_HEURISTIC]")
        
        valid_pattern = self.engines[engine]["link_pattern"] if engine != "unknown" else r"(thread|topic|show|\.\d+/)"
        trash_pattern = r"(login|member|register|search|post|reply|attachment|#)"

        for a_tag in selector.xpath("//a[@href]"):
            href = a_tag.attrib.get('href', '')
            if not href or href.startswith('javascript:'): continue
                
            href_lower = href.lower()
            if re.search(valid_pattern, href_lower) and not re.search(trash_pattern, href_lower):
                anchor = " ".join([t.strip() for t in a_tag.xpath(".//text()").getall() if t.strip()])
                if anchor and len(anchor) > 3:
                    results.append({"raw_url": href, "anchor_text": anchor[:100], "context_snippet": f"Detected via {engine} heuristic"})
        
        seen = set()
        unique = []
        for r in results:
            if r['raw_url'] not in seen:
                seen.add(r['raw_url'])
                unique.append(r)
        return unique

    def extract_posts(self, html: str, config: ParserTemplate, thread_url: str) -> List[Dict[str, Any]]:
        selector = parsel.Selector(text=html)
        results = []

        if config.post_extraction and config.post_extraction.post_container_selector:
            containers = self._query(selector, config.post_extraction.post_container_selector, config.selector_engine)
            for container in containers:
                author_elem = self._query(container, config.post_extraction.author_selector, config.selector_engine) if config.post_extraction.author_selector else []
                author = " ".join([t.strip() for t in author_elem.xpath(".//text()").getall() if t.strip()]) if author_elem else "Unknown"
                
                content_elem = self._query(container, config.post_extraction.content_selector, config.selector_engine) if config.post_extraction.content_selector else [container]
                content = content_elem[0].get() if content_elem else ""
                
                date_elem = self._query(container, config.post_extraction.date_selector, config.selector_engine) if config.post_extraction.date_selector else []
                published_at = " ".join([t.strip() for t in date_elem.xpath(".//text()").getall() if t.strip()]) if date_elem else None

                raw_hash = f"{thread_url}_{author}_{content}_{published_at}"
                results.append({
                    "post_hash": hashlib.sha256(raw_hash.encode('utf-8')).hexdigest(),
                    "author": author[:100],
                    "content": content,
                    "published_at": published_at[:50] if published_at else None
                })
            return results

        engine = self.detect_engine(selector)
        if engine != "unknown":
            containers = selector.css(self.engines[engine]["post_container"])
            for container in containers:
                author_elem = container.css(self.engines[engine]["author"])
                author = " ".join(author_elem.xpath(".//text()").getall()).strip() or "Unknown"
                
                content_elem = container.css(self.engines[engine]["content"])
                content = content_elem.get() or container.get()
                
                raw_hash = f"{thread_url}_{author}_{content}"
                results.append({
                    "post_hash": hashlib.sha256(raw_hash.encode('utf-8')).hexdigest(),
                    "author": author[:100], "content": content, "published_at": "Heuristic extracted"
                })
        else:
            containers = selector.css("article, div[class*='message'], div[class*='post'], .content")
            if not containers:
                containers = [selector.css("body")]
            
            for container in containers:
                content = container.get()
                raw_hash = f"{thread_url}_heuristic_{content}"
                results.append({
                    "post_hash": hashlib.sha256(raw_hash.encode('utf-8')).hexdigest(),
                    "author": "System (Heuristic)", 
                    "content": content, 
                    "published_at": "Auto"
                })
        return results

    def extract_next_page(self, html: str, config: ParserTemplate) -> Optional[str]:
        selector = parsel.Selector(text=html)
        if config.pagination and config.pagination.next_page_selector:
            next_elem = self._query(selector, config.pagination.next_page_selector, config.selector_engine)
            if next_elem:
                return next_elem[0].xpath(f"@{config.pagination.url_attribute}").get()
        
        for a_tag in selector.xpath("//a[@href]"):
            text = " ".join(a_tag.xpath(".//text()").getall()).strip().lower()
            if text in ["next", "вперед", "вперёд", "далее", ">", ">>"]:
                return a_tag.attrib.get('href')
        return None