from typing import Optional, Literal, Dict
from pydantic import BaseModel, Field

class AuthConfig(BaseModel):
    enabled: bool = False
    login_url: Optional[str] = None
    post_url: Optional[str] = None
    username_field: Optional[str] = None
    password_field: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    csrf_selector: Optional[str] = None
    csrf_field_name: Optional[str] = None
    cookies: Optional[Dict[str, str]] = None

class LinkExtractionConfig(BaseModel):
    """Конфигурация для извлечения ссылок на темы из разделов"""
    container_selector: Optional[str] = None
    link_selector: Optional[str] = None
    anchor_selector: Optional[str] = None
    context_selector: Optional[str] = None
    url_attribute: str = "href"

class PostExtractionConfig(BaseModel):
    """Конфигурация для извлечения сообщений изнутри тем (Deep Crawl)"""
    post_container_selector: Optional[str] = None
    author_selector: Optional[str] = None
    content_selector: Optional[str] = None
    date_selector: Optional[str] = None

class PaginationConfig(BaseModel):
    """Конфигурация для перехода по страницам"""
    next_page_selector: Optional[str] = None
    url_attribute: str = "href"

class ParserTemplate(BaseModel):
    """
    Основной шаблон конфигурации парсера для конкретного форума.
    Поддерживает как поверхностный сбор ссылок, так и глубокий парсинг постов.
    """
    selector_engine: Literal["auto", "css", "xpath"] = "auto"
    
    # Модули экстракции
    link_extraction: LinkExtractionConfig = Field(default_factory=LinkExtractionConfig)
    post_extraction: PostExtractionConfig = Field(default_factory=PostExtractionConfig)
    pagination: PaginationConfig = Field(default_factory=PaginationConfig)
    
    # Авторизация
    auth: AuthConfig = Field(default_factory=AuthConfig)