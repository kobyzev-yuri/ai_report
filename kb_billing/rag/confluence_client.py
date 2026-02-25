#!/usr/bin/env python3
"""
Клиент Confluence REST API для извлечения страниц в единый формат KB.
Используется для интеграции docs.steccom.ru и других Confluence в базу знаний биллинга/спутниковых систем.
Аутентификация: Bearer (Personal Access Token).
"""
import os
import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


class ConfluenceClient:
    """Клиент к Confluence REST API (Bearer token)."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: int = 30,
    ):
        self.base_url = (base_url or os.getenv("CONFLUENCE_URL", "")).rstrip("/")
        self.token = token or os.getenv("CONFLUENCE_TOKEN", "")
        self.timeout = timeout
        self.session = requests.Session()
        if self.token:
            self.session.headers["Authorization"] = f"Bearer {self.token}"
        self.session.headers.setdefault("Accept", "application/json")
        self.session.headers.setdefault("Content-Type", "application/json")

    def _url(self, path: str) -> str:
        if path.startswith("/"):
            return f"{self.base_url}{path}"
        return f"{self.base_url}/rest/api/content/{path}"

    def check_connection(self) -> Tuple[bool, str]:
        """
        Проверка доступности Confluence и токена.
        Returns:
            (success, message)
        """
        if not self.base_url:
            return False, "CONFLUENCE_URL не задан"
        if not self.token:
            return False, "CONFLUENCE_TOKEN не задан"
        try:
            # Запрос текущего пользователя или корневой страницы
            r = self.session.get(
                f"{self.base_url}/rest/api/user/current",
                timeout=self.timeout,
            )
            if r.status_code == 200:
                return True, "Подключение успешно"
            if r.status_code == 401:
                return False, "Неверный или истёкший токен (401)"
            return False, f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.exceptions.ConnectionError as e:
            return False, f"Ошибка соединения: {e}"
        except requests.exceptions.Timeout:
            return False, "Таймаут соединения"
        except Exception as e:
            return False, str(e)

    def get_spaces(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Список пространств (пространств Confluence)."""
        result = []
        start = 0
        while True:
            r = self.session.get(
                f"{self.base_url}/rest/api/space",
                params={"limit": limit, "start": start},
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            results = data.get("results", [])
            result.extend(results)
            if len(results) < limit:
                break
            start += limit
        return result

    def get_pages_in_space(
        self,
        space_key: str,
        limit: int = 50,
        expand: str = "body.storage,version",
    ) -> Iterator[Dict[str, Any]]:
        """
        Итератор по страницам пространства (все дочерние страницы рекурсивно не обходим —
        только корневые по spaceKey; при необходимости можно добавить CQL).
        """
        start = 0
        while True:
            r = self.session.get(
                f"{self.base_url}/rest/api/content",
                params={
                    "spaceKey": space_key,
                    "type": "page",
                    "limit": limit,
                    "start": start,
                    "expand": expand,
                },
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            results = data.get("results", [])
            for page in results:
                yield page
            if len(results) < limit:
                break
            start += limit

    def get_page_by_id(
        self,
        page_id: str,
        expand: str = "body.storage,version",
    ) -> Dict[str, Any]:
        """Получить страницу по ID с телом в формате storage."""
        r = self.session.get(
            f"{self.base_url}/rest/api/content/{page_id}",
            params={"expand": expand},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def get_page_content_storage(self, page: Dict[str, Any]) -> str:
        """Извлечь тело страницы в формате storage (HTML-подобная разметка)."""
        body = page.get("body") or {}
        storage = body.get("storage") or {}
        return storage.get("value", "")

    def get_page_version_date(self, page: Dict[str, Any]) -> Optional[str]:
        """Дата последнего обновления страницы (version.when)."""
        version = page.get("version") or {}
        return version.get("when")
