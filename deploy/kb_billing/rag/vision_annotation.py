#!/usr/bin/env python3
"""
Аннотация изображений и схем через Gemini Vision (ProxyAPI.ru), как в brats.

Используется при индексации Confluence: картинки и схемы сети получают
текстовое описание и смысловые блоки для поиска в KB.

Переменные окружения (config.env):
- GEMINI_API_KEY или OPENAI_API_KEY (ключ ProxyAPI)
- GEMINI_BASE_URL (по умолчанию https://api.proxyapi.ru/google)
- GEMINI_VISION_MODEL или GEMINI_MODEL (например gemini-2.0-flash)
- USE_GEMINI_FOR_IMAGES=true — включать аннотацию изображений при индексации (если не задано, аннотация всё равно пробуется при наличии ключа)
"""
import base64
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:
    import httpx
except ImportError:
    httpx = None

# Подгрузка config.env из корня проекта при импорте
def _load_config():
    try:
        from dotenv import load_dotenv
        for d in [Path(__file__).resolve().parent.parent.parent, Path(__file__).resolve().parent.parent]:
            env_file = d / "config.env"
            if env_file.exists():
                load_dotenv(dotenv_path=env_file, override=False)
                break
    except Exception:
        pass
_load_config()


def describe_image(
    data: bytes,
    mime_type: str = "image/png",
    filename: str = "",
    context_text: Optional[str] = None,
    max_tokens: int = 2048,
) -> Optional[str]:
    """
    Описание одного изображения/схемы через Gemini Vision (REST, ProxyAPI).
    Возвращает текст с описанием и смысловыми блоками или None при ошибке/отсутствии ключа.
    """
    if not data:
        return None
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    if not httpx:
        logger.warning("httpx не установлен — аннотация изображений недоступна")
        return None
    base_url = (os.getenv("GEMINI_BASE_URL") or "https://api.proxyapi.ru/google").rstrip("/")
    model = os.getenv("GEMINI_VISION_MODEL") or os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    if os.getenv("USE_GEMINI_FOR_IMAGES", "true").lower() in ("0", "false", "no"):
        return None
    prompt_parts = [
        "Ты помогаешь инженерам спутникового сегмента описывать изображения для базы знаний.",
        "На изображении может быть: схема сети (VSAT, терминалы, антенны, модемы, iDirect, Kingsat и т.д.), диаграмма, скриншот конфигурации или инструкция.",
        "Дай структурированное описание на русском языке:",
        "1) Краткое общее описание (1–2 предложения).",
        "2) Смысловые блоки: перечисли элементы, компоненты, подписи, связи (если схема), ключевые параметры.",
        "Без markdown-разметки, обычный текст. Если текст на изображении читаем — включи его в описание.",
    ]
    if context_text:
        prompt_parts.insert(1, f"Контекст (страница/документ): {context_text[:800]}")
    if filename:
        prompt_parts.append(f"Имя файла: {filename}")
    text_part = {"text": "\n".join(prompt_parts)}
    b64 = base64.b64encode(data).decode("utf-8")
    inline_part = {"inline_data": {"mime_type": mime_type, "data": b64}}
    request_body: Dict[str, Any] = {
        "contents": [{"parts": [text_part, inline_part]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": max_tokens,
        },
    }
    endpoint = f"/v1beta/models/{model}:generateContent"
    try:
        with httpx.Client(
            base_url=base_url,
            timeout=120.0,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        ) as client:
            response = client.post(endpoint, json=request_body)
            response.raise_for_status()
            resp_data = response.json()
    except Exception as e:
        logger.warning("Gemini Vision аннотация изображения не удалась: %s", e)
        return None
    for cand in resp_data.get("candidates", []):
        for part in cand.get("content", {}).get("parts", []):
            if "text" in part:
                text = (part.get("text") or "").strip()
                if text:
                    return text
    return None


def is_vision_available() -> bool:
    """Проверка: настроен ли ключ и можно ли вызывать аннотацию изображений."""
    return bool(os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")) and bool(httpx)


def _gemini_generate_text(prompt: str, max_tokens: int = 2048) -> Optional[str]:
    """Один текстовый запрос к Gemini (generateContent, только text)."""
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key or not httpx:
        return None
    base_url = (os.getenv("GEMINI_BASE_URL") or "https://api.proxyapi.ru/google").rstrip("/")
    model = os.getenv("GEMINI_VISION_MODEL") or os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    request_body: Dict[str, Any] = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": max_tokens},
    }
    endpoint = f"/v1beta/models/{model}:generateContent"
    try:
        with httpx.Client(
            base_url=base_url,
            timeout=120.0,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        ) as client:
            response = client.post(endpoint, json=request_body)
            response.raise_for_status()
            resp_data = response.json()
    except Exception as e:
        logger.warning("Gemini text запрос не удался: %s", e)
        return None
    for cand in resp_data.get("candidates", []):
        for part in cand.get("content", {}).get("parts", []):
            if "text" in part:
                text = (part.get("text") or "").strip()
                if text:
                    return text
    return None


def describe_drawio_content(raw_drawio_text: str, context_text: Optional[str] = None) -> Optional[str]:
    """
    По извлечённому из draw.io тексту (подписи, узлы, связи) сформировать
    интеллектуальное описание схемы и смысловые блоки через Gemini.
    """
    if not (raw_drawio_text or "").strip():
        return None
    prompt_parts = [
        "Ниже приведён извлечённый из файла draw.io (схема/диаграмма) текст: подписи узлов, блоков, связей.",
        "Ты помогаешь инженерам спутникового сегмента. Сформируй на русском языке:",
        "1) Краткое общее описание схемы (что изображено, тип диаграммы).",
        "2) Смысловые блоки: перечисли основные элементы, компоненты, связи, ключевые подписи.",
        "Без markdown, обычный текст. Используй только информацию из приведённого текста.",
    ]
    if context_text:
        prompt_parts.insert(1, f"Контекст (страница/документ): {context_text[:500]}")
    prompt_parts.append("\nТекст из draw.io:\n" + (raw_drawio_text[:12000] or ""))
    return _gemini_generate_text("\n".join(prompt_parts), max_tokens=2048)
