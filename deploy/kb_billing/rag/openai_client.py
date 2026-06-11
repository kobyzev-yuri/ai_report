"""OpenAI-клиент с HTTP-прокси для серверов без прямого интернета (vz2)."""
from __future__ import annotations

import os
from typing import Any, Optional


def _api_proxy() -> Optional[str]:
    return (
        os.getenv("OPENAI_HTTP_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
    )


def _api_base_url(explicit: Optional[str] = None) -> Optional[str]:
    if explicit:
        return explicit
    return os.getenv("OPENAI_API_BASE") or os.getenv("OPENAI_BASE_URL")


def build_openai_client(
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    timeout: float = 120.0,
) -> Any:
    """Создать OpenAI client; на vz2 — через apt-proxy-tunnel (127.0.0.1:3128)."""
    import httpx
    from openai import OpenAI

    key = api_key or os.getenv("OPENAI_API_KEY")
    if not key:
        raise ValueError("OPENAI_API_KEY не установлен")

    kwargs: dict[str, Any] = {"api_key": key}
    base_url = _api_base_url(api_base)
    if base_url:
        kwargs["base_url"] = base_url

    proxy = _api_proxy()
    if proxy:
        kwargs["http_client"] = httpx.Client(proxy=proxy, timeout=timeout)

    return OpenAI(**kwargs)
