"""Загрузка SentenceTransformer: офлайн из кэша на серверах без интернета (vz2)."""
from __future__ import annotations

import logging
import os

from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


def _truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def load_sentence_transformer(model_name: str) -> SentenceTransformer:
    """Загрузить модель эмбеддингов; при HF_HUB_OFFLINE или сбое сети — только локальный кэш."""
    if _truthy("HF_HUB_OFFLINE") or _truthy("TRANSFORMERS_OFFLINE"):
        os.environ.setdefault("HF_HUB_OFFLINE", "1")
        os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
        logger.info("HF offline: loading %s from local cache", model_name)
        return SentenceTransformer(model_name, local_files_only=True)

    try:
        return SentenceTransformer(model_name)
    except Exception as exc:
        logger.warning("Online load failed for %s (%s), retrying local_files_only", model_name, exc)
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        return SentenceTransformer(model_name, local_files_only=True)
