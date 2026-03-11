#!/usr/bin/env python3
"""
Общий модуль оркестрации: синхронизация Confluence → KB (JSON), деактивация устаревшего контента.

Используется скриптами автоматического/полуавтоматического обновления и веб-интерфейсом
спутникового библиотекаря. Единая точка входа для:
- загрузки пространств и отдельных страниц по URL/ID;
- дополнения/апдейта существующего контента (merge);
- пометки устаревших page_id (outdated.txt) и удаления документов из JSON.

Пример из скрипта:
  from kb_billing.rag.confluence_sync_runner import sync_spaces, sync_pages, mark_outdated
  sync_spaces(["~a.zhegalov"], limit=50)
  sync_pages(["https://docs.steccom.ru/pages/viewpage.action?pageId=123"], merge=True)
  mark_outdated(["456"])
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _default_output_dir() -> Path:
    """Каталог confluence_docs по умолчанию (рядом с модулем)."""
    return Path(__file__).resolve().parent.parent / "confluence_docs"


def _parse_page_input(lines: List[str]) -> List[Tuple[str, Optional[str]]]:
    """
    Парсинг строк «URL или page_id» с опциональным описанием (таб или 2+ пробела).
    Возвращает список (page_id, engineer_comment).
    """
    parsed: List[Tuple[str, Optional[str]]] = []
    for raw in (lines or []):
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        if "\t" in raw:
            url_part, desc = raw.split("\t", 1)
            url_part, desc = url_part.strip(), (desc or "").strip() or None
        elif re.search(r"  +", raw):
            parts = re.split(r"  +", raw, maxsplit=1)
            url_part = (parts[0] or "").strip()
            desc = (parts[1] or "").strip() or None if len(parts) > 1 else None
        else:
            url_part, desc = raw, None
        if not url_part:
            continue
        if url_part.isdigit():
            parsed.append((url_part, desc))
            continue
        m = re.search(r"pageId=(\d+)", url_part, re.IGNORECASE)
        if m:
            parsed.append((m.group(1), desc))
            continue
        m_att = re.search(r"/download/attachments/(\d+)(?:/|$|\?)", url_part, re.IGNORECASE)
        if m_att:
            parsed.append((m_att.group(1), desc))
            continue
        # Формат /spaces/SPC/pages/4392243/Спецификации или /wiki/spaces/.../pages/4392243/...
        m_pages = re.search(r"/pages/(\d+)(?:/|$|\?)", url_part, re.IGNORECASE)
        if m_pages:
            parsed.append((m_pages.group(1), desc))
            continue
        logger.warning("Не удалось извлечь pageId из строки: %s", url_part[:80])
    # Дедупликация с сохранением порядка
    seen: set = set()
    unique: List[Tuple[str, Optional[str]]] = []
    for pid, desc in parsed:
        if pid not in seen:
            seen.add(pid)
            unique.append((pid, desc))
    return unique


def get_client(
    base_url: Optional[str] = None,
    token: Optional[str] = None,
) -> Any:
    """Создать ConfluenceClient. Параметры из env (CONFLUENCE_URL, CONFLUENCE_TOKEN), если не переданы."""
    from kb_billing.rag.confluence_client import ConfluenceClient
    import os
    return ConfluenceClient(
        base_url=base_url or os.getenv("CONFLUENCE_URL", "").rstrip("/"),
        token=token or os.getenv("CONFLUENCE_TOKEN", ""),
    )


def get_generator(
    client: Optional[Any] = None,
    output_dir: Optional[Path] = None,
) -> Any:
    """Создать ConfluenceKBGenerator. client — опционально, по умолчанию из get_client()."""
    from kb_billing.rag.confluence_kb_generator import ConfluenceKBGenerator
    if client is None:
        client = get_client()
    out = Path(output_dir) if output_dir else _default_output_dir()
    out.mkdir(parents=True, exist_ok=True)
    return ConfluenceKBGenerator(client=client, output_dir=out)


def sync_spaces(
    space_keys: List[str],
    limit: Optional[int] = 50,
    output_dir: Optional[Path] = None,
    client: Optional[Any] = None,
) -> Dict[str, Path]:
    """
    Синхронизация одного или нескольких пространств Confluence в KB.
    Для каждого пространства создаётся файл confluence_{space_key}.json.

    Returns:
        Словарь {space_key: path_to_json}.
    """
    gen = get_generator(client=client, output_dir=output_dir)
    result: Dict[str, Path] = {}
    for key in space_keys:
        key = (key or "").strip()
        if not key:
            continue
        try:
            docs = gen.sync_space(key, limit=limit)
            result[key] = gen.output_dir / f"confluence_{key}.json"
            logger.info("Пространство %s: сохранено %s документов в %s", key, len(docs), result[key])
        except Exception as e:
            logger.exception("Ошибка синхронизации пространства %s: %s", key, e)
    return result


def sync_all_spaces(
    limit_per_space: Optional[int] = 50,
    output_dir: Optional[Path] = None,
    client: Optional[Any] = None,
    exclude_keys: Optional[List[str]] = None,
    include_only_personal: bool = False,
) -> Dict[str, Path]:
    """
    Синхронизация всех пространств Confluence в KB (по списку из API).
    Для каждого пространства создаётся файл confluence_{space_key}.json.

    Args:
        limit_per_space: макс. страниц на пространство (None — без ограничения).
        output_dir: каталог confluence_docs.
        client: ConfluenceClient (по умолчанию из get_client()).
        exclude_keys: ключи пространств, которые пропускать (например ["DEMO", "TEST"]).
        include_only_personal: если True — синхронизировать только личные пространства (type=personal).

    Returns:
        Словарь {space_key: path_to_json}.
    """
    if client is None:
        client = get_client()
    spaces = client.get_spaces(limit=100)
    exclude_set = {k.strip().lower() for k in (exclude_keys or []) if k}
    space_keys: List[str] = []
    for s in spaces:
        key = (s.get("key") or "").strip()
        if not key:
            continue
        if key.lower() in exclude_set:
            continue
        if include_only_personal:
            if (s.get("type") or "").strip().lower() != "personal":
                continue
        space_keys.append(key)
    if not space_keys:
        logger.warning("Нет пространств для синхронизации (список из API пуст или всё отфильтровано)")
        return {}
    return sync_spaces(
        space_keys=space_keys,
        limit=limit_per_space,
        output_dir=output_dir,
        client=client,
    )


def _expand_with_children(
    parsed: List[Tuple[str, Optional[str]]],
    client: Any,
    max_depth: int = 1,
    limit_children: int = 200,
) -> List[Tuple[str, Optional[str]]]:
    """
    Развернуть список (page_id, comment), добавив дочерние страницы рекурсивно (через API child/page).
    Порядок: сначала корни, затем их дети по уровням. Ограничение по глубине и общему числу детей.
    """
    if max_depth <= 0:
        return parsed
    expanded: List[Tuple[str, Optional[str]]] = []
    seen: set = set()
    to_expand = list(parsed)
    total_children = 0
    for depth in range(max_depth + 1):
        next_level: List[Tuple[str, Optional[str]]] = []
        for page_id, comment in to_expand:
            if page_id in seen:
                continue
            seen.add(page_id)
            expanded.append((page_id, comment))
            if depth >= max_depth or total_children >= limit_children:
                continue
            try:
                child_ids = client.get_child_page_ids(page_id, limit=limit_children - total_children)
                for cid in child_ids:
                    if cid not in seen:
                        next_level.append((cid, None))
                        total_children += 1
                        if total_children >= limit_children:
                            break
            except Exception as e:
                logger.warning("Ошибка получения дочерних страниц %s: %s", page_id, e)
            if total_children >= limit_children:
                break
        to_expand = next_level
        if not to_expand:
            break
    return expanded


def sync_pages(
    urls_or_ids: List[str],
    output_dir: Optional[Path] = None,
    output_suffix: str = "custom_pages",
    merge: bool = False,
    client: Optional[Any] = None,
    include_children: bool = False,
    max_depth: int = 1,
    limit_children: int = 200,
) -> Tuple[Path, int]:
    """
    Синхронизация выбранных страниц по URL или page_id.
    Каждая строка может быть: «page_id», «URL» или «URL  описание» / «URL\tописание».

    Если include_children=True, для каждой страницы через API запрашиваются дочерние (child/page)
    и они тоже синхронизируются (рекурсивно до max_depth уровней, не более limit_children дочерних).

    Если merge=True, существующий файл confluence_{output_suffix}.json загружается,
    документы по переданным page_id обновляются или добавляются, остальные сохраняются.

    Returns:
        (path_to_json, number_of_docs_after_save).
    """
    gen = get_generator(client=client, output_dir=output_dir)
    if client is None:
        client = gen.client
    parsed = _parse_page_input(urls_or_ids)
    if include_children and parsed:
        parsed = _expand_with_children(parsed, client, max_depth=max_depth, limit_children=limit_children)
        logger.info("С учётом дочерних страниц: %s страниц к синхронизации", len(parsed))
    if not parsed:
        out_file = gen.output_dir / f"confluence_{output_suffix}.json"
        if merge and out_file.exists():
            try:
                with open(out_file, "r", encoding="utf-8") as f:
                    docs = json.load(f)
                return out_file, len(docs) if isinstance(docs, list) else 0
            except Exception:
                return out_file, 0
        return out_file, 0

    base_url = gen.client.base_url
    existing_docs: List[Dict[str, Any]] = []
    existing_by_id: Dict[str, int] = {}
    out_file = gen.output_dir / f"confluence_{output_suffix}.json"
    if merge and out_file.exists():
        try:
            with open(out_file, "r", encoding="utf-8") as f:
                existing_docs = json.load(f)
            if isinstance(existing_docs, list):
                existing_by_id = {str((d.get("source") or {}).get("page_id", "")): i for i, d in enumerate(existing_docs)}
        except Exception as e:
            logger.warning("Не удалось загрузить существующий файл для merge: %s", e)

    for page_id, engineer_comment in parsed:
        try:
            page = gen.client.get_page_by_id(page_id)
            doc = gen.page_to_kb_doc(page, base_url)
            if engineer_comment:
                (doc.setdefault("source", {}))["engineer_comment"] = engineer_comment
            if page_id in existing_by_id:
                existing_docs[existing_by_id[page_id]] = doc
            else:
                existing_by_id[page_id] = len(existing_docs)
                existing_docs.append(doc)
        except Exception as e:
            logger.warning("Ошибка обработки страницы %s: %s", page_id, e)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(existing_docs, f, ensure_ascii=False, indent=2)
    logger.info("Сохранено %s документов в %s", len(existing_docs), out_file)
    return out_file, len(existing_docs)


def mark_outdated(
    page_ids: List[str],
    output_dir: Optional[Path] = None,
    client: Optional[Any] = None,
) -> int:
    """
    Добавить page_id в список устаревших (outdated.txt). При перезагрузке KB в Qdrant эти страницы не попадут в поиск.

    Returns:
        Количество добавленных id (без дубликатов).
    """
    gen = get_generator(client=client, output_dir=output_dir)
    path = gen.get_outdated_path()
    before = len(gen.read_outdated_ids())
    for pid in page_ids:
        pid = (pid or "").strip()
        if pid and pid.isdigit():
            gen.add_to_outdated(pid)
    return len(gen.read_outdated_ids()) - before


def unmark_outdated(
    page_ids: List[str],
    output_dir: Optional[Path] = None,
    client: Optional[Any] = None,
) -> int:
    """Убрать page_id из списка устаревших (outdated.txt)."""
    gen = get_generator(client=client, output_dir=output_dir)
    for pid in page_ids:
        pid = (pid or "").strip()
        if pid:
            gen.remove_from_outdated(pid)
    return 0


def remove_pages_from_kb(
    page_ids: List[str],
    output_dir: Optional[Path] = None,
    also_mark_outdated: bool = True,
    client: Optional[Any] = None,
) -> int:
    """
    Удалить документы с указанными page_id из всех JSON в каталоге KB и опционально добавить их в outdated.txt.

    Returns:
        Количество удалённых документов (из JSON).
    """
    out = Path(output_dir) if output_dir else _default_output_dir()
    if not out.exists():
        return 0
    ids_to_remove = {str(pid).strip() for pid in (page_ids or []) if str(pid).strip().isdigit()}
    if not ids_to_remove:
        return 0
    removed = 0
    for json_file in out.glob("*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                docs = json.load(f)
        except Exception as e:
            logger.warning("Не удалось прочитать %s: %s", json_file, e)
            continue
        if not isinstance(docs, list):
            continue
        new_docs = [d for d in docs if str((d.get("source") or {}).get("page_id", "")) not in ids_to_remove]
        if len(new_docs) != len(docs):
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(new_docs, f, ensure_ascii=False, indent=2)
            removed += len(docs) - len(new_docs)
            logger.info("Из %s удалено документов: %s", json_file.name, len(docs) - len(new_docs))
    if also_mark_outdated and removed:
        gen = get_generator(client=client, output_dir=output_dir)
        for pid in ids_to_remove:
            gen.add_to_outdated(pid)
    return removed


def update_pages_in_kb(
    page_ids: List[str],
    output_dir: Optional[Path] = None,
    client: Optional[Any] = None,
) -> int:
    """
    Обновить документы по списку page_id: заново забрать из Confluence и перезаписать в существующих JSON.

    Returns:
        Количество обновлённых документов.
    """
    gen = get_generator(client=client, output_dir=output_dir)
    ids = [str(pid).strip() for pid in (page_ids or []) if str(pid).strip().isdigit()]
    return gen.update_docs_by_page_ids(ids) if ids else 0
