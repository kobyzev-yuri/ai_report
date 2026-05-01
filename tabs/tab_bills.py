"""
Закладка: Загрузка счетов из 1С (директория bills)
"""
from __future__ import annotations

import streamlit as st
from pathlib import Path
from datetime import datetime
import os
import shutil
import subprocess
import tempfile
import zipfile
import io


def _get_project_root() -> Path:
    """
    Определить корень проекта (где лежит директория tabs/).
    Работает как в корне ai_report, так и внутри deploy.
    """
    script_path = Path(__file__).resolve()
    current = script_path
    while current.parent != current:
        if (current / "tabs").exists():
            return current
        current = current.parent
    # Fallback: директория файла
    return script_path.parent


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _count_files_in_tree(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(len(files) for _, _, files in os.walk(root))


def _dir_file_stats(root: Path) -> tuple[int, float | None]:
    """Число файлов в поддереве и mtime самого нового файла."""
    if not root.exists():
        return 0, None
    file_count = 0
    latest: float | None = None
    for walk_root, _, files in os.walk(root):
        for name in files:
            fp = Path(walk_root, name)
            try:
                st_res = fp.stat()
            except OSError:
                continue
            file_count += 1
            if latest is None or st_res.st_mtime > latest:
                latest = st_res.st_mtime
    return file_count, latest


def _merge_move_into(src: Path, dest_parent: Path) -> None:
    """
    Переместить файл или каталог src в dest_parent.
    При совпадении имён: два каталога — слить вложенное; два файла — заменить целевой.
    """
    dest = dest_parent / src.name
    if not dest.exists():
        shutil.move(str(src), str(dest))
        return
    if src.is_dir() and dest.is_dir():
        for child in list(src.iterdir()):
            _merge_move_into(child, dest)
        src.rmdir()
        return
    if src.is_file() and dest.is_file():
        dest.unlink()
        shutil.move(str(src), str(dest))
        return
    raise RuntimeError(
        f"Конфликт при переносе «{src.name}»: в `{dest_parent}` уже есть элемент с тем же именем."
    )


def _flatten_single_bills_wrapper(extracted: Path, target_dir: Path) -> bool:
    """
    Если в extracted ровно одна подпапка «bills», переносит её содержимое прямо в target_dir
    (чтобы не получалось bills/bills/… при symlink bills → NAS).
    Возвращает True, если обёртку сняли.
    """
    if not extracted.is_dir():
        return False
    skip = {".DS_Store", "Thumbs.db"}
    kids = [p for p in extracted.iterdir() if p.name not in skip]
    if len(kids) != 1:
        return False
    only = kids[0]
    if not only.is_dir() or only.name.lower() != "bills":
        return False
    for c in list(only.iterdir()):
        _merge_move_into(c, target_dir)
    only.rmdir()
    return True


def _move_archive_tree_to_bills(extracted: Path, target_dir: Path) -> None:
    """
    Перенос содержимого распаковки в целевой bills: сначала снимаем единственную обёртку bills/,
    иначе — все верхнеуровневые элементы архива в target_dir (папки контрагентов с корня архива).
    """
    if _flatten_single_bills_wrapper(extracted, target_dir):
        return
    for c in list(extracted.iterdir()):
        if c.name in (".DS_Store", "Thumbs.db"):
            continue
        _merge_move_into(c, target_dir)


def _extract_7z_archive(data: bytes, filename: str, target_dir: Path) -> str:
    """
    Распаковать 7z в target_dir (с учётом обёртки bills/ и symlink на NAS).
    """
    if not shutil.which("7z"):
        raise RuntimeError(
            "Для .7z нужна утилита `7z` (пакет p7zip-full / p7zip). Установите её на сервере."
        )

    work = Path(tempfile.mkdtemp(prefix="bills7z_"))
    extract_root = work / "out"
    arc_path = work / "upload.7z"
    try:
        _ensure_dir(extract_root)
        arc_path.write_bytes(data)
        r = subprocess.run(
            ["7z", "x", f"-o{extract_root}", "-y", str(arc_path)],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            err = (r.stderr or r.stdout or "").strip() or f"код {r.returncode}"
            raise RuntimeError(f"7z: {err[:800]}")

        n_files = _count_files_in_tree(extract_root)
        _move_archive_tree_to_bills(extract_root, target_dir)

        # Убрать пустой каталог распаковки, если остался пустым
        try:
            if extract_root.exists() and not any(extract_root.iterdir()):
                extract_root.rmdir()
        except OSError:
            pass

        return (
            f"7Z `{filename}` → содержимое перенесено в `{target_dir.resolve()}` "
            f"(обёртка «bills» снята при необходимости; файлов из архива: {n_files})"
        )
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _extract_rar_archive(data: bytes, filename: str, target_dir: Path) -> str:
    """
    Распаковать RAR в target_dir/<имя_без_rar>/.
    Пробует: unrar → 7z → unar → библиотеку rarfile (нужен UnRAR в системе).
    """
    stem = Path(filename).stem
    subdir_name = stem or f"rar_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    out_dir = target_dir / subdir_name
    if out_dir.exists():
        shutil.rmtree(out_dir)
    _ensure_dir(out_dir)

    with tempfile.NamedTemporaryFile(suffix=".rar", delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    errors: list[str] = []
    try:
        cmd_tries: list[tuple[str, list[str]]] = []
        if shutil.which("unrar"):
            cmd_tries.append(
                ("unrar", ["unrar", "x", "-o+", tmp_path, str(out_dir) + os.sep])
            )
        if shutil.which("7z"):
            cmd_tries.append(
                ("7z", ["7z", "x", f"-o{out_dir}", "-y", tmp_path])
            )
        if shutil.which("unar"):
            cmd_tries.append(
                ("unar", ["unar", "-o", str(out_dir), "-f", tmp_path])
            )

        for label, cmd in cmd_tries:
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.returncode == 0:
                n = _count_files_in_tree(out_dir)
                return f"RAR `{filename}` → распакован в `{out_dir}` ({n} файлов, {label})"
            err = (r.stderr or r.stdout or "").strip() or f"код выхода {r.returncode}"
            errors.append(f"{label}: {err[:500]}")

        try:
            import rarfile

            with rarfile.RarFile(tmp_path) as rf:
                rf.extractall(out_dir)
            n = _count_files_in_tree(out_dir)
            return f"RAR `{filename}` → распакован в `{out_dir}` ({n} файлов, rarfile)"
        except ImportError:
            errors.append("rarfile: пакет не установлен (pip install rarfile)")
        except Exception as e:
            errors.append(f"rarfile: {e}")

        raise RuntimeError(
            "Не удалось распаковать RAR. Установите на сервере один из инструментов: "
            "`unrar` (пакет unrar или unrar-free), `p7zip-full` (команда 7z), `unar` "
            "или `pip install rarfile` вместе с бинарником UnRAR. "
            + (" Подробности: " + " | ".join(errors) if errors else "")
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _save_uploaded_file(file, target_dir: Path) -> str:
    """
    Сохранить один загруженный объект:
    - ZIP / 7z / RAR — распаковать (7z и ZIP с одной корневой «bills/» — в корень целевого bills)
    - иначе сохранить как обычный файл
    Возвращает текстовый отчет.
    """
    _ensure_dir(target_dir)

    filename = Path(file.name).name
    data = file.read()

    lower = filename.lower()

    # RAR: внешний unrar / 7z / unar или rarfile
    if lower.endswith(".rar"):
        return _extract_rar_archive(data, filename, target_dir)

    # 7-Zip: как у выгрузки 1С — часто корневая папка «bills»; переносим контрагентов в целевой bills
    if lower.endswith(".7z"):
        return _extract_7z_archive(data, filename, target_dir)

    # ZIP-архив: распаковываем, сохраняя структуру директорий
    if lower.endswith(".zip"):
        subdir_name = filename[:-4] or f"zip_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        extract_dir = target_dir / subdir_name
        _ensure_dir(extract_dir)

        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(extract_dir)

        n_from_zip = _count_files_in_tree(extract_dir)
        # Если внутри архива только bills/… — переносим контрагентов в корень bills (без лишней вложенности)
        if _flatten_single_bills_wrapper(extract_dir, target_dir):
            try:
                if extract_dir.exists() and not any(extract_dir.iterdir()):
                    extract_dir.rmdir()
            except OSError:
                pass
            return (
                f"ZIP `{filename}` → папки контрагентов в `{target_dir.resolve()}` "
                f"(снята обёртка «bills»; файлов из архива: {n_from_zip})"
            )

        file_count = _count_files_in_tree(extract_dir)
        return f"ZIP `{filename}` → распакован в `{extract_dir}` ({file_count} файлов)"

    # Обычный файл (PDF/HTML/и т.п.)
    save_path = target_dir / filename
    if save_path.exists():
        # Простая защита от перезаписи: добавим timestamp
        stem = save_path.stem
        suffix = save_path.suffix
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = save_path.with_name(f"{stem}_{ts}{suffix}")

    with open(save_path, "wb") as f:
        f.write(data)

    return f"Файл `{filename}` → сохранён как `{save_path.name}`"


def show_tab():
    """
    Закладка для оператора: перенос счетов из 1С в директорию bills.
    """
    st.header("📨 Счета 1С для рассылки клиентам")
    st.markdown(
        """
        Эта вкладка позволяет оператору перенести **пакет счетов из 1С** в директорию `bills`,
        откуда их дальше забирает система рассылки.

        **Как использовать:** сформируйте в 1С папку со счетами и актами (PDF/HTML и т.п.), упакуйте в **ZIP**, **7z** или **RAR**
        и загрузите архив. **7z** и **ZIP** с единственной корневой папкой `bills/` раскладываются сразу в `bills` (папки контрагентов без лишнего `bills/bills`). Для RAR на сервере нужен `unrar`, `7z`, `unar` или пакет `rarfile` + UnRAR; для **.7z** — команда `7z` (пакет `p7zip-full`).
        После работы с рассылкой можно **очистить папку** `bills`.
        """
    )

    project_root = _get_project_root()
    bills_root = project_root / "bills"
    _ensure_dir(bills_root)

    st.markdown("---")
    st.subheader("📁 Целевая директория на сервере")
    st.code(str(bills_root), language="bash")

    # Для простоты всё кладём прямо в `bills/` без дополнительных подкаталогов
    target_dir = bills_root

    st.markdown("---")
    st.subheader("📤 Перенос счетов (загрузка файлов)")

    uploaded_files = st.file_uploader(
        "Загрузите ZIP / 7z / RAR со счетами (или несколько архивов и файлов)",
        accept_multiple_files=True,
        type=None,
        help="ZIP/7z/RAR: распаковка в bills; для архива с одной корневой папкой «bills» содержимое попадает прямо в bills. Остальные файлы сохраняются как есть.",
        key="bills_uploader",
    )

    if uploaded_files:
        if st.button("💾 Сохранить загруженные файлы в bills", type="primary", use_container_width=True):
            reports = []
            with st.spinner("Сохранение файлов..."):
                for f in uploaded_files:
                    try:
                        report = _save_uploaded_file(f, target_dir)
                        reports.append(f"✅ {report}")
                    except Exception as e:
                        reports.append(f"❌ Ошибка для `{f.name}`: {e}")

            st.markdown("### Результат сохранения")
            for line in reports:
                st.write(line)

    st.markdown("---")
    st.subheader("📊 Текущее содержимое директории bills")

    # Только папки верхнего уровня (контрагенты 0000…); без обрезки «:50» и без смеси всех вложенных путей.
    top_dirs = sorted(
        [p for p in bills_root.iterdir() if p.is_dir()],
        key=lambda p: p.name.lower(),
    )
    options: list[str] = []

    if not top_dirs:
        st.info("Папка `bills` пуста. Загрузите ZIP или RAR со счетами — архив распакуется сюда.")
        # Показываем блок «Очистить всю папку» ниже (пустая папка — нечего очищать)
    else:
        rows = []
        options = []
        for d in top_dirs:
            file_count, latest_mtime = _dir_file_stats(d)
            rows.append(
                {
                    "Папка (верхний уровень)": d.name,
                    "Файлов (включая вложенные)": file_count,
                    "Последнее изменение файла": datetime.fromtimestamp(latest_mtime).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if latest_mtime
                    else "-",
                }
            )
            options.append(d.name)

        import pandas as pd

        st.caption(
            f"Папок верхнего уровня: **{len(top_dirs)}** "
            "(подпапки счетов внутри контрагента учитываются в колонке «Файлов»)."
        )
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True, height=480)

    # Управление: очистить всю папку или удалить один подкаталог
    st.markdown("---")
    st.subheader("🗑️ Управление папкой bills")

    # Очистить всю папку bills (после работы — в следующем месяце загрузить новый ZIP)
    has_content = any(bills_root.iterdir())
    if has_content:
        st.markdown("**Очистить всю папку** — удалить всё содержимое `bills`. После этого можно загрузить новый ZIP со счетами и актами.")
        clear_confirm = st.checkbox("Подтверждаю полную очистку папки bills", key="bills_clear_confirm")
        if st.button("🗑️ Очистить всю папку bills", type="secondary", use_container_width=True):
            if not clear_confirm:
                st.warning("Поставьте галочку подтверждения.")
            else:
                try:
                    for item in bills_root.iterdir():
                        if item.is_dir():
                            shutil.rmtree(item)
                        else:
                            item.unlink()
                    st.success("✅ Папка `bills` очищена. Можно загрузить новый ZIP в следующем месяце.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Ошибка при очистке: {e}")
        st.markdown("---")

    if options:
        st.markdown("**Удалить одну папку верхнего уровня** (контрагент целиком, со всеми вложенными счетами):")
        filter_q = st.text_input(
            "Фильтр по имени папки (подстрока)",
            value="",
            key="bills_subdir_filter",
            help="При большом числе контрагентов сузьте список перед выбором.",
        )
        q = filter_q.strip().lower()
        filtered = [n for n in options if q in n.lower()] if q else list(options)
        if not filtered:
            st.warning("Ни одна папка не подходит под фильтр. Очистите поле или измените подстроку.")
        else:
            st.caption(f"В списке выбора: **{len(filtered)}** из {len(options)} папок.")
            col_sel, col_btn = st.columns([3, 1])
            with col_sel:
                to_delete = st.selectbox(
                    "Выберите папку для удаления вместе с содержимым",
                    filtered,
                    index=0,
                    key="bills_delete_subdir",
                )
            with col_btn:
                confirm = st.checkbox(
                    "Подтверждаю удаление",
                    key="bills_delete_confirm",
                    help="Будет удалена выбранная директория и все файлы внутри неё.",
                )

            if st.button("🗑️ Удалить выбранную папку", type="secondary", use_container_width=True):
                if not confirm:
                    st.warning("Поставьте галочку подтверждения, чтобы удалить папку.")
                else:
                    target = bills_root / to_delete
                    if not target.exists() or not target.is_dir():
                        st.error(f"Директория `{to_delete}` не найдена.")
                    else:
                        try:
                            shutil.rmtree(target)
                            st.success(f"✅ Папка `{to_delete}` и всё содержимое удалены.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Ошибка при удалении `{to_delete}`: {e}")


