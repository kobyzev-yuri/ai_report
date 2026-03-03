"""
Закладка: Загрузка счетов из 1С (директория bills)
"""
import streamlit as st
from pathlib import Path
from datetime import datetime
import os
import shutil
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


def _save_uploaded_file(file, target_dir: Path) -> str:
    """
    Сохранить один загруженный объект:
    - если ZIP — распаковать в поддиректорию
    - иначе сохранить как обычный файл
    Возвращает текстовый отчет.
    """
    _ensure_dir(target_dir)

    filename = Path(file.name).name
    data = file.read()

    # ZIP-архив: распаковываем, сохраняя структуру директорий
    if filename.lower().endswith(".zip"):
        subdir_name = filename[:-4] or f"zip_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        extract_dir = target_dir / subdir_name
        _ensure_dir(extract_dir)

        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(extract_dir)

        # Подсчитаем количество файлов
        file_count = 0
        for root, _, files in os.walk(extract_dir):
            file_count += len(files)

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

        **Как использовать:** сформируйте в 1С папку со счетами и актами (PDF/HTML и т.п.), упакуйте в **ZIP‑архив**
        и загрузите архив здесь — он распакуется в `bills`. После работы с рассылкой можно **очистить папку** `bills`;
        в следующем месяце загрузите новый ZIP в пустую папку.
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
        "Загрузите ZIP‑архив со счетами (или несколько ZIP/файлов)",
        accept_multiple_files=True,
        type=None,
        help="ZIP распаковывается в папку bills с сохранением структуры. Отдельные файлы сохраняются в bills.",
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

    subdirs = sorted(
        [p for p in bills_root.glob("**/*") if p.is_dir()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    options = []

    if not subdirs:
        st.info("Папка `bills` пуста. Загрузите ZIP со счетами и актами — он распакуется сюда.")
        # Показываем блок «Очистить всю папку» ниже (пустая папка — нечего очищать)
    else:
        rows = []
        options = []
        for d in subdirs[:50]:
            file_count = 0
            latest_mtime = None
            for root, _, files in os.walk(d):
                for name in files:
                    file_count += 1
                    mtime = Path(root, name).stat().st_mtime
                    if latest_mtime is None or mtime > latest_mtime:
                        latest_mtime = mtime

            rel = str(d.relative_to(bills_root))
            rows.append(
                {
                    "Подкаталог": rel,
                    "Файлов": file_count,
                    "Последнее изменение": datetime.fromtimestamp(latest_mtime).strftime("%Y-%m-%d %H:%M:%S")
                    if latest_mtime
                    else "-",
                }
            )
            options.append(rel)

        import pandas as pd
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True, height=300)

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
        st.markdown("**Удалить один подкаталог:**")
        col_sel, col_btn = st.columns([3, 1])
        with col_sel:
            to_delete = st.selectbox(
                "Выберите подкаталог для удаления вместе с содержимым",
                options=options,
                index=0,
                key="bills_delete_subdir",
            )
        with col_btn:
            confirm = st.checkbox(
                "Подтверждаю удаление",
                key="bills_delete_confirm",
                help="Будет удалена выбранная директория и все файлы внутри неё.",
            )

        if st.button("🗑️ Удалить выбранный подкаталог", type="secondary", use_container_width=True):
            if not confirm:
                st.warning("Поставьте галочку подтверждения, чтобы удалить подкаталог.")
            else:
                target = bills_root / to_delete
                if not target.exists() or not target.is_dir():
                    st.error(f"Директория `{to_delete}` не найдена.")
                else:
                    try:
                        shutil.rmtree(target)
                        st.success(f"✅ Подкаталог `{to_delete}` и все его содержимое удалены.")
                        st.info("Обновите страницу, чтобы увидеть актуальный список подкаталогов.")
                    except Exception as e:
                        st.error(f"❌ Ошибка при удалении `{to_delete}`: {e}")


