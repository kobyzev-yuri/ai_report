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

        **Как использовать:**
        - В 1С сформируйте папку со счетами (обычно набор PDF/HTML файлов).
        - Упакуйте папку(и) в ZIP‑архив **или** выделите все файлы и перетащите их сюда.
        - Здесь файлы будут сохранены в серверной директории `bills`.
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
        "Перетащите сюда ZIP‑архивы с папками счетов или отдельные файлы",
        accept_multiple_files=True,
        type=None,
        help="Браузер не умеет загружать папки напрямую, поэтому для сохранения структуры "
             "используйте ZIP‑архивы. Обычные файлы будут просто сохранены в выбранную директорию.",
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
    st.subheader("📂 Перенос уже имеющихся на сервере папок 1С")

    st.markdown(
        """
        Оператор может предварительно **скопировать папки 1С на сервер** (через WinSCP/Samba и т.п.),
        а затем выбрать их здесь для переноса в `bills` **целиком с поддиректориями**.
        """
    )

    # Исходная директория с папками счетов 1С
    default_source = str(project_root / "bills_inbox")
    source_root_str = st.text_input(
        "Исходная директория с папками счетов (на сервере)",
        value=default_source,
        help="Укажите путь на сервере, куда попадают папки со счетами из 1С. "
             "Например: /usr/local/projects/ai_report/bills_inbox",
        key="bills_source_root",
    )
    source_root = Path(source_root_str).expanduser()

    col_src1, col_src2 = st.columns(2)
    with col_src1:
        if source_root.exists() and source_root.is_dir():
            st.success(f"Источник найден: {source_root}")
        else:
            st.warning(f"Директория источника не найдена: {source_root}\n"
                       f"Создайте её и положите туда папки со счетами.")
            # Даже если директории нет, продолжаем показывать состояние bills ниже

    with col_src2:
        move_after_copy = st.checkbox(
            "После копирования удалять исходные папки (перенос)",
            value=False,
            help="Если включено, после успешного копирования папки будут удалены из исходной директории.",
            key="bills_move_after_copy",
        )

    source_subdirs = []
    if source_root.exists() and source_root.is_dir():
        source_subdirs = sorted(
            [p for p in source_root.iterdir() if p.is_dir()],
            key=lambda p: p.name.lower(),
        )

    if source_subdirs:
        st.markdown("**Папки, доступные для переноса:**")
        options_src = [p.name for p in source_subdirs]
        selected_src = st.multiselect(
            "Выберите одну или несколько папок для переноса в bills",
            options=options_src,
            default=[],
            key="bills_source_subdirs",
        )

        if st.button("📂 Копировать/перенести выбранные папки в bills", type="primary", use_container_width=True):
            if not selected_src:
                st.warning("Не выбрано ни одной папки для переноса.")
            else:
                reports = []
                with st.spinner("Перенос папок..."):
                    for name in selected_src:
                        src_dir = source_root / name
                        if not src_dir.exists() or not src_dir.is_dir():
                            reports.append(f"❌ Исходная папка `{name}` не найдена.")
                            continue

                            # Целевая директория: сохраняем структуру по имени папки
                        dst_dir = target_dir / name
                        # Если уже существует – добавим timestamp
                        if dst_dir.exists():
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            dst_dir = target_dir / f"{name}_{ts}"

                        try:
                            shutil.copytree(src_dir, dst_dir)
                            if move_after_copy:
                                shutil.rmtree(src_dir)
                                reports.append(f"✅ Папка `{name}` перенесена в `{dst_dir}` (копирование+удаление источника).")
                            else:
                                reports.append(f"✅ Папка `{name}` скопирована в `{dst_dir}`.")
                        except Exception as e:
                            reports.append(f"❌ Ошибка при обработке `{name}`: {e}")

                st.markdown("### Результат переноса папок")
                for line in reports:
                    st.write(line)

    else:
        st.info("В исходной директории пока нет подкаталогов с папками счетов 1С.")

    st.markdown("---")
    st.subheader("📊 Текущее содержимое директории bills")

    subdirs = sorted(
        [p for p in bills_root.glob("**/*") if p.is_dir()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not subdirs:
        st.info("Пока нет подкаталогов в `bills` — после первой загрузки они появятся автоматически.")
        return

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

    # Управление подкаталогами (удаление)
    st.markdown("---")
    st.subheader("🗑️ Управление подкаталогами")

    if options:
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


