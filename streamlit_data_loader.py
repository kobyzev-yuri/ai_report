#!/usr/bin/env python3
"""
Streamlit интерфейс для загрузки данных SPNet и STECCOM
Управление импортом файлов в Oracle (production)
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import os
import glob
from datetime import datetime
import subprocess
import sys

# Конфигурация базы данных Oracle
# Используйте переменные окружения для паролей в production!
ORACLE_CONFIG = {
    'username': os.getenv('ORACLE_USER', 'billing7'),
    'password': os.getenv('ORACLE_PASSWORD', 'your-password-here'),
    'host': os.getenv('ORACLE_HOST', 'your-oracle-host'),
    'port': int(os.getenv('ORACLE_PORT', '1521')),
    'service_name': os.getenv('ORACLE_SERVICE', 'bm7')
}

DATA_DIR = Path(__file__).parent / 'data'
SPNET_DIR = DATA_DIR / 'SPNet reports'
STECCOM_DIR = DATA_DIR / 'STECCOMLLCRussiaSBD.AccessFees_reports'


def get_db_connection():
    """Создание подключения к базе данных Oracle"""
    try:
        import cx_Oracle
        conn = cx_Oracle.connect(
            ORACLE_CONFIG['username'],
            ORACLE_CONFIG['password'],
            f"{ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}/{ORACLE_CONFIG['service_name']}"
        )
        return conn
    except ImportError:
        st.error("cx_Oracle не установлен. Установите: pip install cx_Oracle")
        return None
    except Exception as e:
        st.error(f"Ошибка подключения к Oracle: {e}")
        return None


def get_loaded_files_info(table_name, source_file_column='SOURCE_FILE'):
    """Получить информацию о уже загруженных файлах"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Oracle синтаксис
        query = f"""
        SELECT DISTINCT 
            {source_file_column} AS file_name,
            MAX(LOAD_DATE) AS last_load_date,
            COUNT(*) AS records_count
        FROM {table_name}
        WHERE {source_file_column} IS NOT NULL
        GROUP BY {source_file_column}
        ORDER BY MAX(LOAD_DATE) DESC
        FETCH FIRST 20 ROWS ONLY
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.warning(f"Не удалось получить информацию о загруженных файлах: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def get_records_in_db(file_name, table_name='SPNET_TRAFFIC', conn=None):
    """Получить количество записей в базе для конкретного файла
    
    Args:
        file_name: имя файла
        table_name: имя таблицы (Oracle, uppercase)
        conn: существующее подключение (опционально, если не передано - создается новое)
    """
    should_close = False
    if conn is None:
        conn = get_db_connection()
        if not conn:
            return None
        should_close = True
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM {} 
            WHERE UPPER(SOURCE_FILE) = UPPER(:1)
        """.format(table_name), (file_name,))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        return None
    finally:
        if should_close and conn:
            conn.close()


def count_file_records(file_path):
    """Подсчет количества записей в файле (CSV или XLSX)"""
    try:
        import pandas as pd
        file_ext = Path(file_path).suffix.lower()
        
        if not Path(file_path).exists():
            return None
        
        if file_ext == '.xlsx':
            try:
                df = pd.read_excel(file_path, dtype=str, na_filter=False, engine='openpyxl')
                # Удаляем полностью пустые строки
                df = df.dropna(how='all')
                # Удаляем строки, где все значения пустые или пробелы
                df = df[~df.apply(lambda x: x.astype(str).str.strip().eq('').all(), axis=1)]
                return len(df)
            except Exception as e:
                # Пробуем без указания движка
                try:
                    df = pd.read_excel(file_path, dtype=str, na_filter=False)
                    df = df.dropna(how='all')
                    return len(df)
                except:
                    return None
        else:
            # CSV файл
            try:
                df = pd.read_csv(file_path, dtype=str, na_filter=False, quotechar='"')
                return len(df)
            except Exception as e:
                # Пробуем разные кодировки
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(file_path, dtype=str, na_filter=False, encoding=encoding, quotechar='"')
                        return len(df)
                    except:
                        continue
                return None
    except Exception as e:
        return None  # Не удалось прочитать


def list_data_files(directory):
    """Список файлов в директории с подсчетом записей"""
    if not directory.exists():
        return []
    files = []
    for f in directory.iterdir():
        if f.is_file() and f.suffix.lower() in ['.csv', '.xlsx']:
            # Подсчитываем записи в файле
            try:
                record_count = count_file_records(f)
            except Exception as e:
                # Если ошибка при подсчете, все равно добавляем файл
                record_count = None
            
            files.append({
                'name': f.name,
                'size': f.stat().st_size,
                'modified': datetime.fromtimestamp(f.stat().st_mtime),
                'path': str(f),
                'records': record_count  # Может быть None, int или None при ошибке
            })
    return sorted(files, key=lambda x: x['modified'], reverse=True)


def run_import_script(script_name):
    """Запуск скрипта импорта Oracle"""
    import importlib.util
    
    scripts_dir = Path(__file__).parent / 'python'
    
    # Oracle скрипты
    if script_name == 'spnet':
        script_path = scripts_dir / 'load_spnet_traffic.py'
        module_name = "load_spnet_traffic"
        class_name = "SPNetDataLoader"
        method_name = "load_spnet_files"
        config = ORACLE_CONFIG
        connect_method = "connect_to_oracle"
    elif script_name == 'steccom':
        script_path = scripts_dir / 'load_steccom_expenses.py'
        module_name = "load_steccom_expenses"
        class_name = "STECCOMDataLoader"
        method_name = "load_steccom_files"
        config = ORACLE_CONFIG
        connect_method = "connect_to_oracle"
    else:
        return False, "Unknown script type"
    
    if not script_path.exists():
        return False, f"Script not found: {script_path}"
    
    try:
        # Загружаем модуль динамически
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Создаем экземпляр загрузчика
        loader_class = getattr(module, class_name)
        loader = loader_class(config)
        
        # Обновляем пути к директориям
        loader.gdrive_path = str(SPNET_DIR if script_name == 'spnet' else STECCOM_DIR)
        
        # Подключаемся к БД
        connect_func = getattr(loader, connect_method)
        if not connect_func():
            return False, "Failed to connect to Oracle database"
        
        # Запускаем соответствующий метод
        import io
        from contextlib import redirect_stdout, redirect_stderr
        
        log_capture = io.StringIO()
        try:
            with redirect_stdout(log_capture), redirect_stderr(log_capture):
                method = getattr(loader, method_name)
                result = method()
            
            log_output = log_capture.getvalue()
            
            if loader.connection:
                loader.connection.close()
            
            if result:
                return True, log_output if log_output else "Import completed successfully"
            else:
                return False, log_output if log_output else "Import failed"
        finally:
            if hasattr(loader, 'connection') and loader.connection:
                loader.connection.close()
            
    except Exception as e:
        import traceback
        return False, f"Error: {str(e)}\n{traceback.format_exc()}"


def main():
    """Основная функция приложения"""
    
    st.set_page_config(
        page_title="Data Loader - Iridium M2M",
        page_icon="📥",
        layout="wide"
    )
    
    st.title("📥 Data Loader - Iridium M2M")
    st.markdown("Загрузка и импорт данных SPNet и STECCOM в Oracle (production)")
    st.markdown("---")
    
    # Конфигурация Oracle в сайдбаре
    with st.sidebar:
        st.markdown("### ⚙️ Oracle Configuration")
        oracle_host = st.text_input("Host", value=ORACLE_CONFIG['host'], key='oracle_host')
        oracle_port = st.number_input("Port", value=ORACLE_CONFIG['port'], key='oracle_port')
        oracle_service = st.text_input("Service Name", value=ORACLE_CONFIG['service_name'], key='oracle_service')
        oracle_user = st.text_input("Username", value=ORACLE_CONFIG['username'], key='oracle_user')
        oracle_pass = st.text_input("Password", type="password", value=ORACLE_CONFIG['password'], key='oracle_pass')
        
        if st.button("🔄 Update Oracle Config", key='update_oracle_btn'):
            ORACLE_CONFIG.update({
                'host': oracle_host,
                'port': int(oracle_port),
                'service_name': oracle_service,
                'username': oracle_user,
                'password': oracle_pass
            })
            st.success("Oracle configuration updated!")
        
        st.markdown("---")
        st.caption("📡 Database: **ORACLE**")
        
        # Кнопка импорта в sidebar (только кнопка, результаты в основном контенте)
        st.markdown("---")
        st.markdown("### 🔄 Import All Files")
        import_clicked = st.button("📥 Import All Files (SPNet + Access Fees)", use_container_width=True, type="primary", key='import_all_files_btn')
    
    # Основной контент - результаты импорта
    if import_clicked:
        st.markdown("---")
        st.subheader("🔄 Import Results")
        all_logs = []
        
        # Импорт SPNet
        with st.spinner("Импорт данных SPNet в Oracle..."):
            success, message = run_import_script('spnet')
            all_logs.append(("SPNet", success, message))
        
        # Импорт Access Fees
        with st.spinner("Импорт данных Access Fees в Oracle..."):
            success, message = run_import_script('steccom')
            all_logs.append(("Access Fees", success, message))
        
        # Показываем результаты с детальной статистикой
        for file_type, success, message in all_logs:
            st.markdown(f"### {file_type}")
            if success:
                st.success(f"✅ Импорт {file_type} завершен успешно!")
                
                # Парсим логи для извлечения статистики
                if message:
                    import re
                    # Ищем количество загруженных записей
                    records_match = re.search(r'Всего загружено:\s*([\d,]+)\s*записей', message, re.IGNORECASE)
                    if records_match:
                        records_count = records_match.group(1).replace(',', '')
                        st.metric("📊 Загружено записей", f"{int(records_count):,}")
                    
                    # Ищем количество пропущенных файлов
                    skipped_match = re.search(r'Пропущено файлов.*?(\d+)', message, re.IGNORECASE)
                    if skipped_match:
                        skipped_count = skipped_match.group(1)
                        st.metric("⏭ Пропущено файлов", skipped_count)
                    
                    # Ищем время выполнения
                    duration_match = re.search(r'Время выполнения:\s*([\d.]+)\s*сек', message, re.IGNORECASE)
                    if duration_match:
                        duration = duration_match.group(1)
                        st.metric("⏱ Время выполнения", f"{float(duration):.2f} сек")
                    
                    # Показываем детальные логи
                    with st.expander(f"📋 Детальные логи {file_type}"):
                        st.text_area("", message, height=200, key=f'log_{file_type.lower().replace(" ", "_")}')
            else:
                st.error(f"❌ Ошибка импорта {file_type}")
                if message:
                    st.text_area(f"{file_type} Log", message, height=200, key=f'log_{file_type.lower().replace(" ", "_")}')
        
        # Обновляем информацию о загруженных файлах
        st.markdown("---")
        st.info("💡 **Обновите страницу или перейдите на вкладку '📊 SPNet Traffic' / '💰 Access Fees (Financial)' чтобы увидеть обновленную статистику загруженных файлов.**")
        st.markdown("---")
    
    # Табы для разных типов данных
    tab1, tab2, tab3 = st.tabs([
        "📊 SPNet Traffic", 
        "💰 Access Fees (Financial)",
        "📋 Load History"
    ])
    
    # ========== SPNet Traffic Tab ==========
    with tab1:
        st.subheader("SPNet Traffic Reports")
        st.markdown("**Директория:** `data/SPNet reports/`")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Список файлов
            spnet_files = list_data_files(SPNET_DIR)
            
            if spnet_files:
                st.markdown(f"**Найдено файлов: {len(spnet_files)}**")
                
                files_df = pd.DataFrame(spnet_files)
                
                # Убеждаемся, что колонка records существует
                if 'records' not in files_df.columns:
                    files_df['records'] = None
                
                # Заполняем None значениями, если их нет
                files_df['records'] = files_df['records'].fillna(None)
                
                files_df['size_mb'] = files_df['size'] / (1024 * 1024)
                files_df['modified'] = files_df['modified'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Форматируем количество записей
                def format_records(x):
                    if x is None:
                        return "⏳ Calculating..."
                    try:
                        if pd.isna(x):
                            return "⏳ Calculating..."
                        return f"{int(x):,}"
                    except (ValueError, TypeError):
                        return "N/A"
                
                files_df['records'] = files_df['records'].apply(format_records)
                
                # Получаем количество записей в базе для каждого файла (одно подключение для всех)
                records_in_db_list = []
                conn_check = get_db_connection()
                if conn_check:
                    try:
                        for _, row in files_df.iterrows():
                            file_name = row['name']
                            try:
                                records_in_db = get_records_in_db(file_name, 'SPNET_TRAFFIC', conn=conn_check)
                                records_in_db_list.append(f"{records_in_db:,}" if records_in_db is not None and records_in_db > 0 else "-")
                            except:
                                records_in_db_list.append("-")
                    finally:
                        conn_check.close()
                else:
                    # Если не удалось подключиться, показываем "-" для всех
                    records_in_db_list = ["-"] * len(files_df)
                
                # Создаем display_df с обязательными колонками
                display_df = pd.DataFrame()
                display_df['File Name'] = files_df['name']
                display_df['Size (MB)'] = files_df['size_mb'].round(2)
                display_df['Records in File'] = files_df['records']
                display_df['Records in DB'] = records_in_db_list
                display_df['Modified'] = files_df['modified']
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                    key='spnet_files_df'
                )
            else:
                st.info("📁 Директория пуста или не найдена", icon="📁")
        
        with col2:
            st.markdown("### Действия")
            
            # Загрузка нового файла
            uploaded_file = st.file_uploader(
                "Upload SPNet file",
                type=['csv', 'xlsx'],
                key='spnet_upload'
            )
            
            if uploaded_file:
                save_path = SPNET_DIR / uploaded_file.name
                if save_path.exists():
                    st.warning(f"⚠️ Файл `{uploaded_file.name}` уже существует")
                else:
                    if st.button("💾 Save File", key='save_spnet'):
                        try:
                            SPNET_DIR.mkdir(parents=True, exist_ok=True)
                            with open(save_path, 'wb') as f:
                                f.write(uploaded_file.getbuffer())
                            st.success(f"✅ Файл сохранен: {uploaded_file.name}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Ошибка сохранения: {e}")
            
            st.markdown("---")
            
            # Проверка уже загруженных файлов перед импортом
            already_loaded = []
            new_files = []
            orphaned_logs = []  # Файлы с записью в load_logs, но без данных в таблице
            files_to_reload = []  # Файлы, где записей в базе меньше, чем в файле
            if spnet_files:
                conn = get_db_connection()
                if conn:
                    try:
                        for file_info in spnet_files:
                            file_name = file_info['name']
                            records_in_file = file_info.get('records')  # Количество записей в файле
                            has_log_entry = False
                            has_data = False
                            records_in_db = 0
                            
                            # Oracle - определяем структуру таблицы
                            cursor = conn.cursor()
                            # Проверяем, какой столбец используется
                            try:
                                test_query = "SELECT FILE_NAME FROM LOAD_LOGS WHERE ROWNUM = 1"
                                cursor.execute(test_query)
                                file_col = "FILE_NAME"
                            except:
                                try:
                                    test_query = "SELECT SOURCE_FILE FROM LOAD_LOGS WHERE ROWNUM = 1"
                                    cursor.execute(test_query)
                                    file_col = "SOURCE_FILE"
                                except:
                                    file_col = "FILE_NAME"  # по умолчанию
                            
                            # Проверяем load_logs
                            cursor.execute(f"""
                                SELECT COUNT(*) FROM LOAD_LOGS 
                                WHERE UPPER({file_col}) = UPPER(:1) 
                                AND UPPER(TABLE_NAME) = 'SPNET_TRAFFIC'
                                AND LOAD_STATUS = 'SUCCESS'
                            """, (file_name,))
                            has_log_entry = cursor.fetchone()[0] > 0
                            
                            # Проверяем наличие данных в таблице и количество записей
                            cursor.execute("""
                                SELECT COUNT(*) FROM SPNET_TRAFFIC 
                                WHERE UPPER(SOURCE_FILE) = UPPER(:1)
                            """, (file_name,))
                            records_in_db = cursor.fetchone()[0]
                            has_data = records_in_db > 0
                            cursor.close()
                            
                            # Определяем статус файла с учетом количества записей
                            if has_log_entry and has_data:
                                # Проверяем, все ли записи загружены
                                if records_in_file is not None and records_in_file > 0:
                                    if records_in_db < records_in_file:
                                        # В базе меньше записей, чем в файле - нужно перезагрузить
                                        files_to_reload.append((file_name, records_in_file, records_in_db))
                                        new_files.append(file_name)
                                    else:
                                        # Все записи загружены
                                        already_loaded.append(file_name)
                                else:
                                    # Не удалось подсчитать записи в файле, но данные есть - считаем загруженным
                                    already_loaded.append(file_name)
                            elif has_log_entry and not has_data:
                                # Ошибочная запись в load_logs без данных в таблице
                                orphaned_logs.append(file_name)
                                new_files.append(file_name)  # Разрешаем перезагрузку
                            else:
                                new_files.append(file_name)
                    except Exception as e:
                        st.warning(f"Не удалось проверить загруженные файлы: {e}")
                    finally:
                        conn.close()
            
            # Показываем информацию о статусе файлов
            if orphaned_logs:
                st.error(f"⚠️ **Обнаружены несоответствия:** {len(orphaned_logs)} файл(ов) имеют запись в логах, но данных в таблице нет:")
                for orphan in orphaned_logs[:5]:
                    st.text(f"  - {orphan}")
                if len(orphaned_logs) > 5:
                    st.caption(f"  ... и еще {len(orphaned_logs) - 5} файл(ов)")
                st.info("💡 Эти файлы будут загружены заново, чтобы исправить несоответствие.")
            
            if files_to_reload:
                st.warning(f"⚠️ **Неполная загрузка:** {len(files_to_reload)} файл(ов) имеют меньше записей в базе, чем в файле:")
                for file_name, in_file, in_db in files_to_reload[:5]:
                    st.text(f"  - {file_name}: {in_file:,} в файле → {in_db:,} в базе (не хватает {in_file - in_db:,})")
                if len(files_to_reload) > 5:
                    st.caption(f"  ... и еще {len(files_to_reload) - 5} файл(ов)")
                st.info("💡 Эти файлы будут загружены заново, чтобы дополнить недостающие записи.")
            
            if already_loaded:
                if len(already_loaded) == len(spnet_files) and not orphaned_logs and not files_to_reload:
                    st.success(f"✅ **Все файлы уже загружены полностью!** Загружать нечего.")
                    st.info(f"Всего файлов: {len(already_loaded)}")
                else:
                    st.info(f"✅ {len(already_loaded)} из {len(spnet_files)} файл(ов) полностью загружены и будут пропущены:\n- " + "\n- ".join(already_loaded[:5]))
                    if len(already_loaded) > 5:
                        st.caption(f"... и еще {len(already_loaded) - 5} файл(ов)")
                    if new_files:
                        st.info(f"📥 Будет загружено новых/неполных файлов: {len(new_files)}")
            
        
        # Информация о загруженных файлах
        st.markdown("---")
        st.subheader("📊 Загруженные в базу файлы")
        loaded_spnet = get_loaded_files_info('SPNET_TRAFFIC', 'SOURCE_FILE')
        if not loaded_spnet.empty:
            loaded_spnet.columns = ['File Name', 'Last Load Date', 'Records Count']
            st.dataframe(loaded_spnet, use_container_width=True, hide_index=True, key='loaded_spnet_df')
        else:
            st.info("Нет информации о загруженных файлах", icon="ℹ️")
    
    # ========== Access Fees (Financial) Tab ==========
    with tab2:
        
        st.subheader("Access Fees Reports (Financial Files)")
        st.markdown("**Директория:** `data/STECCOMLLCRussiaSBD.AccessFees_reports/`")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Список файлов
            access_fees_files = list_data_files(STECCOM_DIR)
            
            if access_fees_files:
                st.markdown(f"**Найдено файлов: {len(access_fees_files)}**")
                
                files_df = pd.DataFrame(access_fees_files)
                
                # Убеждаемся, что колонка records существует
                if 'records' not in files_df.columns:
                    files_df['records'] = None
                
                # Заполняем None значениями, если их нет
                files_df['records'] = files_df['records'].fillna(None)
                
                files_df['size_mb'] = files_df['size'] / (1024 * 1024)
                files_df['modified'] = files_df['modified'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Форматируем количество записей
                def format_records(x):
                    if x is None:
                        return "⏳ Calculating..."
                    try:
                        import pandas as pd
                        if pd.isna(x):
                            return "⏳ Calculating..."
                        return f"{int(x):,}"
                    except (ValueError, TypeError):
                        return "N/A"
                
                files_df['records'] = files_df['records'].apply(format_records)
                
                # Получаем количество записей в базе для каждого файла (одно подключение для всех)
                records_in_db_list = []
                conn_check = get_db_connection()
                if conn_check:
                    try:
                        for _, row in files_df.iterrows():
                            file_name = row['name']
                            try:
                                records_in_db = get_records_in_db(file_name, 'STECCOM_EXPENSES', conn=conn_check)
                                records_in_db_list.append(f"{records_in_db:,}" if records_in_db is not None and records_in_db > 0 else "-")
                            except:
                                records_in_db_list.append("-")
                    finally:
                        conn_check.close()
                else:
                    # Если не удалось подключиться, показываем "-" для всех
                    records_in_db_list = ["-"] * len(files_df)
                
                # Создаем display_df с обязательными колонками
                display_df = pd.DataFrame()
                display_df['File Name'] = files_df['name']
                display_df['Size (MB)'] = files_df['size_mb'].round(2)
                display_df['Records in File'] = files_df['records']
                display_df['Records in DB'] = records_in_db_list
                display_df['Modified'] = files_df['modified']
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                    key='access_fees_files_df'
                )
            else:
                st.info("📁 Директория пуста или не найдена", icon="📁")
        
        with col2:
            st.markdown("### Действия")
            
            # Загрузка нового файла
            uploaded_file = st.file_uploader(
                "Upload Access Fees file",
                type=['csv'],
                key='access_fees_upload'
            )
            
            if uploaded_file:
                save_path = STECCOM_DIR / uploaded_file.name
                if save_path.exists():
                    st.warning(f"⚠️ Файл `{uploaded_file.name}` уже существует")
                else:
                    if st.button("💾 Save File", key='save_access_fees'):
                        try:
                            STECCOM_DIR.mkdir(parents=True, exist_ok=True)
                            with open(save_path, 'wb') as f:
                                f.write(uploaded_file.getbuffer())
                            st.success(f"✅ Файл сохранен: {uploaded_file.name}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Ошибка сохранения: {e}")
            
            st.markdown("---")
            
            # Проверка уже загруженных файлов перед импортом
            already_loaded = []
            new_files = []
            orphaned_logs = []  # Файлы с записью в load_logs, но без данных в таблице
            files_to_reload = []  # Файлы, где записей в базе меньше, чем в файле
            if access_fees_files:
                conn = get_db_connection()
                if conn:
                    try:
                        for file_info in access_fees_files:
                            file_name = file_info['name']
                            records_in_file = file_info.get('records')  # Количество записей в файле
                            has_log_entry = False
                            has_data = False
                            records_in_db = 0
                            
                            # Oracle - определяем структуру таблицы
                            cursor = conn.cursor()
                            # Проверяем, какой столбец используется
                            try:
                                test_query = "SELECT FILE_NAME FROM LOAD_LOGS WHERE ROWNUM = 1"
                                cursor.execute(test_query)
                                file_col = "FILE_NAME"
                            except:
                                try:
                                    test_query = "SELECT SOURCE_FILE FROM LOAD_LOGS WHERE ROWNUM = 1"
                                    cursor.execute(test_query)
                                    file_col = "SOURCE_FILE"
                                except:
                                    file_col = "FILE_NAME"  # по умолчанию
                            
                            # Проверяем load_logs
                            cursor.execute(f"""
                                SELECT COUNT(*) FROM LOAD_LOGS 
                                WHERE UPPER({file_col}) = UPPER(:1) 
                                AND UPPER(TABLE_NAME) = 'STECCOM_EXPENSES'
                                AND LOAD_STATUS = 'SUCCESS'
                            """, (file_name,))
                            has_log_entry = cursor.fetchone()[0] > 0
                            
                            # Проверяем наличие данных в таблице и количество записей
                            cursor.execute("""
                                SELECT COUNT(*) FROM STECCOM_EXPENSES 
                                WHERE UPPER(SOURCE_FILE) = UPPER(:1)
                            """, (file_name,))
                            records_in_db = cursor.fetchone()[0]
                            has_data = records_in_db > 0
                            cursor.close()
                            
                            # Определяем статус файла с учетом количества записей
                            if has_log_entry and has_data:
                                # Проверяем, все ли записи загружены
                                if records_in_file is not None and records_in_file > 0:
                                    if records_in_db < records_in_file:
                                        # В базе меньше записей, чем в файле - нужно перезагрузить
                                        files_to_reload.append((file_name, records_in_file, records_in_db))
                                        new_files.append(file_name)
                                    else:
                                        # Все записи загружены
                                        already_loaded.append(file_name)
                                else:
                                    # Не удалось подсчитать записи в файле, но данные есть - считаем загруженным
                                    already_loaded.append(file_name)
                            elif has_log_entry and not has_data:
                                # Ошибочная запись в load_logs без данных в таблице
                                orphaned_logs.append(file_name)
                                new_files.append(file_name)  # Разрешаем перезагрузку
                            else:
                                new_files.append(file_name)
                    except Exception as e:
                        st.warning(f"Не удалось проверить загруженные файлы: {e}")
                    finally:
                        conn.close()
            
            # Показываем информацию о статусе файлов
            if orphaned_logs:
                st.error(f"⚠️ **Обнаружены несоответствия:** {len(orphaned_logs)} файл(ов) имеют запись в логах, но данных в таблице нет:")
                for orphan in orphaned_logs[:5]:
                    st.text(f"  - {orphan}")
                if len(orphaned_logs) > 5:
                    st.caption(f"  ... и еще {len(orphaned_logs) - 5} файл(ов)")
                st.info("💡 Эти файлы будут загружены заново, чтобы исправить несоответствие.")
            
            if files_to_reload:
                st.warning(f"⚠️ **Неполная загрузка:** {len(files_to_reload)} файл(ов) имеют меньше записей в базе, чем в файле:")
                for file_name, in_file, in_db in files_to_reload[:5]:
                    st.text(f"  - {file_name}: {in_file:,} в файле → {in_db:,} в базе (не хватает {in_file - in_db:,})")
                if len(files_to_reload) > 5:
                    st.caption(f"  ... и еще {len(files_to_reload) - 5} файл(ов)")
                st.info("💡 Эти файлы будут загружены заново, чтобы дополнить недостающие записи.")
            
            if already_loaded:
                if len(already_loaded) == len(access_fees_files) and not orphaned_logs and not files_to_reload:
                    st.success(f"✅ **Все файлы уже загружены полностью!** Загружать нечего.")
                    st.info(f"Всего файлов: {len(already_loaded)}")
                else:
                    st.info(f"✅ {len(already_loaded)} из {len(access_fees_files)} файл(ов) полностью загружены и будут пропущены:\n- " + "\n- ".join(already_loaded[:5]))
                    if len(already_loaded) > 5:
                        st.caption(f"... и еще {len(already_loaded) - 5} файл(ов)")
                    if new_files:
                        st.info(f"📥 Будет загружено новых/неполных файлов: {len(new_files)}")
            
        
        # Информация о загруженных файлах
        st.markdown("---")
        st.subheader("📊 Загруженные в базу файлы")
        loaded_access_fees = get_loaded_files_info('STECCOM_EXPENSES', 'SOURCE_FILE')
        if not loaded_access_fees.empty:
            loaded_access_fees.columns = ['File Name', 'Last Load Date', 'Records Count']
            st.dataframe(loaded_access_fees, use_container_width=True, hide_index=True)
        else:
            st.info("Нет информации о загруженных файлах")
    
    # ========== Load History Tab ==========
    with tab3:
        st.subheader("📋 История загрузок")
        
        conn = get_db_connection()
        if conn:
            try:
                # История из load_logs (Oracle)
                # Определяем структуру таблицы
                test_cursor = conn.cursor()
                try:
                    test_query = "SELECT FILE_NAME FROM LOAD_LOGS WHERE ROWNUM = 1"
                    test_cursor.execute(test_query)
                    file_col = "FILE_NAME"
                except:
                    try:
                        test_query = "SELECT SOURCE_FILE FROM LOAD_LOGS WHERE ROWNUM = 1"
                        test_cursor.execute(test_query)
                        file_col = "SOURCE_FILE"
                    except:
                        file_col = "FILE_NAME"  # по умолчанию
                test_cursor.close()
                
                query = f"""
                SELECT 
                    TABLE_NAME,
                    {file_col} AS FILE_NAME,
                    LOAD_START_TIME,
                    RECORDS_LOADED,
                    LOAD_STATUS,
                    ERROR_MESSAGE
                FROM LOAD_LOGS
                ORDER BY LOAD_START_TIME DESC
                FETCH FIRST 50 ROWS ONLY
                """
                history_df = pd.read_sql(query, conn)
                
                if not history_df.empty:
                    history_df.columns = ['Table Name', 'File Name', 'Load Date', 'Records Count', 'Status', 'Error Message']
                    history_df['Load Date'] = pd.to_datetime(history_df['Load Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    st.dataframe(history_df, use_container_width=True, hide_index=True)
                else:
                    st.info("История загрузок пуста")
            except Exception as e:
                st.warning(f"Таблица LOAD_LOGS недоступна: {e}")
            finally:
                conn.close()


if __name__ == "__main__":
    main()

