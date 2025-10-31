#!/usr/bin/env python3
"""
Streamlit интерфейс для загрузки данных SPNet и STECCOM
Управление импортом файлов в Oracle (production) или PostgreSQL (testing)
"""

import streamlit as st
import pandas as pd
import psycopg2
from pathlib import Path
import os
import glob
from datetime import datetime
import subprocess
import sys

# Конфигурация базы данных
# Используйте переменные окружения для паролей в production!
ORACLE_CONFIG = {
    'username': os.getenv('ORACLE_USER', 'billing7'),
    'password': os.getenv('ORACLE_PASSWORD', 'your-password-here'),
    'host': os.getenv('ORACLE_HOST', 'your-oracle-host'),
    'port': int(os.getenv('ORACLE_PORT', '1521')),
    'service_name': os.getenv('ORACLE_SERVICE', 'bm7')
}

POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'billing'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your-password-here'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432'))
}

# По умолчанию используем PostgreSQL для тестирования
DEFAULT_DB_TYPE = 'postgresql'  # или 'oracle'

DATA_DIR = Path(__file__).parent / 'data'
SPNET_DIR = DATA_DIR / 'SPNet reports'
STECCOM_DIR = DATA_DIR / 'STECCOMLLCRussiaSBD.AccessFees_reports'


def get_db_connection(db_type='postgresql'):
    """Создание подключения к базе данных (Oracle или PostgreSQL)"""
    if db_type == 'oracle':
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
    else:  # postgresql
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            return conn
        except Exception as e:
            st.error(f"Ошибка подключения к PostgreSQL: {e}")
            return None


def get_loaded_files_info(table_name, source_file_column='SOURCE_FILE', db_type='postgresql'):
    """Получить информацию о уже загруженных файлах"""
    conn = get_db_connection(db_type)
    if not conn:
        return pd.DataFrame()
    
    try:
        if db_type == 'oracle':
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
        else:
            # PostgreSQL синтаксис
            query = f"""
            SELECT DISTINCT 
                {source_file_column.lower()} AS file_name,
                MAX(load_date) AS last_load_date,
                COUNT(*) AS records_count
            FROM {table_name.lower()}
            WHERE {source_file_column.lower()} IS NOT NULL
            GROUP BY {source_file_column.lower()}
            ORDER BY MAX(load_date) DESC
            LIMIT 20
            """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.warning(f"Не удалось получить информацию о загруженных файлах: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def list_data_files(directory):
    """Список файлов в директории"""
    if not directory.exists():
        return []
    files = []
    for f in directory.iterdir():
        if f.is_file():
            files.append({
                'name': f.name,
                'size': f.stat().st_size,
                'modified': datetime.fromtimestamp(f.stat().st_mtime),
                'path': str(f)
            })
    return sorted(files, key=lambda x: x['modified'], reverse=True)


def run_import_script(script_name, db_type='postgresql'):
    """Запуск скрипта импорта (Oracle или PostgreSQL)"""
    import importlib.util
    
    scripts_dir = Path(__file__).parent / 'python'
    
    if db_type == 'oracle':
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
    else:
        # PostgreSQL скрипт
        script_path = scripts_dir / 'load_data_postgres.py'
        module_name = "load_data_postgres"
        class_name = "PostgresDataLoader"
        if script_name == 'spnet':
            method_name = "load_spnet_files"
        elif script_name == 'steccom':
            method_name = "load_steccom_files"
        else:
            return False, "Unknown script type"
        config = POSTGRES_CONFIG
        connect_method = "connect"
    
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
        if db_type == 'oracle':
            loader.gdrive_path = str(SPNET_DIR if script_name == 'spnet' else STECCOM_DIR)
        else:
            loader.spnet_path = str(SPNET_DIR)
            loader.steccom_path = str(STECCOM_DIR)
        
        # Подключаемся к БД
        connect_func = getattr(loader, connect_method)
        if not connect_func():
            return False, f"Failed to connect to {db_type} database"
        
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
    st.markdown("Загрузка и импорт данных SPNet и STECCOM")
    st.markdown("---")
    
    # Выбор типа БД и конфигурация в сайдбаре
    with st.sidebar:
        st.markdown("### 🗄️ Database Type")
        db_type = st.radio(
            "Select Database",
            ["postgresql", "oracle"],
            index=0 if DEFAULT_DB_TYPE == 'postgresql' else 1,
            help="PostgreSQL для тестирования, Oracle для production"
        )
        
        st.markdown("---")
        
        if db_type == 'oracle':
            st.markdown("### ⚙️ Oracle Configuration")
            oracle_host = st.text_input("Host", value=ORACLE_CONFIG['host'], key='oracle_host')
            oracle_port = st.number_input("Port", value=ORACLE_CONFIG['port'], key='oracle_port')
            oracle_service = st.text_input("Service Name", value=ORACLE_CONFIG['service_name'], key='oracle_service')
            oracle_user = st.text_input("Username", value=ORACLE_CONFIG['username'], key='oracle_user')
            oracle_pass = st.text_input("Password", type="password", value=ORACLE_CONFIG['password'], key='oracle_pass')
            
            if st.button("🔄 Update Oracle Config"):
                ORACLE_CONFIG.update({
                    'host': oracle_host,
                    'port': int(oracle_port),
                    'service_name': oracle_service,
                    'username': oracle_user,
                    'password': oracle_pass
                })
                st.success("Oracle configuration updated!")
        else:
            st.markdown("### ⚙️ PostgreSQL Configuration")
            pg_host = st.text_input("Host", value=POSTGRES_CONFIG['host'], key='pg_host')
            pg_port = st.number_input("Port", value=POSTGRES_CONFIG['port'], key='pg_port')
            pg_db = st.text_input("Database", value=POSTGRES_CONFIG['dbname'], key='pg_db')
            pg_user = st.text_input("Username", value=POSTGRES_CONFIG['user'], key='pg_user')
            pg_pass = st.text_input("Password", type="password", value=POSTGRES_CONFIG['password'], key='pg_pass')
            
            if st.button("🔄 Update PostgreSQL Config"):
                POSTGRES_CONFIG.update({
                    'host': pg_host,
                    'port': int(pg_port),
                    'dbname': pg_db,
                    'user': pg_user,
                    'password': pg_pass
                })
                st.success("PostgreSQL configuration updated!")
        
        st.markdown("---")
        st.caption(f"📡 Current DB: **{db_type.upper()}**")
    
    # Табы для разных типов данных
    tab1, tab2, tab3 = st.tabs([
        "📊 SPNet Traffic", 
        "💰 STECCOM Access Fees",
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
                files_df['size_mb'] = files_df['size'] / (1024 * 1024)
                files_df['modified'] = files_df['modified'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                display_df = files_df[['name', 'size_mb', 'modified']].copy()
                display_df.columns = ['File Name', 'Size (MB)', 'Modified']
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    height=300
                )
            else:
                st.info("📁 Директория пуста или не найдена")
        
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
            
            # Импорт данных
            if st.button("🔄 Import All SPNet Files", use_container_width=True, type="primary"):
                with st.spinner(f"Импорт данных SPNet в {db_type.upper()}..."):
                    success, message = run_import_script('spnet', db_type)
                    if success:
                        st.success("✅ Импорт завершен успешно")
                        st.text_area("Log output", message, height=200)
                    else:
                        st.error(f"❌ Ошибка импорта: {message}")
        
        # Информация о загруженных файлах
        st.markdown("---")
        st.subheader("📊 Загруженные в базу файлы")
        table_name = 'SPNET_TRAFFIC' if db_type == 'oracle' else 'spnet_traffic'
        source_col = 'SOURCE_FILE' if db_type == 'oracle' else 'source_file'
        loaded_spnet = get_loaded_files_info(table_name, source_col, db_type)
        if not loaded_spnet.empty:
            loaded_spnet.columns = ['File Name', 'Last Load Date', 'Records Count']
            st.dataframe(loaded_spnet, use_container_width=True, hide_index=True)
        else:
            st.info("Нет информации о загруженных файлах")
    
    # ========== STECCOM Access Fees Tab ==========
    with tab2:
        st.subheader("STECCOM Access Fees Reports")
        st.markdown("**Директория:** `data/STECCOMLLCRussiaSBD.AccessFees_reports/`")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Список файлов
            steccom_files = list_data_files(STECCOM_DIR)
            
            if steccom_files:
                st.markdown(f"**Найдено файлов: {len(steccom_files)}**")
                
                files_df = pd.DataFrame(steccom_files)
                files_df['size_mb'] = files_df['size'] / (1024 * 1024)
                files_df['modified'] = files_df['modified'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                display_df = files_df[['name', 'size_mb', 'modified']].copy()
                display_df.columns = ['File Name', 'Size (MB)', 'Modified']
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    height=300
                )
            else:
                st.info("📁 Директория пуста или не найдена")
        
        with col2:
            st.markdown("### Действия")
            
            # Загрузка нового файла
            uploaded_file = st.file_uploader(
                "Upload STECCOM file",
                type=['csv'],
                key='steccom_upload'
            )
            
            if uploaded_file:
                save_path = STECCOM_DIR / uploaded_file.name
                if save_path.exists():
                    st.warning(f"⚠️ Файл `{uploaded_file.name}` уже существует")
                else:
                    if st.button("💾 Save File", key='save_steccom'):
                        try:
                            STECCOM_DIR.mkdir(parents=True, exist_ok=True)
                            with open(save_path, 'wb') as f:
                                f.write(uploaded_file.getbuffer())
                            st.success(f"✅ Файл сохранен: {uploaded_file.name}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Ошибка сохранения: {e}")
            
            st.markdown("---")
            
            # Импорт данных
            if st.button("🔄 Import All STECCOM Files", use_container_width=True, type="primary"):
                with st.spinner(f"Импорт данных STECCOM в {db_type.upper()}..."):
                    success, message = run_import_script('steccom', db_type)
                    if success:
                        st.success("✅ Импорт завершен успешно")
                        st.text_area("Log output", message, height=200)
                    else:
                        st.error(f"❌ Ошибка импорта: {message}")
        
        # Информация о загруженных файлах
        st.markdown("---")
        st.subheader("📊 Загруженные в базу файлы")
        table_name = 'STECCOM_EXPENSES' if db_type == 'oracle' else 'steccom_expenses'
        source_col = 'SOURCE_FILE' if db_type == 'oracle' else 'source_file'
        loaded_steccom = get_loaded_files_info(table_name, source_col, db_type)
        if not loaded_steccom.empty:
            loaded_steccom.columns = ['File Name', 'Last Load Date', 'Records Count']
            st.dataframe(loaded_steccom, use_container_width=True, hide_index=True)
        else:
            st.info("Нет информации о загруженных файлах")
    
    # ========== Load History Tab ==========
    with tab3:
        st.subheader("📋 История загрузок")
        
        conn = get_db_connection(db_type)
        if conn:
            try:
                # История из load_logs
                if db_type == 'oracle':
                    query = """
                    SELECT 
                        TABLE_NAME,
                        LOAD_DATE,
                        RECORDS_COUNT,
                        STATUS,
                        ERROR_MESSAGE
                    FROM LOAD_LOGS
                    ORDER BY LOAD_DATE DESC
                    FETCH FIRST 50 ROWS ONLY
                    """
                else:
                    query = """
                    SELECT 
                        table_name,
                        load_date,
                        records_count,
                        status,
                        error_message
                    FROM load_logs
                    ORDER BY load_date DESC
                    LIMIT 50
                    """
                history_df = pd.read_sql(query, conn)
                
                if not history_df.empty:
                    history_df.columns = ['Table Name', 'Load Date', 'Records Count', 'Status', 'Error Message']
                    history_df['Load Date'] = pd.to_datetime(history_df['Load Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    st.dataframe(history_df, use_container_width=True, hide_index=True)
                else:
                    st.info("История загрузок пуста")
            except Exception as e:
                table_name = 'LOAD_LOGS' if db_type == 'oracle' else 'load_logs'
                st.warning(f"Таблица {table_name} недоступна: {e}")
            finally:
                conn.close()


if __name__ == "__main__":
    main()

