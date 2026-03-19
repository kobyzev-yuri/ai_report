"""
Закладка: Загрузка данных
"""
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys
import io

def show_tab(get_connection, count_file_records, get_records_in_db):
    """
    Отображение закладки загрузки данных
    """
    st.header("📥 Data Loader")
    st.markdown("Загрузка и импорт данных Иридиум (трафик и финансовые файлы)")
    
    # Expander с комментарием к процедуре загрузки
    with st.expander("ℹ️ О процедуре загрузки документов (CSV) в базу", expanded=False):
        st.markdown("""
        **Процедура загрузки CSV файлов:**
        
        1. **Автоматическое определение типа файла:**
           - Файлы с именами, содержащими "spnet" или "traffic" → загружаются как SPNet
           - Файлы с именами, содержащими "steccom", "access" или "fee" → загружаются как STECCOM
        
        2. **Автоматическое сохранение:**
           - SPNet файлы сохраняются в `data/SPNet reports/`
           - STECCOM файлы сохраняются в `data/STECCOMLLCRussiaSBD.AccessFees_reports/`
        
        3. **Проверка дубликатов:**
           - Система автоматически проверяет, был ли файл уже загружен
           - Уже загруженные файлы пропускаются
           - Неполные загрузки перезагружаются автоматически
        
        4. **Типы данных:**
           - **SPNet**: данные об использовании трафика (CSV/Excel)
           - **STECCOM**: финансовые данные из инвойсов (CSV/Excel)
        
        5. **После загрузки:**
           - Обновите вкладку "Report" для просмотра новых данных
           - Данные автоматически попадают в базу данных Oracle
        
        **Формат файлов:**
        - Поддерживаются форматы: CSV, XLSX
        - Файлы должны соответствовать структуре таблиц SPNET_TRAFFIC или STECCOM_EXPENSES
        """)
    
    st.markdown("---")
    
    # Директории для данных
    # Определяем путь к директории данных
    script_dir = Path(__file__).resolve()
    # Находим корень проекта (где есть tabs/)
    current = script_dir
    while current.parent != current:
        if (current / 'tabs').exists():
            project_root = current
            break
        current = current.parent
    else:
        # Fallback: используем текущую директорию
        project_root = script_dir
    DATA_DIR = project_root / 'data'
    SPNET_DIR = DATA_DIR / 'SPNet reports'
    ACCESS_FEES_DIR = DATA_DIR / 'STECCOMLLCRussiaSBD.AccessFees_reports'
    
    # Функция для определения типа файла по имени
    def detect_file_type(filename):
        """Определяет тип файла (SPNet или STECCOM) по имени"""
        filename_lower = filename.lower()
        if 'spnet' in filename_lower or 'traffic' in filename_lower:
            return 'SPNet'
        elif 'steccom' in filename_lower or 'access' in filename_lower or 'fee' in filename_lower:
            return 'STECCOM'
        return None
    
    st.markdown("---")
    
    # Универсальный загрузчик файлов
    st.subheader("📤 Upload File")
    uploaded_file = st.file_uploader(
        "📤 Upload file (drag & drop) - автоматически определит тип по имени",
        type=['csv', 'xlsx'],
        key='file_uploader',
        help="Файлы автоматически сохраняются в нужную директорию на основе имени файла"
    )
    
    if uploaded_file:
        file_type = detect_file_type(uploaded_file.name)
        
        if file_type == 'SPNet':
            target_dir = SPNET_DIR
            file_type_msg = "✅ **Определен как SPNet файл** - будет сохранен в SPNet reports"
        elif file_type == 'STECCOM':
            target_dir = ACCESS_FEES_DIR
            file_type_msg = "✅ **Определен как Access Fees файл** - будет сохранен в Access Fees directory"
        else:
            file_type = st.radio(
                "Не удалось определить тип файла. Выберите тип:",
                ["SPNet Traffic", "Access Fees (Financial)"],
                horizontal=True,
                key='file_type_selector'
            )
            if file_type == "SPNet Traffic":
                target_dir = SPNET_DIR
                file_type_msg = "⚠️ **Выбран SPNet** - будет сохранен в SPNet reports"
            else:
                target_dir = ACCESS_FEES_DIR
                file_type_msg = "⚠️ **Выбран Access Fees** - будет сохранен в Access Fees directory"
        
        if file_type:
            st.info(file_type_msg)
            save_path = target_dir / uploaded_file.name
            
            if save_path.exists():
                st.warning(f"⚠️ File `{uploaded_file.name}` already exists")
            else:
                with st.form(key='save_file_form', clear_on_submit=True):
                    if st.form_submit_button("💾 Save File", use_container_width=True):
                        try:
                            with st.spinner("Saving file..."):
                                target_dir.mkdir(parents=True, exist_ok=True)
                                with open(save_path, 'wb') as f:
                                    f.write(uploaded_file.getbuffer())
                            st.success(f"✅ File saved to {target_dir.name}/: {uploaded_file.name}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error saving: {e}")
    
    st.markdown("---")
    
    # Показываем оба типа файлов в двух колонках
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 SPNet Traffic Reports")
        st.markdown(f"**Directory:** `{SPNET_DIR}`")
        
        if SPNET_DIR.exists():
            spnet_files = list(SPNET_DIR.glob("*.csv")) + list(SPNET_DIR.glob("*.xlsx"))
            if spnet_files:
                cache_key = 'spnet_loaded_files'
                if cache_key not in st.session_state:
                    conn_status = get_connection()
                    loaded_files = set()
                    if conn_status:
                        try:
                            cursor = conn_status.cursor()
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
                                    file_col = "FILE_NAME"
                            
                            cursor.execute(f"""
                                SELECT LOWER({file_col}) FROM LOAD_LOGS 
                                WHERE UPPER(TABLE_NAME) = 'SPNET_TRAFFIC' 
                                AND LOAD_STATUS = 'SUCCESS'
                            """)
                            loaded_files = {row[0] for row in cursor.fetchall()}
                            cursor.close()
                            st.session_state[cache_key] = loaded_files
                        except:
                            st.session_state[cache_key] = set()
                        finally:
                            conn_status.close()
                    else:
                        st.session_state[cache_key] = set()
                else:
                    loaded_files = st.session_state[cache_key]
                
                st.markdown(f"**Found files: {len(spnet_files)}**")
                files_info = []
                for f in sorted(spnet_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
                    is_loaded = f.name.lower() in loaded_files
                    records_in_file = count_file_records(f)
                    records_in_file_str = f"{records_in_file:,}" if records_in_file is not None else "N/A"
                    records_in_db = None
                    if is_loaded:
                        records_in_db = get_records_in_db(get_connection, f.name, 'SPNET_TRAFFIC')
                    records_in_db_str = f"{records_in_db:,}" if records_in_db is not None and records_in_db > 0 else "-"
                    
                    files_info.append({
                        'File Name': f.name,
                        'Size (MB)': round(f.stat().st_size / (1024 * 1024), 2),
                        'Records in File': records_in_file_str,
                        'Records in DB': records_in_db_str,
                        'Modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                        'Status': '✅ Loaded' if is_loaded else '⏳ Not loaded'
                    })
                df_files = pd.DataFrame(files_info)
                st.dataframe(df_files, use_container_width=True, hide_index=True, height=200)
                
                if st.button("🔄 Refresh Load Status", key='refresh_spnet_status'):
                    if cache_key in st.session_state:
                        del st.session_state[cache_key]
                    st.rerun()
            else:
                st.info("📁 Directory is empty")
        else:
            st.info(f"📁 Directory does not exist: {SPNET_DIR}")
    
    with col2:
        st.subheader("💰 Access Fees (Financial)")
        st.markdown(f"**Directory:** `{ACCESS_FEES_DIR}`")
        
        if ACCESS_FEES_DIR.exists():
            access_fees_files = list(ACCESS_FEES_DIR.glob("*.csv"))
            if access_fees_files:
                cache_key = 'access_fees_loaded_files'
                if cache_key not in st.session_state:
                    conn_status = get_connection()
                    loaded_files = set()
                    if conn_status:
                        try:
                            cursor = conn_status.cursor()
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
                                    file_col = "FILE_NAME"
                            
                            cursor.execute(f"""
                                SELECT LOWER({file_col}) FROM LOAD_LOGS 
                                WHERE UPPER(TABLE_NAME) = 'STECCOM_EXPENSES' 
                                AND LOAD_STATUS = 'SUCCESS'
                            """)
                            loaded_files = {row[0] for row in cursor.fetchall()}
                            cursor.close()
                            st.session_state[cache_key] = loaded_files
                        except:
                            st.session_state[cache_key] = set()
                        finally:
                            conn_status.close()
                    else:
                        st.session_state[cache_key] = set()
                else:
                    loaded_files = st.session_state[cache_key]
                
                st.markdown(f"**Found files: {len(access_fees_files)}**")
                files_info = []
                for f in sorted(access_fees_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
                    is_loaded = f.name.lower() in loaded_files
                    records_in_file = count_file_records(f)
                    records_in_file_str = f"{records_in_file:,}" if records_in_file is not None else "N/A"
                    records_in_db = None
                    if is_loaded:
                        records_in_db = get_records_in_db(get_connection, f.name, 'STECCOM_EXPENSES')
                    records_in_db_str = f"{records_in_db:,}" if records_in_db is not None and records_in_db > 0 else "-"
                    
                    files_info.append({
                        'File Name': f.name,
                        'Size (MB)': round(f.stat().st_size / (1024 * 1024), 2),
                        'Records in File': records_in_file_str,
                        'Records in DB': records_in_db_str,
                        'Modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                        'Status': '✅ Loaded' if is_loaded else '⏳ Not loaded'
                    })
                df_files = pd.DataFrame(files_info)
                st.dataframe(df_files, use_container_width=True, hide_index=True, height=200)
                
                if st.button("🔄 Refresh Load Status", key='refresh_access_fees_status'):
                    if cache_key in st.session_state:
                        del st.session_state[cache_key]
                    st.rerun()
            else:
                st.info("📁 Directory is empty")
        else:
            st.info(f"📁 Directory does not exist: {ACCESS_FEES_DIR}")
    
    st.markdown("---")
    
    # Импорт файлов
    st.subheader("🔄 Import Files")
    st.markdown("Импорт файлов в базу данных Oracle")
    
    if st.button("📥 Import All Files (SPNet + Access Fees)", use_container_width=True, type="primary", key='import_all_files_btn'):
        all_logs = []
        
        # Импорт SPNet
        if SPNET_DIR.exists():
            spnet_files = list(SPNET_DIR.glob("*.csv")) + list(SPNET_DIR.glob("*.xlsx"))
            if spnet_files:
                try:
                    # Импорт из python/ или deploy/python/
                    try:
                        from python.load_spnet_traffic import SPNetDataLoader
                    except ImportError:
                        # Модуль может находиться на уровень выше
                        sys.path.insert(0, str(project_root))
                        from python.load_spnet_traffic import SPNetDataLoader
                    import os
                    oracle_config = {
                        'username': os.getenv('ORACLE_USER', 'billing7'),
                        'password': os.getenv('ORACLE_PASSWORD', ''),
                        'host': os.getenv('ORACLE_HOST', ''),
                        'port': int(os.getenv('ORACLE_PORT', '1521')),
                        'service_name': os.getenv('ORACLE_SERVICE', 'bm7')
                    }
                    loader = SPNetDataLoader(oracle_config)
                    if loader.connect_to_oracle():
                        loader.gdrive_path = str(SPNET_DIR)
                        log_capture = io.StringIO()
                        old_stdout = sys.stdout
                        old_stderr = sys.stderr
                        try:
                            sys.stdout = log_capture
                            sys.stderr = log_capture
                            result = loader.load_spnet_files()
                            log_output = log_capture.getvalue()
                            all_logs.append(("SPNet", result, log_output))
                        finally:
                            sys.stdout = old_stdout
                            sys.stderr = old_stderr
                            if loader.connection:
                                loader.close_connection()
                    else:
                        all_logs.append(("SPNet", False, "❌ Не удалось подключиться к базе данных"))
                except Exception as e:
                    import traceback
                    all_logs.append(("SPNet", False, f"❌ Ошибка: {e}\n{traceback.format_exc()}"))
        
        # Импорт Access Fees
        if ACCESS_FEES_DIR.exists():
            access_fees_files = list(ACCESS_FEES_DIR.glob("*.csv"))
            if access_fees_files:
                try:
                    # Импорт из python/ или deploy/python/
                    try:
                        from python.load_steccom_expenses import STECCOMDataLoader
                    except ImportError:
                        # Модуль может находиться на уровень выше
                        sys.path.insert(0, str(project_root))
                        from python.load_steccom_expenses import STECCOMDataLoader
                    import os
                    oracle_config = {
                        'username': os.getenv('ORACLE_USER', 'billing7'),
                        'password': os.getenv('ORACLE_PASSWORD', ''),
                        'host': os.getenv('ORACLE_HOST', ''),
                        'port': int(os.getenv('ORACLE_PORT', '1521')),
                        'service_name': os.getenv('ORACLE_SERVICE', 'bm7')
                    }
                    loader = STECCOMDataLoader(oracle_config)
                    if loader.connect_to_oracle():
                        loader.gdrive_path = str(ACCESS_FEES_DIR)
                        log_capture = io.StringIO()
                        old_stdout = sys.stdout
                        old_stderr = sys.stderr
                        try:
                            sys.stdout = log_capture
                            sys.stderr = log_capture
                            result = loader.load_steccom_files()
                            log_output = log_capture.getvalue()
                            all_logs.append(("Access Fees", result, log_output))
                        finally:
                            sys.stdout = old_stdout
                            sys.stderr = old_stderr
                            if loader.connection:
                                loader.close_connection()
                    else:
                        all_logs.append(("Access Fees", False, "❌ Не удалось подключиться к базе данных"))
                except Exception as e:
                    import traceback
                    all_logs.append(("Access Fees", False, f"❌ Ошибка: {e}\n{traceback.format_exc()}"))
        
        # Краткий результат без закладок с логами — не мешает сразу снова нажать Import
        for file_type, success, log_output in all_logs:
            if success:
                st.success(f"✅ Импорт {file_type} завершен успешно!")
            else:
                st.error(f"❌ Ошибка импорта {file_type}")
            if log_output and not success:
                st.text_area(f"Лог {file_type}", log_output, height=120, key=f'log_{file_type.lower().replace(" ", "_")}')
            elif log_output and success:
                with st.expander("Подробности (лог)", expanded=False):
                    st.text(log_output)
    
    st.markdown("---")
    st.caption("💡 **Tip:** After importing, refresh the Report tab to see updated data")

