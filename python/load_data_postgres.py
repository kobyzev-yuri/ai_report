#!/usr/bin/env python3
"""
Загрузка данных SPNet и STECCOM в PostgreSQL для тестирования
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import glob
import os
from pathlib import Path
import logging
from datetime import datetime
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgresDataLoader:
    def __init__(self, db_config):
        """
        Инициализация загрузчика данных
        
        Args:
            db_config (dict): Конфигурация подключения к PostgreSQL
        """
        self.db_config = db_config
        self.connection = None
        self.spnet_path = "/mnt/gdrive/ai_report/SPNet reports"
        self.steccom_path = "/mnt/gdrive/ai_report/STECCOMLLCRussiaSBD.AccessFees_reports"
    
    def connect(self):
        """Подключение к PostgreSQL"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            logger.info("Успешное подключение к PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            return False
    
    def is_file_loaded(self, file_name, table_name='spnet_traffic', file_path=None):
        """Проверка, загружен ли файл уже и полностью ли загружен
        
        Args:
            file_name: имя файла
            table_name: имя таблицы
            file_path: путь к файлу (опционально, для проверки количества записей)
        
        Returns:
            tuple: (is_loaded: bool, records_in_file: int, records_in_db: int)
        """
        if not self.connection:
            return (False, 0, 0)
        
        cursor = self.connection.cursor()
        records_in_file = 0
        records_in_db = 0
        
        try:
            # Проверяем наличие записи в load_logs
            cursor.execute("""
                SELECT COUNT(*) FROM load_logs 
                WHERE LOWER(source_file) = LOWER(%s) 
                AND LOWER(table_name) = LOWER(%s)
                AND load_status = 'SUCCESS'
            """, (file_name, table_name))
            has_log_entry = cursor.fetchone()[0] > 0
            
            # Проверяем количество записей в базе
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE LOWER(source_file) = LOWER(%s)
            """, (file_name,))
            records_in_db = cursor.fetchone()[0]
            
            # Если есть путь к файлу, проверяем количество записей в файле
            if file_path and Path(file_path).exists():
                try:
                    file_ext = Path(file_path).suffix.lower()
                    if file_ext == '.xlsx':
                        df = pd.read_excel(file_path, dtype=str, na_filter=False, engine='openpyxl')
                    else:
                        df = pd.read_csv(file_path, dtype=str, na_filter=False)
                    df = df.dropna(how='all')
                    records_in_file = len(df)
                except Exception as e:
                    logger.warning(f"Не удалось подсчитать записи в файле {file_name}: {e}")
            
            # Файл считается загруженным, если:
            # 1. Есть запись в load_logs
            # 2. Есть данные в таблице
            # 3. Количество записей в базе >= количеству записей в файле (если удалось подсчитать)
            if has_log_entry and records_in_db > 0:
                if records_in_file > 0:
                    # Если удалось подсчитать записи в файле, сравниваем
                    is_loaded = records_in_db >= records_in_file
                else:
                    # Если не удалось подсчитать, считаем загруженным
                    is_loaded = True
            else:
                is_loaded = False
            
            return (is_loaded, records_in_file, records_in_db)
        except Exception as e:
            logger.warning(f"Ошибка проверки load_logs: {e}")
            return (False, 0, 0)
        finally:
            cursor.close()
    
    def log_load_result(self, table_name, file_name, records_loaded, load_status='SUCCESS', error_message=None):
        """Логирование результата загрузки в load_logs"""
        if not self.connection:
            return
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO load_logs (
                    table_name, source_file, records_loaded, load_status, 
                    error_message, load_start_time, load_end_time, created_by
                ) VALUES (
                    %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s
                )
            """, (table_name, file_name, records_loaded, load_status, error_message, 'STREAMLIT_LOADER'))
            self.connection.commit()
        except Exception as e:
            logger.error(f"Ошибка логирования в load_logs: {e}")
            self.connection.rollback()
        finally:
            cursor.close()
    
    def load_spnet_files(self):
        """Загрузка SPNet CSV файлов (пропускает уже загруженные)"""
        logger.info("="*80)
        logger.info("Начинаем загрузку данных SPNet...")
        logger.info("="*80)
        logger.info(f"Путь к директории SPNet: {self.spnet_path}")
        
        # Проверяем существование директории
        if not Path(self.spnet_path).exists():
            logger.error(f"Директория не существует: {self.spnet_path}")
            return False
        
        csv_files = glob.glob(f"{self.spnet_path}/*.csv") + glob.glob(f"{self.spnet_path}/*.xlsx")
        logger.info(f"Найдено файлов: {len(csv_files)} (CSV + XLSX)")
        
        if not csv_files:
            logger.warning(f"Файлы SPNet не найдены в {self.spnet_path}")
            # Показываем содержимое директории для отладки
            try:
                dir_contents = list(Path(self.spnet_path).glob("*"))
                logger.info(f"Содержимое директории: {[f.name for f in dir_contents]}")
            except Exception as e:
                logger.error(f"Ошибка при чтении директории: {e}")
            return False
        
        total_records = 0
        skipped_files = 0
        load_start_time = datetime.now()
        
        for file_path in csv_files:
            file_name = Path(file_path).name
            file_ext = Path(file_path).suffix.lower()
            logger.info(f"\n{'='*60}")
            logger.info(f"Файл: {file_name} (тип: {file_ext})")
            logger.info(f"Полный путь: {file_path}")
            
            try:
                # Проверяем существование файла
                if not Path(file_path).exists():
                    logger.error(f"Файл не существует: {file_path}")
                    self.log_load_result('spnet_traffic', file_name, 0, 'FAILED', f"File not found: {file_path}")
                    continue
                
                # Проверяем, загружен ли файл уже
                is_loaded, records_in_file, records_in_db = self.is_file_loaded(file_name, 'spnet_traffic', file_path)
                if is_loaded:
                    logger.info(f"⏭ Пропускаем файл (уже загружен полностью): {file_name}")
                    if records_in_file > 0 and records_in_db > 0:
                        logger.info(f"   Записей в файле: {records_in_file:,}, в базе: {records_in_db:,}")
                    skipped_files += 1
                    continue
                elif records_in_db > 0:
                    logger.info(f"⚠️ Файл загружен не полностью: {file_name}")
                    logger.info(f"   Записей в файле: {records_in_file:,}, в базе: {records_in_db:,} (не хватает {records_in_file - records_in_db:,})")
                    logger.info(f"   Перезагружаем файл...")
                
                logger.info(f"🔄 Начинаем обработку файла: {file_name}")
                load_start = datetime.now()
                records_loaded = self.load_spnet_file(file_path)
                load_end = datetime.now()
                duration = (load_end - load_start).total_seconds()
                total_records += records_loaded
                
                # Логируем успешную загрузку
                self.log_load_result('spnet_traffic', file_name, records_loaded, 'SUCCESS')
                logger.info(f"✅ Загружено {records_loaded} записей за {duration:.2f} сек")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Ошибка при обработке файла {file_path}: {error_msg}")
                import traceback
                logger.error(traceback.format_exc())
                # Логируем ошибку
                self.log_load_result('spnet_traffic', file_name, 0, 'FAILED', error_msg)
        
        load_end_time = datetime.now()
        duration = (load_end_time - load_start_time).total_seconds()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Загрузка SPNet завершена")
        logger.info(f"Всего загружено: {total_records:,} записей")
        logger.info(f"Пропущено файлов (уже загружены): {skipped_files}")
        logger.info(f"Время выполнения: {duration:.2f} сек")
        logger.info(f"{'='*80}\n")
        
        return True
    
    def load_spnet_file(self, file_path):
        """Загрузка одного SPNet CSV или XLSX файла"""
        file_ext = Path(file_path).suffix.lower()
        
        # Читаем XLSX файлы
        if file_ext == '.xlsx':
            try:
                # Пробуем прочитать XLSX файл с явным указанием движка
                # Пробуем разные варианты: с заголовком в первой строке или без
                df = None
                for header_row in [0, None]:
                    try:
                        if header_row is not None:
                            df = pd.read_excel(file_path, dtype=str, na_filter=False, engine='openpyxl', header=header_row)
                        else:
                            # Пробуем без заголовка, потом назначим заголовки вручную
                            df = pd.read_excel(file_path, dtype=str, na_filter=False, engine='openpyxl', header=None)
                            # Если первая строка похожа на заголовки, используем её
                            if not df.empty and len(df) > 0:
                                first_row = df.iloc[0].astype(str).tolist()
                                # Проверяем, похожи ли значения первой строки на названия колонок
                                if any('contract' in str(v).lower() or 'imei' in str(v).lower() or 'total' in str(v).lower() for v in first_row):
                                    df.columns = first_row
                                    df = df.iloc[1:].reset_index(drop=True)
                        break
                    except Exception as e:
                        logger.warning(f"Не удалось прочитать с header={header_row}: {e}")
                        continue
                
                if df is None:
                    raise Exception("Не удалось прочитать XLSX файл ни с одним вариантом заголовков")
                
                logger.info(f"Успешно прочитан XLSX файл {file_path}: {len(df)} строк, {len(df.columns)} колонок")
                logger.info(f"Колонки в файле: {list(df.columns)}")
                
                # Нормализуем названия колонок (убираем лишние пробелы, приводим к стандартному виду)
                df.columns = [str(col).strip() for col in df.columns]
                
                # Проверяем, что файл не пустой
                if df.empty:
                    logger.warning(f"XLSX файл {file_path} пуст")
                    return 0
                
                # Удаляем полностью пустые строки
                df = df.dropna(how='all')
                if df.empty:
                    logger.warning(f"XLSX файл {file_path} содержит только пустые строки")
                    return 0
                
                logger.info(f"После удаления пустых строк: {len(df)} строк")
                
            except ImportError as e:
                logger.error(f"Не установлена библиотека openpyxl для чтения XLSX файлов: {e}")
                logger.error("Установите: pip install openpyxl")
                raise
            except Exception as e:
                logger.error(f"Ошибка чтения XLSX файла {file_path}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise
        else:
            # Читаем CSV файлы
            try:
                df = pd.read_csv(file_path, dtype=str, na_filter=False)
                logger.info(f"Успешно прочитан CSV файл {file_path}: {len(df)} строк, {len(df.columns)} колонок")
            except Exception as e:
                logger.error(f"Ошибка чтения CSV файла {file_path}: {e}")
                raise
        
        # Функция для поиска колонки по различным вариантам названия
        def find_column(df, possible_names):
            """Ищет колонку по различным вариантам названия (с учетом пробелов, регистра и т.д.)"""
            df_cols_lower = {str(col).lower().strip(): col for col in df.columns}
            for name in possible_names:
                name_lower = name.lower().strip()
                # Точное совпадение
                if name_lower in df_cols_lower:
                    return df_cols_lower[name_lower]
                # Частичное совпадение (убираем пробелы, скобки и т.д.)
                name_normalized = name_lower.replace(' ', '').replace('(', '').replace(')', '').replace('-', '').replace('_', '')
                for col_name, col_orig in df_cols_lower.items():
                    col_normalized = col_name.replace(' ', '').replace('(', '').replace(')', '').replace('-', '').replace('_', '')
                    if name_normalized in col_normalized or col_normalized in name_normalized:
                        return col_orig
            return None
        
        # Проверяем наличие необходимых колонок и логируем
        required_columns = ['Contract ID', 'IMEI', 'Total Amount']
        missing_columns = []
        for col_name in required_columns:
            if find_column(df, [col_name]) is None:
                missing_columns.append(col_name)
        
        if missing_columns:
            logger.warning(f"Отсутствуют некоторые колонки в файле {file_path}: {missing_columns}")
            logger.info(f"Доступные колонки: {list(df.columns)}")
        
        df['source_file'] = Path(file_path).name
        df['load_date'] = datetime.now()
        df['created_by'] = 'SPNET_LOADER'
        
        # Подготавливаем записи с гибким поиском колонок
        records = []
        skipped_rows = 0
        for idx, row in df.iterrows():
            try:
                # Используем гибкий поиск колонок
                total_rows_col = find_column(df, ['Total Rows', 'TotalRows', 'total_rows'])
                contract_id_col = find_column(df, ['Contract ID', 'ContractID', 'Contract_Id', 'contract_id'])
                imei_col = find_column(df, ['IMEI', 'imei'])
                sim_iccid_col = find_column(df, ['SIM (ICCID)', 'SIM(ICCID)', 'SIM_ICCID', 'sim_iccid', 'ICCID'])
                service_col = find_column(df, ['Service', 'service'])
                usage_type_col = find_column(df, ['Usage Type', 'UsageType', 'usage_type'])
                usage_col = find_column(df, ['Usage', 'usage'])
                usage_unit_col = find_column(df, ['Usage Unit', 'UsageUnit', 'usage_unit'])
                total_amount_col = find_column(df, ['Total Amount', 'TotalAmount', 'total_amount', 'Amount', 'amount'])
                bill_month_col = find_column(df, ['Bill Month', 'BillMonth', 'bill_month'])
                plan_name_col = find_column(df, ['Plan Name', 'PlanName', 'plan_name'])
                imsi_col = find_column(df, ['IMSI', 'imsi'])
                msisdn_col = find_column(df, ['MSISDN', 'msisdn'])
                actual_usage_col = find_column(df, ['Actual Usage', 'ActualUsage', 'actual_usage'])
                call_session_count_col = find_column(df, ['Call/Session Count', 'CallSessionCount', 'call_session_count'])
                sp_account_no_col = find_column(df, ['SP Account No', 'SPAccountNo', 'sp_account_no'])
                sp_name_col = find_column(df, ['SP Name', 'SPName', 'sp_name'])
                sp_reference_col = find_column(df, ['SP Reference', 'SPReference', 'sp_reference'])
                
                record = (
                    self.parse_number(row.get(total_rows_col) if total_rows_col else None),
                    row.get(contract_id_col) if contract_id_col else None,
                    row.get(imei_col) if imei_col else None,
                    row.get(sim_iccid_col) if sim_iccid_col else None,
                    row.get(service_col) if service_col else None,
                    row.get(usage_type_col) if usage_type_col else None,
                    self.parse_number(row.get(usage_col) if usage_col else None),
                    row.get(usage_unit_col) if usage_unit_col else None,
                    self.parse_number(row.get(total_amount_col) if total_amount_col else None),
                    self.parse_number(row.get(bill_month_col) if bill_month_col else None),
                    row.get(plan_name_col) if plan_name_col else None,
                    row.get(imsi_col) if imsi_col else None,
                    row.get(msisdn_col) if msisdn_col else None,
                    self.parse_number(row.get(actual_usage_col) if actual_usage_col else None),
                    self.parse_number(row.get(call_session_count_col) if call_session_count_col else None),
                    self.parse_number(row.get(sp_account_no_col) if sp_account_no_col else None),
                    row.get(sp_name_col) if sp_name_col else None,
                    row.get(sp_reference_col) if sp_reference_col else None,
                    row.get('source_file'),
                    row.get('load_date'),
                    row.get('created_by')
                )
                records.append(record)
            except Exception as e:
                skipped_rows += 1
                logger.warning(f"Ошибка обработки строки {idx} в файле {file_path}: {e}")
                continue
        
        if skipped_rows > 0:
            logger.warning(f"Пропущено строк при обработке: {skipped_rows}")
        
        if not records:
            logger.error(f"Не удалось подготовить ни одной записи из файла {file_path}")
            return 0
        
        logger.info(f"Подготовлено {len(records)} записей для вставки")
        
        # Вставляем данные
        return self.insert_spnet_records(records)
    
    def insert_spnet_records(self, records):
        """Вставка записей SPNet в PostgreSQL"""
        cursor = self.connection.cursor()
        
        try:
            insert_sql = """
            INSERT INTO spnet_traffic (
                total_rows, contract_id, imei, sim_iccid, service, usage_type,
                usage_bytes, usage_unit, total_amount, bill_month, plan_name,
                imsi, msisdn, actual_usage, call_session_count, sp_account_no,
                sp_name, sp_reference, source_file, load_date, created_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            execute_batch(cursor, insert_sql, records)
            self.connection.commit()
            
            return len(records)
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка при вставке данных SPNet: {e}")
            raise
        finally:
            cursor.close()
    
    def load_steccom_files(self):
        """Загрузка STECCOM CSV файлов (пропускает уже загруженные)"""
        logger.info("="*80)
        logger.info("Начинаем загрузку данных STECCOM...")
        logger.info("="*80)
        
        csv_files = glob.glob(f"{self.steccom_path}/*.csv")
        if not csv_files:
            logger.warning(f"CSV файлы STECCOM не найдены в {self.steccom_path}")
            return False
        
        total_records = 0
        skipped_files = 0
        load_start_time = datetime.now()
        
        for file_path in csv_files:
            file_name = Path(file_path).name
            try:
                # Проверяем, загружен ли файл уже
                is_loaded, records_in_file, records_in_db = self.is_file_loaded(file_name, 'steccom_expenses', file_path)
                if is_loaded:
                    logger.info(f"\n⏭ Пропускаем файл (уже загружен полностью): {file_name}")
                    if records_in_file > 0 and records_in_db > 0:
                        logger.info(f"   Записей в файле: {records_in_file:,}, в базе: {records_in_db:,}")
                    skipped_files += 1
                    continue
                elif records_in_db > 0:
                    logger.info(f"\n⚠️ Файл загружен не полностью: {file_name}")
                    logger.info(f"   Записей в файле: {records_in_file:,}, в базе: {records_in_db:,} (не хватает {records_in_file - records_in_db:,})")
                    logger.info(f"   Перезагружаем файл...")
                
                logger.info(f"\nОбрабатываем файл: {file_name}")
                load_start = datetime.now()
                records_loaded = self.load_steccom_file(file_path)
                load_end = datetime.now()
                total_records += records_loaded
                
                # Логируем успешную загрузку
                self.log_load_result('steccom_expenses', file_name, records_loaded, 'SUCCESS')
                logger.info(f"✓ Загружено {records_loaded} записей")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"✗ Ошибка при обработке файла {file_path}: {error_msg}")
                # Логируем ошибку
                self.log_load_result('steccom_expenses', file_name, 0, 'FAILED', error_msg)
        
        load_end_time = datetime.now()
        duration = (load_end_time - load_start_time).total_seconds()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Загрузка STECCOM завершена")
        logger.info(f"Всего загружено: {total_records:,} записей")
        logger.info(f"Пропущено файлов (уже загружены): {skipped_files}")
        logger.info(f"Время выполнения: {duration:.2f} сек")
        logger.info(f"{'='*80}\n")
        
        return True
    
    def load_steccom_file(self, file_path):
        """Загрузка одного STECCOM CSV файла"""
        # Читаем CSV
        df = pd.read_csv(file_path, dtype=str, na_filter=False)
        
        df['source_file'] = Path(file_path).name
        df['load_date'] = datetime.now()
        df['created_by'] = 'STECCOM_LOADER'
        
        # Подготавливаем записи
        records = []
        for _, row in df.iterrows():
            record = (
                self.parse_date(row.get('Invoice Date')),
                row.get('Company Name') or None,
                self.parse_number(row.get('Company Number')),
                self.parse_number(row.get('Settling Period')),
                row.get('Fee Type') or None,
                row.get('Contract ID') or None,
                row.get('IMSI/ISDNA') or None,
                row.get('ICC-ID/IMEI') or None,
                self.parse_date(row.get('Activation Date')),
                self.parse_date(row.get('Transaction Date')),
                row.get('Service') or None,
                row.get('Rate Type') or None,
                row.get('Plan/Discount') or None,
                row.get('Description') or None,
                self.parse_number(row.get('Prorated Days')),
                self.parse_number(row.get('Amount')),
                row.get('Group') or None,
                row.get('source_file'),
                row.get('load_date'),
                row.get('created_by')
            )
            records.append(record)
        
        # Вставляем данные
        return self.insert_steccom_records(records)
    
    def insert_steccom_records(self, records):
        """Вставка записей STECCOM в PostgreSQL"""
        cursor = self.connection.cursor()
        
        try:
            insert_sql = """
            INSERT INTO steccom_expenses (
                invoice_date, company_name, company_number, settling_period,
                fee_type, contract_id, imsi_isdna, icc_id_imei, activation_date,
                transaction_date, service, rate_type, plan_discount, description,
                prorated_days, amount, group_name, source_file, load_date, created_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            execute_batch(cursor, insert_sql, records)
            self.connection.commit()
            
            return len(records)
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка при вставке данных STECCOM: {e}")
            raise
        finally:
            cursor.close()
    
    def parse_date(self, date_str):
        """Парсинг даты"""
        if not date_str or str(date_str).strip() == '':
            return None
        
        date_formats = [
            '%Y/%m/%d',
            '%d.%m.%Y',
            '%Y-%m-%d',
            '%d/%m/%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except:
                continue
        
        return None
    
    def parse_number(self, value):
        """Парсинг числового значения"""
        if not value or str(value).strip() == '':
            return None
        
        try:
            clean_value = str(value).strip()
            
            # Научная нотация
            if 'E+' in clean_value or 'E-' in clean_value:
                clean_value = clean_value.replace(',', '.')
            
            # Убираем валютные символы
            clean_value = clean_value.replace('$', '').replace('€', '').replace(' ', '')
            
            # Убираем нечисловые символы
            import re
            clean_value = re.sub(r'[^\d\.\-\+E]', '', clean_value)
            
            if clean_value and clean_value != '-':
                return float(clean_value)
            
            return None
        except:
            return None
    
    def get_statistics(self):
        """Получение статистики загрузки"""
        cursor = self.connection.cursor()
        
        try:
            logger.info("\n" + "="*80)
            logger.info("СТАТИСТИКА ЗАГРУЖЕННЫХ ДАННЫХ")
            logger.info("="*80)
            
            # SPNet статистика
            cursor.execute("SELECT COUNT(*) FROM spnet_traffic")
            spnet_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT imei) FROM spnet_traffic WHERE imei IS NOT NULL")
            spnet_imeis = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(usage_bytes) FROM spnet_traffic")
            spnet_usage = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT SUM(total_amount) FROM spnet_traffic")
            spnet_amount = cursor.fetchone()[0] or 0
            
            logger.info(f"\nSPNET_TRAFFIC:")
            logger.info(f"  Всего записей: {spnet_count:,}")
            logger.info(f"  Уникальных IMEI: {spnet_imeis:,}")
            logger.info(f"  Общий трафик: {spnet_usage:,.0f} байт ({spnet_usage/1000/1000:.2f} MB)")
            logger.info(f"  Общая сумма: ${spnet_amount:,.2f}")
            
            # STECCOM статистика
            cursor.execute("SELECT COUNT(*) FROM steccom_expenses")
            steccom_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT icc_id_imei) FROM steccom_expenses WHERE icc_id_imei IS NOT NULL")
            steccom_imeis = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(amount) FROM steccom_expenses")
            steccom_amount = cursor.fetchone()[0] or 0
            
            logger.info(f"\nSTECCOM_EXPENSES:")
            logger.info(f"  Всего записей: {steccom_count:,}")
            logger.info(f"  Уникальных IMEI: {steccom_imeis:,}")
            logger.info(f"  Общая сумма: ${steccom_amount:,.2f}")
            
            # Тарифные планы
            cursor.execute("SELECT plan_code, plan_name, COUNT(*) FROM spnet_traffic WHERE plan_name IS NOT NULL GROUP BY plan_code, plan_name")
            plans = cursor.fetchall()
            
            logger.info(f"\nТАРИФНЫЕ ПЛАНЫ:")
            for plan in plans:
                logger.info(f"  {plan[0] or 'Unknown'}: {plan[1]} - {plan[2]:,} записей")
            
            logger.info("\n" + "="*80 + "\n")
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
        finally:
            cursor.close()
    
    def close(self):
        """Закрытие подключения"""
        if self.connection:
            self.connection.close()
            logger.info("Подключение к PostgreSQL закрыто\n")


def main():
    """Основная функция"""
    
    # Конфигурация PostgreSQL (аналог Oracle billing7/billing@bm7)
    db_config = {
        'dbname': 'billing',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': 5432
    }
    
    logger.info("="*80)
    logger.info("ЗАГРУЗКА ДАННЫХ В POSTGRESQL")
    logger.info("="*80)
    
    loader = PostgresDataLoader(db_config)
    
    try:
        # Подключение
        if not loader.connect():
            logger.error("Не удалось подключиться к PostgreSQL")
            return False
        
        # Загрузка SPNet
        loader.load_spnet_files()
        
        # Загрузка STECCOM
        loader.load_steccom_files()
        
        # Статистика
        loader.get_statistics()
        
        logger.info("✓ Загрузка данных завершена успешно!")
        return True
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        loader.close()


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

