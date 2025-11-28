#!/usr/bin/env python3
"""
Скрипт загрузки данных STECCOM расходов в Oracle
Загружает данные биллинга Iridium M2M из CSV файлов в таблицу STECCOM_EXPENSES
"""

import pandas as pd
import cx_Oracle
import glob
import os
from pathlib import Path
import logging
from datetime import datetime
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('load_steccom.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class STECCOMDataLoader:
    def __init__(self, oracle_config):
        """
        Инициализация загрузчика данных STECCOM
        
        Args:
            oracle_config (dict): Конфигурация подключения к Oracle
        """
        self.oracle_config = oracle_config
        self.connection = None
        self.gdrive_path = "STECCOMLLCRussiaSBD.AccessFees_reports"
        
    def connect_to_oracle(self):
        """Подключение к Oracle базе данных (с поддержкой SID и SERVICE_NAME)"""
        try:
            # Используем SID если задан, иначе SERVICE_NAME
            if self.oracle_config.get('sid'):
                dsn = cx_Oracle.makedsn(
                    self.oracle_config['host'],
                    self.oracle_config['port'],
                    sid=self.oracle_config['sid']
                )
            else:
                service_name = self.oracle_config.get('service_name', 'bm7')
                dsn = cx_Oracle.makedsn(
                    self.oracle_config['host'],
                    self.oracle_config['port'],
                    service_name=service_name
                )
            
            self.connection = cx_Oracle.connect(
                user=self.oracle_config['username'],
                password=self.oracle_config['password'],
                dsn=dsn
            )
            logger.info("Успешное подключение к Oracle")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к Oracle: {e}")
            return False
    
    def load_steccom_files(self):
        """Загрузка всех CSV файлов STECCOM (пропускает уже загруженные)"""
        logger.info("Начинаем загрузку данных STECCOM...")
        
        csv_files = glob.glob(f"{self.gdrive_path}/*.csv")
        if not csv_files:
            logger.warning("CSV файлы STECCOM не найдены")
            return False
        
        total_records = 0
        skipped_files = 0
        load_start_time = datetime.now()
        
        for file_path in csv_files:
            file_name = Path(file_path).name
            try:
                # Проверяем, загружен ли файл уже
                is_loaded, records_in_file, records_in_db = self.is_file_loaded(file_name, 'STECCOM_EXPENSES', file_path)
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
                
                logger.info(f"Обрабатываем файл: {file_name}")
                file_start_time = datetime.now()
                records_loaded = self.load_single_file(file_path)
                file_end_time = datetime.now()
                file_duration = (file_end_time - file_start_time).total_seconds()
                total_records += records_loaded
                
                # Логируем успешную загрузку файла
                self.log_load_success('STECCOM_EXPENSES', file_name, records_loaded, file_start_time, file_end_time, file_duration)
                logger.info(f"✓ Загружено {records_loaded} записей из {file_name}")
                
            except Exception as e:
                logger.error(f"✗ Ошибка при обработке файла {file_path}: {e}")
                self.log_load_error('STECCOM_EXPENSES', file_name, str(e))
        
        load_end_time = datetime.now()
        duration = (load_end_time - load_start_time).total_seconds()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Загрузка STECCOM завершена")
        logger.info(f"Всего загружено: {total_records:,} записей")
        logger.info(f"Пропущено файлов (уже загружены): {skipped_files}")
        logger.info(f"Время выполнения: {duration:.2f} сек")
        logger.info(f"{'='*80}\n")
        
        return True
    
    def load_single_file(self, file_path):
        """Загрузка одного CSV файла"""
        try:
            # Пробуем разные кодировки для чтения файла
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            
            for encoding in encodings:
                try:
                    # Пробуем разные разделители
                    separators = [';', '\t', ',']
                    for sep in separators:
                        try:
                            df = pd.read_csv(file_path, sep=sep, encoding=encoding, 
                                           dtype=str,  # Читаем все как строки
                                           na_filter=False)  # Не конвертируем в NaN
                            if len(df.columns) > 1:
                                logger.info(f"Успешно прочитан файл {file_path} с разделителем '{sep}' и кодировкой {encoding}")
                                break
                        except Exception as e:
                            logger.warning(f"Ошибка чтения {file_path} с разделителем '{sep}' и кодировкой {encoding}: {e}")
                            continue
                    if len(df.columns) > 1:
                        break
                except Exception as e:
                    logger.warning(f"Ошибка чтения {file_path} с кодировкой {encoding}: {e}")
                    continue
            
            if df is None or len(df.columns) <= 1:
                logger.error(f"Не удалось прочитать файл {file_path}")
                return 0
            
            # Если данные в одной колонке, пытаемся разделить
            if len(df.columns) == 2:
                main_col = df.columns[0]
                split_data = df[main_col].str.split(';', expand=True)
                
                if len(split_data.columns) >= 17:
                    column_names = [
                        'Invoice Date', 'Company Name', 'Company Number', 'Settling Period',
                        'Fee Type', 'Contract ID', 'IMSI/ISDNA', 'ICC-ID/IMEI', 'Activation Date',
                        'Transaction Date', 'Service', 'Rate Type', 'Plan/Discount', 'Description',
                        'Prorated Days', 'Amount', 'Group'
                    ]
                    split_data.columns = column_names[:len(split_data.columns)]
                    df = split_data
            
            df['source_file'] = Path(file_path).name
            df['load_date'] = datetime.now()
            df['created_by'] = 'STECCOM_LOADER'
            
            # Подготавливаем данные для вставки (исключаем только BROADBAND)
            records = []
            skipped_broadband = 0
            for _, row in df.iterrows():
                # Пропускаем записи с SERVICE = 'BROADBAND'
                service = str(row.get('Service', '')).strip().upper() if row.get('Service') else ''
                if service == 'BROADBAND':
                    skipped_broadband += 1
                    continue
                
                record = {
                    'invoice_date': self.parse_date(row.get('Invoice Date')),
                    'company_name': row.get('Company Name'),
                    'company_number': self.parse_number(row.get('Company Number')),
                    'settling_period': self.parse_number(row.get('Settling Period')),
                    'fee_type': row.get('Fee Type'),
                    'contract_id': row.get('Contract ID'),
                    'imsi_isdna': row.get('IMSI/ISDNA'),
                    'icc_id_imei': row.get('ICC-ID/IMEI'),
                    'activation_date': self.parse_date(row.get('Activation Date')),
                    'transaction_date': self.parse_date(row.get('Transaction Date')),
                    'service': row.get('Service'),
                    'rate_type': row.get('Rate Type'),
                    'plan_discount': row.get('Plan/Discount'),
                    'description': row.get('Description'),
                    'prorated_days': self.parse_number(row.get('Prorated Days')),
                    'amount': self.parse_amount(row.get('Amount')),
                    'group_name': row.get('Group'),
                    'source_file': row.get('source_file'),
                    'load_date': row.get('load_date'),
                    'created_by': row.get('created_by')
                }
                records.append(record)
            
            if skipped_broadband > 0:
                logger.info(f"Пропущено {skipped_broadband} записей с SERVICE = 'BROADBAND'")
            
            # Вставляем данные в Oracle
            return self.insert_records(records)
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {file_path}: {e}")
            raise
    
    def parse_date(self, date_str):
        """Парсинг даты"""
        if pd.isna(date_str) or date_str is None or str(date_str).strip() == '':
            return None
        
        try:
            # Пробуем разные форматы даты
            date_formats = [
                '%d.%m.%Y',      # 02.02.2025
                '%Y-%m-%d',      # 2025-10-02
                '%d/%m/%Y',      # 02/02/2025
                '%Y/%m/%d',      # 2025/10/02
                '%d-%m-%Y',      # 02-02-2025
                '%Y.%m.%d'       # 2025.10.02
            ]
            for fmt in date_formats:
                try:
                    return datetime.strptime(str(date_str).strip(), fmt).date()
                except:
                    continue
            logger.warning(f"Не удалось распарсить дату '{date_str}'")
            return None
        except Exception as e:
            logger.warning(f"Ошибка парсинга даты '{date_str}': {e}")
            return None
    
    def parse_number(self, value):
        """Парсинг числового значения"""
        if pd.isna(value) or value is None or str(value).strip() == '':
            return None
        
        try:
            # Обрабатываем научную нотацию с запятой
            clean_value = str(value).strip()
            
            # Если это научная нотация с запятой (например, 3,00025E+14)
            if 'E+' in clean_value or 'E-' in clean_value:
                clean_value = clean_value.replace(',', '.')
            
            # Убираем валютные символы и пробелы
            clean_value = clean_value.replace('$', '').replace('€', '').replace('₽', '').replace(' ', '')
            
            # Убираем все нечисловые символы кроме точки, минуса и E
            import re
            clean_value = re.sub(r'[^\d\.\-\+E]', '', clean_value)
            
            if clean_value and clean_value != '-':
                return float(clean_value)
            else:
                return None
        except Exception as e:
            logger.warning(f"Не удалось распарсить число '{value}': {e}")
            return None
    
    def parse_amount(self, amount_str):
        """Парсинг суммы"""
        if pd.isna(amount_str) or amount_str is None:
            return None
        
        try:
            # Убираем валютные символы и пробелы
            clean_amount = str(amount_str).replace('$', '').replace('€', '').replace(' ', '')
            return float(clean_amount) if clean_amount else None
        except:
            return None
    
    def insert_records(self, records):
        """Вставка записей в Oracle"""
        if not self.connection:
            raise Exception("Нет подключения к Oracle")
        
        cursor = self.connection.cursor()
        
        try:
            # SQL для вставки
            insert_sql = """
            INSERT INTO STECCOM_EXPENSES (
                INVOICE_DATE, COMPANY_NAME, COMPANY_NUMBER, SETTLING_PERIOD,
                FEE_TYPE, CONTRACT_ID, IMSI_ISDNA, ICC_ID_IMEI, ACTIVATION_DATE,
                TRANSACTION_DATE, SERVICE, RATE_TYPE, PLAN_DISCOUNT, DESCRIPTION,
                PRORATED_DAYS, AMOUNT, GROUP_NAME, SOURCE_FILE, LOAD_DATE, CREATED_BY
            ) VALUES (
                :invoice_date, :company_name, :company_number, :settling_period,
                :fee_type, :contract_id, :imsi_isdna, :icc_id_imei, :activation_date,
                :transaction_date, :service, :rate_type, :plan_discount, :description,
                :prorated_days, :amount, :group_name, :source_file, :load_date, :created_by
            )
            """
            
            # Выполняем вставку
            cursor.executemany(insert_sql, records)
            self.connection.commit()
            
            return len(records)
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка при вставке данных: {e}")
            raise
        finally:
            cursor.close()
    
    def _get_load_logs_columns(self):
        """Определение структуры таблицы LOAD_LOGS (динамически)"""
        cursor = self.connection.cursor()
        try:
            # Проверяем FILE_NAME vs SOURCE_FILE
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
                    logger.warning("Не удалось определить структуру LOAD_LOGS, используем SOURCE_FILE по умолчанию")
                    file_col = "SOURCE_FILE"
            
            # Проверяем LOADED_BY vs CREATED_BY
            try:
                test_query = "SELECT LOADED_BY FROM LOAD_LOGS WHERE ROWNUM = 1"
                cursor.execute(test_query)
                loaded_by_col = "LOADED_BY"
            except:
                try:
                    test_query = "SELECT CREATED_BY FROM LOAD_LOGS WHERE ROWNUM = 1"
                    cursor.execute(test_query)
                    loaded_by_col = "CREATED_BY"
                except:
                    logger.warning("Не удалось определить столбец для created_by, используем CREATED_BY по умолчанию")
                    loaded_by_col = "CREATED_BY"
            
            return file_col, loaded_by_col
        except Exception as e:
            logger.warning(f"Ошибка при определении структуры LOAD_LOGS: {e}, используем значения по умолчанию")
            return "SOURCE_FILE", "CREATED_BY"
        finally:
            cursor.close()
    
    def is_file_loaded(self, file_name, table_name='STECCOM_EXPENSES', file_path=None):
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
            file_col, _ = self._get_load_logs_columns()
            # Проверяем наличие записи в load_logs
            cursor.execute(f"""
                SELECT COUNT(*) FROM LOAD_LOGS 
                WHERE UPPER({file_col}) = UPPER(:1) 
                AND UPPER(TABLE_NAME) = UPPER(:2)
                AND LOAD_STATUS = 'SUCCESS'
            """, (file_name, table_name))
            has_log_entry = cursor.fetchone()[0] > 0
            
            # Проверяем количество записей в базе
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE UPPER(SOURCE_FILE) = UPPER(:1)
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
    
    def log_load_success(self, table_name, source_file, records_count, start_time, end_time, duration):
        """Логирование успешной загрузки"""
        cursor = self.connection.cursor()
        try:
            # Определяем структуру таблицы динамически
            file_col, loaded_by_col = self._get_load_logs_columns()
            
            insert_sql = f"""
            INSERT INTO LOAD_LOGS (
                TABLE_NAME, {file_col}, RECORDS_LOADED, LOAD_STATUS,
                LOAD_START_TIME, LOAD_END_TIME, LOAD_DURATION_SECONDS, {loaded_by_col}
            ) VALUES (
                :table_name, :source_file, :records_count, 'SUCCESS',
                :start_time, :end_time, :duration, 'STECCOM_LOADER'
            )
            """
            cursor.execute(insert_sql, {
                'table_name': table_name,
                'source_file': source_file,
                'records_count': records_count,
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration
            })
            self.connection.commit()
        except Exception as e:
            logger.error(f"Ошибка при логировании: {e}")
        finally:
            cursor.close()
    
    def log_load_error(self, table_name, source_file, error_message):
        """Логирование ошибки загрузки"""
        cursor = self.connection.cursor()
        try:
            # Определяем структуру таблицы динамически
            file_col, loaded_by_col = self._get_load_logs_columns()
            
            insert_sql = f"""
            INSERT INTO LOAD_LOGS (
                TABLE_NAME, {file_col}, RECORDS_LOADED, LOAD_STATUS,
                ERROR_MESSAGE, LOAD_START_TIME, LOAD_END_TIME, {loaded_by_col}
            ) VALUES (
                :table_name, :source_file, 0, 'ERROR',
                :error_message, SYSDATE, SYSDATE, 'STECCOM_LOADER'
            )
            """
            cursor.execute(insert_sql, {
                'table_name': table_name,
                'source_file': source_file,
                'error_message': error_message
            })
            self.connection.commit()
        except Exception as e:
            logger.error(f"Ошибка при логировании ошибки: {e}")
        finally:
            cursor.close()
    
    def get_load_statistics(self):
        """Получение статистики загрузки"""
        cursor = self.connection.cursor()
        try:
            # Общее количество записей
            cursor.execute("SELECT COUNT(*) FROM STECCOM_EXPENSES")
            total_records = cursor.fetchone()[0]
            
            # Количество уникальных IMEI
            cursor.execute("SELECT COUNT(DISTINCT ICC_ID_IMEI) FROM STECCOM_EXPENSES")
            unique_imeis = cursor.fetchone()[0]
            
            # Общая сумма расходов
            cursor.execute("SELECT SUM(AMOUNT) FROM STECCOM_EXPENSES")
            total_amount = cursor.fetchone()[0] or 0
            
            # Средняя сумма
            cursor.execute("SELECT AVG(AMOUNT) FROM STECCOM_EXPENSES")
            avg_amount = cursor.fetchone()[0] or 0
            
            logger.info(f"Статистика загрузки STECCOM:")
            logger.info(f"  Всего записей: {total_records:,}")
            logger.info(f"  Уникальных IMEI: {unique_imeis:,}")
            logger.info(f"  Общая сумма расходов: ${total_amount:.2f}")
            logger.info(f"  Средняя сумма: ${avg_amount:.2f}")
            
            return {
                'total_records': total_records,
                'unique_imeis': unique_imeis,
                'total_amount': total_amount,
                'avg_amount': avg_amount
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return None
        finally:
            cursor.close()
    
    def close_connection(self):
        """Закрытие подключения к Oracle"""
        if self.connection:
            self.connection.close()
            logger.info("Подключение к Oracle закрыто")

def main():
    """Основная функция"""
    # Конфигурация Oracle (прямое подключение из интранет)
    oracle_config = {
        'host': os.getenv('ORACLE_HOST'),  # Из переменной окружения
        'port': int(os.getenv('ORACLE_PORT', '1521')),
        'service_name': os.getenv('ORACLE_SERVICE', os.getenv('ORACLE_SID')),  # Имя сервиса Oracle
        'username': os.getenv('ORACLE_USER'),  # Пользователь Oracle
        'password': os.getenv('ORACLE_PASSWORD')   # Пароль Oracle
    }
    
    # Проверка обязательных параметров
    required_params = ['host', 'username', 'password', 'service_name']
    missing = [p for p in required_params if not oracle_config.get(p)]
    if missing:
        logger.error(f"❌ Ошибка: Не установлены переменные окружения: {', '.join(missing)}")
        return
    
    logger.info("Запуск загрузчика данных STECCOM...")
    
    # Создаем загрузчик
    loader = STECCOMDataLoader(oracle_config)
    
    try:
        # Подключаемся к Oracle
        if not loader.connect_to_oracle():
            logger.error("Не удалось подключиться к Oracle")
            return False
        
        # Загружаем данные
        if loader.load_steccom_files():
            # Получаем статистику
            stats = loader.get_load_statistics()
            if stats:
                logger.info("Загрузка STECCOM завершена успешно!")
                return True
            else:
                logger.error("Не удалось получить статистику")
                return False
        else:
            logger.error("Ошибка при загрузке данных STECCOM")
            return False
            
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return False
    finally:
        loader.close_connection()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

