#!/usr/bin/env python3
"""
Скрипт загрузки данных SPNet трафика в Oracle
Загружает данные об использовании Iridium M2M из CSV файлов в таблицу SPNET_TRAFFIC
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
        logging.FileHandler('load_spnet.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SPNetDataLoader:
    def __init__(self, oracle_config):
        """
        Инициализация загрузчика данных SPNet
        
        Args:
            oracle_config (dict): Конфигурация подключения к Oracle
        """
        self.oracle_config = oracle_config
        self.connection = None
        self.gdrive_path = "SPNet reports"
        
    def connect_to_oracle(self):
        """Подключение к Oracle базе данных (прямое подключение без TNS)"""
        try:
            # Прямое подключение без TNS
            self.connection = cx_Oracle.connect(
                self.oracle_config['username'],
                self.oracle_config['password'],
                f"{self.oracle_config['host']}:{self.oracle_config['port']}/{self.oracle_config['service_name']}"
            )
            logger.info("Успешное подключение к Oracle")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к Oracle: {e}")
            return False
    
    def load_spnet_files(self):
        """Загрузка всех CSV файлов SPNet"""
        logger.info("Начинаем загрузку данных SPNet...")
        
        csv_files = glob.glob(f"{self.gdrive_path}/*.csv")
        if not csv_files:
            logger.warning("CSV файлы SPNet не найдены")
            return False
        
        total_records = 0
        load_start_time = datetime.now()
        
        for file_path in csv_files:
            try:
                logger.info(f"Обрабатываем файл: {file_path}")
                records_loaded = self.load_single_file(file_path)
                total_records += records_loaded
                logger.info(f"Загружено {records_loaded} записей из {file_path}")
                
            except Exception as e:
                logger.error(f"Ошибка при обработке файла {file_path}: {e}")
                self.log_load_error('SPNET_TRAFFIC', file_path, str(e))
        
        load_end_time = datetime.now()
        duration = (load_end_time - load_start_time).total_seconds()
        
        # Логируем общий результат
        self.log_load_success('SPNET_TRAFFIC', 'ALL_FILES', total_records, load_start_time, load_end_time, duration)
        
        logger.info(f"Загрузка SPNet завершена. Всего загружено: {total_records} записей")
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
            
            df['source_file'] = Path(file_path).name
            df['load_date'] = datetime.now()
            df['created_by'] = 'SPNET_LOADER'
            
            # Подготавливаем данные для вставки
            records = []
            for _, row in df.iterrows():
                record = {
                    'total_rows': self.parse_number(row.get('Total Rows', None)),
                    'contract_id': str(row.get('Contract ID', '')).strip() if row.get('Contract ID') else None,
                    'imei': str(row.get('IMEI', '')).strip() if row.get('IMEI') else None,
                    'sim_iccid': str(row.get('SIM (ICCID)', '')).strip() if row.get('SIM (ICCID)') else None,
                    'service': str(row.get('Service', '')).strip() if row.get('Service') else None,
                    'usage_type': str(row.get('Usage Type', '')).strip() if row.get('Usage Type') else None,
                    'usage_bytes': self.parse_number(row.get('Usage', None)),
                    'usage_unit': str(row.get('Usage Unit', '')).strip() if row.get('Usage Unit') else None,
                    'total_amount': self.parse_number(row.get('Total Amount', None)),
                    'bill_month': self.parse_number(row.get('Bill Month', None)),
                    'plan_name': str(row.get('Plan Name', '')).strip() if row.get('Plan Name') else None,
                    'imsi': str(row.get('IMSI', '')).strip() if row.get('IMSI') else None,
                    'msisdn': str(row.get('MSISDN', '')).strip() if row.get('MSISDN') else None,
                    'actual_usage': self.parse_number(row.get('Actual Usage', None)),
                    'call_session_count': self.parse_number(row.get('Call/Session Count', None)),
                    'sp_account_no': self.parse_number(row.get('SP Account No', None)),
                    'sp_name': str(row.get('SP Name', '')).strip() if row.get('SP Name') else None,
                    'sp_reference': str(row.get('SP Reference', '')).strip() if row.get('SP Reference') else None,
                    'source_file': row.get('source_file', None),
                    'load_date': row.get('load_date', None),
                    'created_by': row.get('created_by', None)
                }
                records.append(record)
            
            # Вставляем данные в Oracle
            return self.insert_records(records)
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {file_path}: {e}")
            raise
    
    def insert_records(self, records):
        """Вставка записей в Oracle"""
        if not self.connection:
            raise Exception("Нет подключения к Oracle")
        
        cursor = self.connection.cursor()
        
        try:
            # SQL для вставки
            insert_sql = """
            INSERT INTO SPNET_TRAFFIC (
                TOTAL_ROWS, CONTRACT_ID, IMEI, SIM_ICCID, SERVICE, USAGE_TYPE,
                USAGE_BYTES, USAGE_UNIT, TOTAL_AMOUNT, BILL_MONTH, PLAN_NAME,
                IMSI, MSISDN, ACTUAL_USAGE, CALL_SESSION_COUNT, SP_ACCOUNT_NO,
                SP_NAME, SP_REFERENCE, SOURCE_FILE, LOAD_DATE, CREATED_BY
            ) VALUES (
                :total_rows, :contract_id, :imei, :sim_iccid, :service, :usage_type,
                :usage_bytes, :usage_unit, :total_amount, :bill_month, :plan_name,
                :imsi, :msisdn, :actual_usage, :call_session_count, :sp_account_no,
                :sp_name, :sp_reference, :source_file, :load_date, :created_by
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
    
    def log_load_success(self, table_name, source_file, records_count, start_time, end_time, duration):
        """Логирование успешной загрузки"""
        cursor = self.connection.cursor()
        try:
            insert_sql = """
            INSERT INTO LOAD_LOGS (
                TABLE_NAME, SOURCE_FILE, RECORDS_LOADED, LOAD_STATUS,
                LOAD_START_TIME, LOAD_END_TIME, LOAD_DURATION_SECONDS, CREATED_BY
            ) VALUES (
                :table_name, :source_file, :records_count, 'SUCCESS',
                :start_time, :end_time, :duration, 'SPNET_LOADER'
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
            insert_sql = """
            INSERT INTO LOAD_LOGS (
                TABLE_NAME, SOURCE_FILE, RECORDS_LOADED, LOAD_STATUS,
                ERROR_MESSAGE, LOAD_START_TIME, LOAD_END_TIME, CREATED_BY
            ) VALUES (
                :table_name, :source_file, 0, 'ERROR',
                :error_message, SYSDATE, SYSDATE, 'SPNET_LOADER'
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
            cursor.execute("SELECT COUNT(*) FROM SPNET_TRAFFIC")
            total_records = cursor.fetchone()[0]
            
            # Количество уникальных IMEI
            cursor.execute("SELECT COUNT(DISTINCT IMEI) FROM SPNET_TRAFFIC")
            unique_imeis = cursor.fetchone()[0]
            
            # Общий объем данных
            cursor.execute("SELECT SUM(USAGE_BYTES) FROM SPNET_TRAFFIC")
            total_usage = cursor.fetchone()[0] or 0
            
            # Общая сумма
            cursor.execute("SELECT SUM(TOTAL_AMOUNT) FROM SPNET_TRAFFIC")
            total_amount = cursor.fetchone()[0] or 0
            
            logger.info(f"Статистика загрузки SPNet:")
            logger.info(f"  Всего записей: {total_records:,}")
            logger.info(f"  Уникальных IMEI: {unique_imeis:,}")
            logger.info(f"  Общий объем данных: {total_usage:,} байт ({total_usage/1000/1000:.2f} MB)")
            logger.info(f"  Общая сумма: ${total_amount:.2f}")
            
            return {
                'total_records': total_records,
                'unique_imeis': unique_imeis,
                'total_usage_bytes': total_usage,
                'total_amount': total_amount
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return None
        finally:
            cursor.close()
    
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
    
    def close_connection(self):
        """Закрытие подключения к Oracle"""
        if self.connection:
            self.connection.close()
            logger.info("Подключение к Oracle закрыто")

def main():
    """Основная функция"""
    # Конфигурация Oracle (прямое подключение из интранет)
    oracle_config = {
        'host': '192.168.3.35',  # Прямой доступ к Oracle
        'port': 1521,
        'service_name': 'bm7',  # Имя сервиса Oracle
        'username': 'billing7',  # Пользователь Oracle
        'password': 'billing'   # Пароль Oracle
    }
    
    logger.info("Запуск загрузчика данных SPNet...")
    
    # Создаем загрузчик
    loader = SPNetDataLoader(oracle_config)
    
    try:
        # Подключаемся к Oracle
        if not loader.connect_to_oracle():
            logger.error("Не удалось подключиться к Oracle")
            return False
        
        # Загружаем данные
        if loader.load_spnet_files():
            # Получаем статистику
            stats = loader.get_load_statistics()
            if stats:
                logger.info("Загрузка SPNet завершена успешно!")
                return True
            else:
                logger.error("Не удалось получить статистику")
                return False
        else:
            logger.error("Ошибка при загрузке данных SPNet")
            return False
            
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return False
    finally:
        loader.close_connection()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

