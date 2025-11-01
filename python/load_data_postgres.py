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
    
    def load_spnet_files(self):
        """Загрузка SPNet CSV файлов"""
        logger.info("="*80)
        logger.info("Начинаем загрузку данных SPNet...")
        logger.info("="*80)
        
        csv_files = glob.glob(f"{self.spnet_path}/*.csv")
        if not csv_files:
            logger.warning(f"CSV файлы SPNet не найдены в {self.spnet_path}")
            return False
        
        total_records = 0
        load_start_time = datetime.now()
        
        for file_path in csv_files:
            try:
                logger.info(f"\nОбрабатываем файл: {Path(file_path).name}")
                records_loaded = self.load_spnet_file(file_path)
                total_records += records_loaded
                logger.info(f"✓ Загружено {records_loaded} записей")
                
            except Exception as e:
                logger.error(f"✗ Ошибка при обработке файла {file_path}: {e}")
        
        load_end_time = datetime.now()
        duration = (load_end_time - load_start_time).total_seconds()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Загрузка SPNet завершена")
        logger.info(f"Всего загружено: {total_records:,} записей")
        logger.info(f"Время выполнения: {duration:.2f} сек")
        logger.info(f"{'='*80}\n")
        
        return True
    
    def load_spnet_file(self, file_path):
        """Загрузка одного SPNet CSV файла"""
        # Читаем CSV
        df = pd.read_csv(file_path, dtype=str, na_filter=False)
        
        df['source_file'] = Path(file_path).name
        df['load_date'] = datetime.now()
        df['created_by'] = 'SPNET_LOADER'
        
        # Подготавливаем записи
        records = []
        for _, row in df.iterrows():
            record = (
                self.parse_number(row.get('Total Rows')),
                row.get('Contract ID') or None,
                row.get('IMEI') or None,
                row.get('SIM (ICCID)') or None,
                row.get('Service') or None,
                row.get('Usage Type') or None,
                self.parse_number(row.get('Usage')),
                row.get('Usage Unit') or None,
                self.parse_number(row.get('Total Amount')),
                self.parse_number(row.get('Bill Month')),
                row.get('Plan Name') or None,
                row.get('IMSI') or None,
                row.get('MSISDN') or None,
                self.parse_number(row.get('Actual Usage')),
                self.parse_number(row.get('Call/Session Count')),
                self.parse_number(row.get('SP Account No')),
                row.get('SP Name') or None,
                row.get('SP Reference') or None,
                row.get('source_file'),
                row.get('load_date'),
                row.get('created_by')
            )
            records.append(record)
        
        # Вставляем данные
        return self.insert_spnet_records(records)
    
    def insert_spnet_records(self, records):
        """Вставка записей SPNet в PostgreSQL"""
        cursor = self.connection.cursor()
        
        try:
            insert_sql = """
            INSERT INTO spnet_traffic (
                TOTAL_ROWS, CONTRACT_ID, IMEI, SIM_ICCID, SERVICE, USAGE_TYPE,
                USAGE_BYTES, USAGE_UNIT, TOTAL_AMOUNT, BILL_MONTH, PLAN_NAME,
                IMSI, MSISDN, ACTUAL_USAGE, CALL_SESSION_COUNT, SP_ACCOUNT_NO,
                SP_NAME, SP_REFERENCE, SOURCE_FILE, LOAD_DATE, CREATED_BY
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
        """Загрузка STECCOM CSV файлов"""
        logger.info("="*80)
        logger.info("Начинаем загрузку данных STECCOM...")
        logger.info("="*80)
        
        csv_files = glob.glob(f"{self.steccom_path}/*.csv")
        if not csv_files:
            logger.warning(f"CSV файлы STECCOM не найдены в {self.steccom_path}")
            return False
        
        total_records = 0
        load_start_time = datetime.now()
        
        for file_path in csv_files:
            try:
                logger.info(f"\nОбрабатываем файл: {Path(file_path).name}")
                records_loaded = self.load_steccom_file(file_path)
                total_records += records_loaded
                logger.info(f"✓ Загружено {records_loaded} записей")
                
            except Exception as e:
                logger.error(f"✗ Ошибка при обработке файла {file_path}: {e}")
        
        load_end_time = datetime.now()
        duration = (load_end_time - load_start_time).total_seconds()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Загрузка STECCOM завершена")
        logger.info(f"Всего загружено: {total_records:,} записей")
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
            INSERT INTO STECCOM_EXPENSES (
                INVOICE_DATE, COMPANY_NAME, COMPANY_NUMBER, SETTLING_PERIOD,
                FEE_TYPE, CONTRACT_ID, IMSI_ISDNA, ICC_ID_IMEI, ACTIVATION_DATE,
                TRANSACTION_DATE, SERVICE, RATE_TYPE, PLAN_DISCOUNT, DESCRIPTION,
                PRORATED_DAYS, AMOUNT, GROUP_NAME, SOURCE_FILE, LOAD_DATE, CREATED_BY
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

