#!/usr/bin/env python3
"""
Скрипт для загрузки данных из Oracle views в PostgreSQL tables
Используется для тестирования - копируем данные из production в testing
"""

import cx_Oracle
import psycopg2
import pandas as pd
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Настройки подключения
ORACLE_CONFIG = {
    'user': 'billing7',
    'password': 'billing',
    'dsn': 'bm7'
}

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'billing',
    'user': 'postgres',
    'password': '1234'
}

def get_oracle_connection():
    """Подключение к Oracle"""
    try:
        conn = cx_Oracle.connect(
            user=ORACLE_CONFIG['user'],
            password=ORACLE_CONFIG['password'],
            dsn=ORACLE_CONFIG['dsn'],
            encoding='UTF-8'
        )
        logger.info(f"Подключено к Oracle: {ORACLE_CONFIG['dsn']}")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к Oracle: {e}")
        raise

def get_postgres_connection():
    """Подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            port=POSTGRES_CONFIG['port'],
            database=POSTGRES_CONFIG['database'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
        logger.info(f"Подключено к PostgreSQL: {POSTGRES_CONFIG['database']}")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise

def load_spnet_traffic():
    """Загрузка SPNet traffic из Oracle в PostgreSQL"""
    logger.info("=" * 60)
    logger.info("Загрузка SPNET_TRAFFIC из Oracle")
    
    oracle_conn = get_oracle_connection()
    pg_conn = get_postgres_connection()
    
    try:
        # Читаем из Oracle
        query = """
            SELECT 
                TOTAL_ROWS, CONTRACT_ID, IMEI, SIM_ICCID, SERVICE,
                USAGE_TYPE, USAGE_BYTES, USAGE_UNIT, TOTAL_AMOUNT,
                BILL_MONTH, PLAN_NAME, IMSI, MSISDN, ACTUAL_USAGE,
                CALL_SESSION_COUNT, SP_ACCOUNT_NO, SP_NAME, SP_REFERENCE,
                SOURCE_FILE, LOAD_DATE
            FROM SPNET_TRAFFIC
        """
        logger.info("Чтение данных из Oracle SPNET_TRAFFIC...")
        df = pd.read_sql(query, oracle_conn)
        logger.info(f"Прочитано записей: {len(df)}")
        
        if len(df) == 0:
            logger.warning("Нет данных для загрузки")
            return
        
        # Очистка PostgreSQL таблицы
        cursor = pg_conn.cursor()
        cursor.execute("TRUNCATE TABLE SPNET_TRAFFIC RESTART IDENTITY CASCADE")
        pg_conn.commit()
        logger.info("Таблица PostgreSQL SPNET_TRAFFIC очищена")
        
        # Загрузка в PostgreSQL
        logger.info("Загрузка в PostgreSQL...")
        from io import StringIO
        
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        cursor.copy_from(
            buffer,
            'spnet_traffic',
            columns=df.columns.tolist(),
            null='\\N'
        )
        pg_conn.commit()
        logger.info(f"✓ Загружено записей: {len(df)}")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке SPNET_TRAFFIC: {e}")
        pg_conn.rollback()
        raise
    finally:
        oracle_conn.close()
        pg_conn.close()

def load_steccom_expenses():
    """Загрузка STECCOM expenses из Oracle в PostgreSQL"""
    logger.info("=" * 60)
    logger.info("Загрузка STECCOM_EXPENSES из Oracle")
    
    oracle_conn = get_oracle_connection()
    pg_conn = get_postgres_connection()
    
    try:
        # Читаем из Oracle
        query = """
            SELECT 
                INVOICE_DATE, COMPANY_NAME, COMPANY_NUMBER, SETTLING_PERIOD,
                FEE_TYPE, CONTRACT_ID, IMSI_ISDNA, ICC_ID_IMEI, ACTIVATION_DATE,
                TRANSACTION_DATE, SERVICE, RATE_TYPE, PLAN_DISCOUNT,
                DESCRIPTION, PRORATED_DAYS, AMOUNT, GROUP_NAME,
                SOURCE_FILE, LOAD_DATE
            FROM STECCOM_EXPENSES
        """
        logger.info("Чтение данных из Oracle STECCOM_EXPENSES...")
        df = pd.read_sql(query, oracle_conn)
        logger.info(f"Прочитано записей: {len(df)}")
        
        if len(df) == 0:
            logger.warning("Нет данных для загрузки")
            return
        
        # Очистка PostgreSQL таблицы
        cursor = pg_conn.cursor()
        cursor.execute("TRUNCATE TABLE STECCOM_EXPENSES RESTART IDENTITY CASCADE")
        pg_conn.commit()
        logger.info("Таблица PostgreSQL STECCOM_EXPENSES очищена")
        
        # Загрузка в PostgreSQL
        logger.info("Загрузка в PostgreSQL...")
        from io import StringIO
        
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        cursor.copy_from(
            buffer,
            'steccom_expenses',
            columns=df.columns.tolist(),
            null='\\N'
        )
        pg_conn.commit()
        logger.info(f"✓ Загружено записей: {len(df)}")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке STECCOM_EXPENSES: {e}")
        pg_conn.rollback()
        raise
    finally:
        oracle_conn.close()
        pg_conn.close()

def main():
    """Главная функция"""
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("ЗАГРУЗКА ДАННЫХ ИЗ ORACLE В POSTGRESQL")
    logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    try:
        # Загрузка таблиц
        load_spnet_traffic()
        load_steccom_expenses()
        
        # Итоги
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info("ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО")
        logger.info(f"Время выполнения: {duration:.2f} сек")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке: {e}")
        raise

if __name__ == '__main__':
    main()

























