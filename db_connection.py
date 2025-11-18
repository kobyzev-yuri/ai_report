#!/usr/bin/env python3
"""
Абстракция подключения к базам данных (PostgreSQL и Oracle)
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any

def load_config_env():
    """Загрузка config.env если переменные окружения не установлены"""
    config_file = Path(__file__).parent / 'config.env'
    if config_file.exists():
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"\'')
                    if not os.getenv(key):
                        os.environ[key] = value

def get_db_type():
    """Получение типа БД из переменной окружения DB_TYPE (postgresql или oracle)"""
    db_type = os.getenv('DB_TYPE', 'postgresql').lower()
    return db_type if db_type in ['postgresql', 'oracle'] else 'postgresql'

# Загружаем config.env при импорте
load_config_env()

def get_postgres_config() -> Dict[str, Any]:
    """Получение конфигурации PostgreSQL"""
    return {
        'dbname': os.getenv('POSTGRES_DB', 'billing'),
        'user': os.getenv('POSTGRES_USER', 'cnn'),
        'password': os.getenv('POSTGRES_PASSWORD', ''),
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', '5432'))
    }

def get_oracle_config() -> Dict[str, Any]:
    """Получение конфигурации Oracle"""
    # Поддержка как ORACLE_SID, так и ORACLE_SERVICE для совместимости
    service_name = os.getenv('ORACLE_SID') or os.getenv('ORACLE_SERVICE', 'your_service')
    return {
        'user': os.getenv('ORACLE_USER', 'your_user'),
        'password': os.getenv('ORACLE_PASSWORD', 'your_password'),
        'host': os.getenv('ORACLE_HOST', 'localhost'),
        'port': int(os.getenv('ORACLE_PORT', '1521')),
        'service_name': service_name,
        'username': os.getenv('ORACLE_USER', 'your_user')  # Для совместимости с загрузчиками
    }

def get_db_connection(db_type: str = 'postgresql'):
    """
    Создание подключения к базе данных
    
    Args:
        db_type: 'postgresql' или 'oracle'
    
    Returns:
        Connection object или None
    """
    if db_type == 'postgresql':
        try:
            import psycopg2
            config = get_postgres_config()
            if not config['password']:
                return None
            return psycopg2.connect(**config)
        except ImportError:
            raise ImportError("psycopg2 not installed. Install with: pip install psycopg2-binary")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None
    
    elif db_type == 'oracle':
        try:
            import cx_Oracle
            config = get_oracle_config()
            dsn = cx_Oracle.makedsn(
                config['host'],
                config['port'],
                service_name=config['service_name']
            )
            return cx_Oracle.connect(
                user=config['user'],
                password=config['password'],
                dsn=dsn
            )
        except ImportError:
            raise ImportError("cx_Oracle not installed. Install with: pip install cx_Oracle")
        except Exception as e:
            print(f"Error connecting to Oracle: {e}")
            return None
    
    else:
        raise ValueError(f"Unknown database type: {db_type}")

def get_data_loader(db_type: str = 'postgresql'):
    """
    Получение загрузчика данных для указанной БД
    
    Args:
        db_type: 'postgresql' или 'oracle'
    
    Returns:
        DataLoader instance
    """
    if db_type == 'postgresql':
        from python.load_data_postgres import PostgresDataLoader
        return PostgresDataLoader(get_postgres_config())
    
    elif db_type == 'oracle':
        # Для Oracle нужны отдельные загрузчики для SPNet и STECCOM
        from python.load_spnet_traffic import SPNetDataLoader
        from python.load_steccom_expenses import STECCOMDataLoader
        return {
            'spnet': SPNetDataLoader(get_oracle_config()),
            'steccom': STECCOMDataLoader(get_oracle_config())
        }
    
    else:
        raise ValueError(f"Unknown database type: {db_type}")

