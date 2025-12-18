#!/usr/bin/env python3
"""
Получение email из CRM по коду 1C
Использование: python get_email_from_crm.py CODE_1C
Пример: python get_email_from_crm.py 1C00123
"""

import sys
import re
import pyodbc
from typing import Optional


def get_handle_crm():
    """Подключение к CRM через ODBC"""
    dsn = 'DSN=crmdb'
    user = 'bpm'
    password = 'bpm'
    
    try:
        conn = pyodbc.connect(f'{dsn};UID={user};PWD={password}')
        return conn
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")


def is_valid_email(email: str) -> bool:
    """
    Валидация email адреса
    
    Args:
        email: Email адрес для проверки
        
    Returns:
        True если email валидный, False иначе
    """
    if not email:
        return False
    
    # Базовый паттерн для email
    # Email должен содержать: локальная часть @ домен
    # Локальная часть: буквы, цифры, точки, дефисы, подчеркивания
    # Домен: буквы, цифры, точки, дефисы, минимум одна точка
    email_pattern = r'^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if not re.match(email_pattern, email):
        return False
    
    # Дополнительные проверки
    # Не должен начинаться или заканчиваться точкой или дефисом
    if email.startswith(('.', '_', '-')) or email.endswith(('.', '_', '-')):
        return False
    
    # Не должно быть двух точек подряд
    if '..' in email:
        return False
    
    # Должен быть хотя бы один символ до @
    if email.startswith('@'):
        return False
    
    # Должен быть хотя бы один символ после @
    if email.endswith('@'):
        return False
    
    # Разделяем на локальную часть и домен
    parts = email.split('@')
    if len(parts) != 2:
        return False
    
    local_part, domain = parts
    
    # Локальная часть не должна быть пустой
    if not local_part:
        return False
    
    # Домен должен содержать хотя бы одну точку
    if '.' not in domain:
        return False
    
    # Домен не должен начинаться или заканчиваться точкой или дефисом
    if domain.startswith(('.', '-')) or domain.endswith(('.', '-')):
        return False
    
    return True


def get_email_by_code1c(code_1c: str) -> Optional[str]:
    """
    Получение email из CRM по коду 1C с валидацией
    
    Args:
        code_1c: Код 1C клиента
        
    Returns:
        Валидный email адрес или None если не найден или невалидный
    """
    conn = get_handle_crm()
    cursor = conn.cursor()
    
    # SQL запрос для получения email (поле Web)
    # Используем параметризованный запрос для безопасности
    sql = """
    SELECT Web 
    FROM [Account]
    WHERE Web LIKE '%@%'
      AND Code1C = ?
    """
    
    try:
        cursor.execute(sql, code_1c)
        row = cursor.fetchone()
        
        if row:
            web_value = row[0]
            
            # Проверяем, что это валидный email
            if is_valid_email(web_value):
                return web_value
            else:
                # Логируем невалидный email для отладки
                print(f"Warning: Found invalid email in Web field for Code1C={code_1c}: '{web_value}'", 
                      file=sys.stderr)
                return None
        
        return None
    finally:
        cursor.close()
        conn.close()


def main():
    """Основная функция"""
    if len(sys.argv) < 2:
        print("Usage: python get_email_from_crm.py CODE_1C")
        print("Example: python get_email_from_crm.py 1C00123")
        sys.exit(1)
    
    code_1c = sys.argv[1]
    
    try:
        email = get_email_by_code1c(code_1c)
        
        if email:
            print(email)
            sys.exit(0)
        else:
            print(f"Email not found for Code1C: {code_1c}", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

