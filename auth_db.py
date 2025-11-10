#!/usr/bin/env python3
"""
Модуль для работы с базой данных пользователей (SQLite)
Управление учетными записями для системы отчетов по Iridium
"""

import sqlite3
import bcrypt
from datetime import datetime
from pathlib import Path
import os

# Путь к базе данных
DB_PATH = Path(__file__).parent / 'users.db'

def get_db_connection():
    """Создать подключение к базе данных SQLite"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Инициализировать базу данных и создать таблицу пользователей"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_superuser INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            created_by TEXT,
            last_login TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def hash_password(password):
    """Хешировать пароль с помощью bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password, password_hash):
    """Проверить пароль против хеша"""
    return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))

def create_user(username, password, is_superuser=False, created_by=None):
    """Создать нового пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Проверка, существует ли пользователь
        cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
        if cursor.fetchone():
            return False, "Пользователь с таким именем уже существует"
        
        # Создание пользователя
        password_hash = hash_password(password)
        created_at = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO users (username, password_hash, is_superuser, created_at, created_by)
            VALUES (?, ?, ?, ?, ?)
        ''', (username, password_hash, 1 if is_superuser else 0, created_at, created_by))
        
        conn.commit()
        return True, f"Пользователь '{username}' успешно создан"
    except sqlite3.Error as e:
        return False, f"Ошибка при создании пользователя: {str(e)}"
    finally:
        conn.close()

def authenticate_user(username, password):
    """Аутентифицировать пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT password_hash, is_superuser FROM users WHERE username = ?', (username,))
        user = cursor.fetchone()
        
        if not user:
            return False, None, False
        
        password_hash = user['password_hash']
        is_superuser = bool(user['is_superuser'])
        
        if verify_password(password, password_hash):
            # Обновить время последнего входа
            cursor.execute('''
                UPDATE users SET last_login = ? WHERE username = ?
            ''', (datetime.now().isoformat(), username))
            conn.commit()
            return True, username, is_superuser
        else:
            return False, None, False
    except sqlite3.Error as e:
        return False, None, False
    finally:
        conn.close()

def change_password(username, new_password):
    """Изменить пароль пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
        if not cursor.fetchone():
            return False, "Пользователь не найден"
        
        password_hash = hash_password(new_password)
        cursor.execute('''
            UPDATE users SET password_hash = ? WHERE username = ?
        ''', (password_hash, username))
        
        conn.commit()
        return True, f"Пароль для пользователя '{username}' успешно изменен"
    except sqlite3.Error as e:
        return False, f"Ошибка при изменении пароля: {str(e)}"
    finally:
        conn.close()

def delete_user(username):
    """Удалить пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT is_superuser FROM users WHERE username = ?', (username,))
        user = cursor.fetchone()
        
        if not user:
            return False, "Пользователь не найден"
        
        if user['is_superuser']:
            return False, "Нельзя удалить суперпользователя"
        
        cursor.execute('DELETE FROM users WHERE username = ?', (username,))
        conn.commit()
        return True, f"Пользователь '{username}' успешно удален"
    except sqlite3.Error as e:
        return False, f"Ошибка при удалении пользователя: {str(e)}"
    finally:
        conn.close()

def list_users():
    """Получить список всех пользователей"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT username, is_superuser, created_at, created_by, last_login
            FROM users
            ORDER BY created_at DESC
        ''')
        return cursor.fetchall()
    finally:
        conn.close()

def is_superuser(username):
    """Проверить, является ли пользователь суперпользователем"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT is_superuser FROM users WHERE username = ?', (username,))
        user = cursor.fetchone()
        return bool(user['is_superuser']) if user else False
    finally:
        conn.close()

# Инициализация базы данных при импорте модуля
if not DB_PATH.exists():
    init_db()

