#!/usr/bin/env python3
"""
Модуль для работы с базой данных пользователей (SQLite) - Версия 2
Управление учетными записями для системы отчетов по Iridium с поддержкой прав доступа к вкладкам
"""

import sqlite3
import bcrypt
import json
from datetime import datetime
from pathlib import Path
import os

# Путь к базе данных
DB_PATH = Path(__file__).resolve().parent.parent / "users.db"

# Список всех доступных вкладок в системе
AVAILABLE_TABS = {
    'assistant': '🤖 Биллинг ассистент',
    'kb_expansion': '📚 Расширение KB',
    'confluence_librarian': '🛰️ Спутниковый ассистент',
    'report': '💰 Расходы Иридиум',
    'revenue': '💰 Доходы',
    'bills': '📄 Рассылка счетов',
    'analytics': '📋 Счета за период',
    'loader': '📥 Data Loader',
    'campaigns': '📧 Кампании',
}

_FALLBACK_TAB_LABELS = {
    "bills": "📄 Рассылка счетов",
    "campaigns": "📧 Кампании",
}
for _fk, _fl in _FALLBACK_TAB_LABELS.items():
    if _fk not in AVAILABLE_TABS:
        AVAILABLE_TABS[_fk] = _fl


def get_db_connection():
    """Создать подключение к базе данных SQLite"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Инициализировать базу данных и создать таблицу пользователей"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем, существует ли таблица users
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='users'
    """)
    table_exists = cursor.fetchone() is not None
    
    if table_exists:
        # Проверяем, есть ли колонка allowed_tabs
        cursor.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'allowed_tabs' not in columns:
            # Добавляем колонку allowed_tabs
            cursor.execute('''
                ALTER TABLE users ADD COLUMN allowed_tabs TEXT DEFAULT NULL
            ''')
            print("✅ Добавлена колонка allowed_tabs в таблицу users")
            
            # Обновляем существующих суперпользователей - даем им доступ ко всем вкладкам
            all_tabs = json.dumps(list(AVAILABLE_TABS.keys()))
            cursor.execute('''
                UPDATE users 
                SET allowed_tabs = ? 
                WHERE is_superuser = 1
            ''', (all_tabs,))
            print("✅ Обновлены права суперпользователей")
    else:
        # Создаем таблицу с нуля
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_superuser INTEGER DEFAULT 0,
                allowed_tabs TEXT DEFAULT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT,
                last_login TEXT
            )
        ''')
        print("✅ Создана таблица users")
    
    conn.commit()
    conn.close()

def hash_password(password):
    """Хешировать пароль с помощью bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password, password_hash):
    """Проверить пароль против хеша"""
    return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))

def create_user(username, password, is_superuser=False, allowed_tabs=None, created_by=None):
    """
    Создать нового пользователя
    
    Args:
        username: Имя пользователя
        password: Пароль
        is_superuser: Является ли суперпользователем (имеет доступ ко всем вкладкам)
        allowed_tabs: Список разрешенных вкладок (ключи из AVAILABLE_TABS)
        created_by: Кто создал пользователя
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Проверка, существует ли пользователь
        cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
        if cursor.fetchone():
            return False, "Пользователь с таким именем уже существует"
        
        # Если суперпользователь, даем доступ ко всем вкладкам
        if is_superuser:
            allowed_tabs = list(AVAILABLE_TABS.keys())
        
        # Если allowed_tabs не указан, даем доступ только к основным вкладкам
        if allowed_tabs is None:
            allowed_tabs = ['report', 'revenue']  # По умолчанию только отчеты
        
        # Валидация allowed_tabs
        if not isinstance(allowed_tabs, list):
            return False, "allowed_tabs должен быть списком"
        
        # Проверяем, что все вкладки существуют
        invalid_tabs = [tab for tab in allowed_tabs if tab not in AVAILABLE_TABS]
        if invalid_tabs:
            return False, f"Неизвестные вкладки: {', '.join(invalid_tabs)}"
        
        # Создание пользователя
        password_hash = hash_password(password)
        created_at = datetime.now().isoformat()
        allowed_tabs_json = json.dumps(allowed_tabs)
        
        cursor.execute('''
            INSERT INTO users (username, password_hash, is_superuser, allowed_tabs, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (username, password_hash, 1 if is_superuser else 0, allowed_tabs_json, created_at, created_by))
        
        conn.commit()
        return True, f"Пользователь '{username}' успешно создан"
    except sqlite3.Error as e:
        return False, f"Ошибка при создании пользователя: {str(e)}"
    finally:
        conn.close()

def authenticate_user(username, password):
    """
    Аутентифицировать пользователя
    
    Returns:
        (success, username, is_superuser, allowed_tabs)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT password_hash, is_superuser, allowed_tabs 
            FROM users 
            WHERE username = ?
        ''', (username,))
        user = cursor.fetchone()
        
        if not user:
            return False, None, False, []
        
        password_hash = user['password_hash']
        is_superuser = bool(user['is_superuser'])
        allowed_tabs_json = user['allowed_tabs']
        
        # Парсим allowed_tabs
        if allowed_tabs_json:
            try:
                allowed_tabs = json.loads(allowed_tabs_json)
            except json.JSONDecodeError:
                allowed_tabs = []
        else:
            # Если allowed_tabs не установлен, даем доступ к базовым вкладкам
            allowed_tabs = ['report', 'revenue']
        
        # Суперпользователи имеют доступ ко всем вкладкам
        if is_superuser:
            allowed_tabs = list(AVAILABLE_TABS.keys())
        
        if verify_password(password, password_hash):
            # Обновить время последнего входа
            cursor.execute('''
                UPDATE users SET last_login = ? WHERE username = ?
            ''', (datetime.now().isoformat(), username))
            conn.commit()
            return True, username, is_superuser, allowed_tabs
        else:
            return False, None, False, []
    except sqlite3.Error as e:
        return False, None, False, []
    finally:
        conn.close()

def update_user_permissions(username, allowed_tabs):
    """
    Обновить права доступа пользователя к вкладкам
    
    Args:
        username: Имя пользователя
        allowed_tabs: Список разрешенных вкладок (ключи из AVAILABLE_TABS)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Проверяем, существует ли пользователь
        cursor.execute('SELECT id, is_superuser FROM users WHERE username = ?', (username,))
        user = cursor.fetchone()
        
        if not user:
            return False, "Пользователь не найден"
        
        # Нельзя изменять права суперпользователя
        if user['is_superuser']:
            return False, "Нельзя изменять права суперпользователя"
        
        # Валидация allowed_tabs
        if not isinstance(allowed_tabs, list):
            return False, "allowed_tabs должен быть списком"
        
        # Проверяем, что все вкладки существуют
        invalid_tabs = [tab for tab in allowed_tabs if tab not in AVAILABLE_TABS]
        if invalid_tabs:
            return False, f"Неизвестные вкладки: {', '.join(invalid_tabs)}"
        
        # Обновляем права
        allowed_tabs_json = json.dumps(allowed_tabs)
        cursor.execute('''
            UPDATE users SET allowed_tabs = ? WHERE username = ?
        ''', (allowed_tabs_json, username))
        
        conn.commit()
        return True, f"Права пользователя '{username}' успешно обновлены"
    except sqlite3.Error as e:
        return False, f"Ошибка при обновлении прав: {str(e)}"
    finally:
        conn.close()

def get_user_permissions(username):
    """
    Получить права доступа пользователя
    
    Returns:
        (success, allowed_tabs)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT is_superuser, allowed_tabs 
            FROM users 
            WHERE username = ?
        ''', (username,))
        user = cursor.fetchone()
        
        if not user:
            return False, []
        
        is_superuser = bool(user['is_superuser'])
        allowed_tabs_json = user['allowed_tabs']
        
        # Суперпользователи имеют доступ ко всем вкладкам
        if is_superuser:
            return True, list(AVAILABLE_TABS.keys())
        
        # Парсим allowed_tabs
        if allowed_tabs_json:
            try:
                allowed_tabs = json.loads(allowed_tabs_json)
                return True, allowed_tabs
            except json.JSONDecodeError:
                return True, ['report', 'revenue']  # Базовые права по умолчанию
        else:
            return True, ['report', 'revenue']  # Базовые права по умолчанию
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
    """Получить список всех пользователей с их правами"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT username, is_superuser, allowed_tabs, created_at, created_by, last_login
            FROM users
            ORDER BY created_at DESC
        ''')
        users = cursor.fetchall()
        
        # Преобразуем allowed_tabs из JSON в список
        result = []
        for user in users:
            user_dict = dict(user)
            if user_dict['allowed_tabs']:
                try:
                    user_dict['allowed_tabs'] = json.loads(user_dict['allowed_tabs'])
                except json.JSONDecodeError:
                    user_dict['allowed_tabs'] = []
            else:
                user_dict['allowed_tabs'] = []
            
            # Суперпользователи имеют доступ ко всем вкладкам
            if user_dict['is_superuser']:
                user_dict['allowed_tabs'] = list(AVAILABLE_TABS.keys())
            
            result.append(user_dict)
        
        return result
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
else:
    # Обновляем существующую базу данных (добавляем колонку allowed_tabs если её нет)
    init_db()
