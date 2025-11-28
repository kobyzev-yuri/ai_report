#!/usr/bin/env python3
"""
Скрипт для создания и управления пользователями в системе отчетов по Iridium
Использование:
    python create_user.py create --username admin --password secret123 --superuser
    python create_user.py list
    python create_user.py change-password --username admin --password newpass123
    python create_user.py delete --username user1
"""

import argparse
import sys
from auth_db import (
    init_db, create_user, list_users, change_password, 
    delete_user, is_superuser
)

def main():
    parser = argparse.ArgumentParser(
        description='Управление пользователями системы отчетов по Iridium',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Примеры использования:
  # Создать суперпользователя
  python create_user.py create --username admin --password secret123 --superuser
  
  # Создать обычного пользователя
  python create_user.py create --username user1 --password pass123
  
  # Список всех пользователей
  python create_user.py list
  
  # Изменить пароль
  python create_user.py change-password --username admin --password newpass123
  
  # Удалить пользователя
  python create_user.py delete --username user1
        '''
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Команда для выполнения')
    
    # Команда создания пользователя
    create_parser = subparsers.add_parser('create', help='Создать нового пользователя')
    create_parser.add_argument('--username', required=True, help='Имя пользователя')
    create_parser.add_argument('--password', required=True, help='Пароль')
    create_parser.add_argument('--superuser', action='store_true', help='Сделать суперпользователем')
    
    # Команда списка пользователей
    list_parser = subparsers.add_parser('list', help='Показать список всех пользователей')
    
    # Команда изменения пароля
    change_parser = subparsers.add_parser('change-password', help='Изменить пароль пользователя')
    change_parser.add_argument('--username', required=True, help='Имя пользователя')
    change_parser.add_argument('--password', required=True, help='Новый пароль')
    
    # Команда удаления пользователя
    delete_parser = subparsers.add_parser('delete', help='Удалить пользователя')
    delete_parser.add_argument('--username', required=True, help='Имя пользователя')
    
    args = parser.parse_args()
    
    # Инициализация базы данных
    init_db()
    
    if args.command == 'create':
        success, message = create_user(
            args.username, 
            args.password, 
            is_superuser=args.superuser
        )
        if success:
            print(f"✓ {message}")
            sys.exit(0)
        else:
            print(f"✗ {message}")
            sys.exit(1)
    
    elif args.command == 'list':
        users = list_users()
        if not users:
            print("Пользователи не найдены.")
            return
        
        print("\n" + "=" * 80)
        print("СПИСОК ПОЛЬЗОВАТЕЛЕЙ")
        print("=" * 80)
        for user in users:
            superuser_mark = " [SUPERUSER]" if user['is_superuser'] else ""
            print(f"\nИмя пользователя: {user['username']}{superuser_mark}")
            print(f"  Создан: {user['created_at']}")
            if user['created_by']:
                print(f"  Создан пользователем: {user['created_by']}")
            if user['last_login']:
                print(f"  Последний вход: {user['last_login']}")
        print("\n" + "=" * 80)
    
    elif args.command == 'change-password':
        success, message = change_password(args.username, args.password)
        if success:
            print(f"✓ {message}")
            sys.exit(0)
        else:
            print(f"✗ {message}")
            sys.exit(1)
    
    elif args.command == 'delete':
        success, message = delete_user(args.username)
        if success:
            print(f"✓ {message}")
            sys.exit(0)
        else:
            print(f"✗ {message}")
            sys.exit(1)
    
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    main()

