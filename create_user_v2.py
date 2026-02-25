#!/usr/bin/env python3
"""
Скрипт для создания и управления пользователями в системе отчетов по Iridium - Версия 2
С поддержкой управления правами доступа к вкладкам

Использование:
    # Создать суперпользователя (доступ ко всем вкладкам)
    python create_user_v2.py create --username admin --password secret123 --superuser
    
    # Создать пользователя с доступом к конкретным вкладкам
    python create_user_v2.py create --username user1 --password pass123 --tabs report revenue
    
    # Список всех пользователей с их правами
    python create_user_v2.py list
    
    # Обновить права пользователя
    python create_user_v2.py update-permissions --username user1 --tabs assistant report revenue analytics
    
    # Показать доступные вкладки
    python create_user_v2.py show-tabs
    
    # Изменить пароль
    python create_user_v2.py change-password --username admin --password newpass123
    
    # Удалить пользователя
    python create_user_v2.py delete --username user1
"""

import argparse
import sys
from auth_db_v2 import (
    init_db, create_user, list_users, change_password, 
    delete_user, is_superuser, update_user_permissions,
    get_user_permissions, AVAILABLE_TABS
)

def main():
    parser = argparse.ArgumentParser(
        description='Управление пользователями системы отчетов по Iridium (v2 с правами доступа)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Примеры использования:
  # Создать суперпользователя (доступ ко всем вкладкам)
  python create_user_v2.py create --username admin --password secret123 --superuser
  
  # Создать пользователя с доступом только к отчетам
  python create_user_v2.py create --username user1 --password pass123 --tabs report revenue
  
  # Создать пользователя с доступом к ассистенту и расширению KB
  python create_user_v2.py create --username analyst --password pass123 --tabs assistant kb_expansion report
  
  # Список всех пользователей с их правами
  python create_user_v2.py list
  
  # Показать все доступные вкладки
  python create_user_v2.py show-tabs
  
  # Обновить права пользователя
  python create_user_v2.py update-permissions --username user1 --tabs assistant report revenue analytics
  
  # Изменить пароль
  python create_user_v2.py change-password --username admin --password newpass123
  
  # Удалить пользователя
  python create_user_v2.py delete --username user1

Доступные вкладки:
  assistant        - 🤖 Ассистент (RAG-ассистент для SQL)
  kb_expansion     - 📚 Расширение KB (добавление примеров)
  report           - 💰 Расходы Иридиум (основной отчет)
  revenue          - 💰 Доходы (счета-фактуры)
  analytics        - 📋 Счета за период (ANALYTICS)
  loader           - 📥 Data Loader (загрузка данных)
        '''
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Команда для выполнения')
    
    # Команда создания пользователя
    create_parser = subparsers.add_parser('create', help='Создать нового пользователя')
    create_parser.add_argument('--username', required=True, help='Имя пользователя')
    create_parser.add_argument('--password', required=True, help='Пароль')
    create_parser.add_argument('--superuser', action='store_true', help='Сделать суперпользователем (доступ ко всем вкладкам)')
    create_parser.add_argument('--tabs', nargs='+', help='Список разрешенных вкладок (ключи из show-tabs)')
    
    # Команда списка пользователей
    list_parser = subparsers.add_parser('list', help='Показать список всех пользователей с их правами')
    
    # Команда показа доступных вкладок
    show_tabs_parser = subparsers.add_parser('show-tabs', help='Показать все доступные вкладки')
    
    # Команда обновления прав пользователя
    update_parser = subparsers.add_parser('update-permissions', help='Обновить права доступа пользователя')
    update_parser.add_argument('--username', required=True, help='Имя пользователя')
    update_parser.add_argument('--tabs', nargs='+', required=True, help='Список разрешенных вкладок')
    
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
        # Валидация вкладок
        allowed_tabs = args.tabs if args.tabs else None
        
        if allowed_tabs:
            invalid_tabs = [tab for tab in allowed_tabs if tab not in AVAILABLE_TABS]
            if invalid_tabs:
                print(f"✗ Неизвестные вкладки: {', '.join(invalid_tabs)}")
                print(f"\nДоступные вкладки: {', '.join(AVAILABLE_TABS.keys())}")
                print("\nИспользуйте команду 'show-tabs' для просмотра всех вкладок")
                sys.exit(1)
        
        success, message = create_user(
            args.username, 
            args.password, 
            is_superuser=args.superuser,
            allowed_tabs=allowed_tabs
        )
        if success:
            print(f"✓ {message}")
            
            # Показываем права пользователя
            success_perm, user_tabs = get_user_permissions(args.username)
            if success_perm:
                print(f"\nРазрешенные вкладки:")
                for tab in user_tabs:
                    print(f"  • {tab}: {AVAILABLE_TABS.get(tab, 'Unknown')}")
            sys.exit(0)
        else:
            print(f"✗ {message}")
            sys.exit(1)
    
    elif args.command == 'list':
        users = list_users()
        if not users:
            print("Пользователи не найдены.")
            return
        
        print("\n" + "=" * 100)
        print("СПИСОК ПОЛЬЗОВАТЕЛЕЙ С ПРАВАМИ ДОСТУПА")
        print("=" * 100)
        for user in users:
            superuser_mark = " [SUPERUSER]" if user['is_superuser'] else ""
            print(f"\n👤 Имя пользователя: {user['username']}{superuser_mark}")
            print(f"   Создан: {user['created_at']}")
            if user['created_by']:
                print(f"   Создан пользователем: {user['created_by']}")
            if user['last_login']:
                print(f"   Последний вход: {user['last_login']}")
            
            # Показываем разрешенные вкладки
            print(f"   Разрешенные вкладки:")
            if user['is_superuser']:
                print(f"      👑 ВСЕ ВКЛАДКИ (суперпользователь)")
            else:
                allowed_tabs = user.get('allowed_tabs', [])
                if allowed_tabs:
                    for tab in allowed_tabs:
                        tab_name = AVAILABLE_TABS.get(tab, 'Unknown')
                        print(f"      • {tab}: {tab_name}")
                else:
                    print(f"      ⚠️  Нет доступа к вкладкам")
        print("\n" + "=" * 100)
    
    elif args.command == 'show-tabs':
        print("\n" + "=" * 100)
        print("ДОСТУПНЫЕ ВКЛАДКИ В СИСТЕМЕ")
        print("=" * 100)
        print("\nКлюч вкладки → Название в интерфейсе\n")
        for key, name in AVAILABLE_TABS.items():
            print(f"  {key:20} → {name}")
        print("\n" + "=" * 100)
        print("\nПримеры использования:")
        print("  # Дать доступ только к отчетам")
        print("  python create_user_v2.py create --username user1 --password pass --tabs report revenue")
        print("\n  # Дать доступ к ассистенту и расширению KB")
        print("  python create_user_v2.py create --username analyst --password pass --tabs assistant kb_expansion")
        print("\n  # Дать доступ ко всем вкладкам (суперпользователь)")
        print("  python create_user_v2.py create --username admin --password pass --superuser")
        print()
    
    elif args.command == 'update-permissions':
        # Валидация вкладок
        invalid_tabs = [tab for tab in args.tabs if tab not in AVAILABLE_TABS]
        if invalid_tabs:
            print(f"✗ Неизвестные вкладки: {', '.join(invalid_tabs)}")
            print(f"\nДоступные вкладки: {', '.join(AVAILABLE_TABS.keys())}")
            print("\nИспользуйте команду 'show-tabs' для просмотра всех вкладок")
            sys.exit(1)
        
        success, message = update_user_permissions(args.username, args.tabs)
        if success:
            print(f"✓ {message}")
            
            # Показываем обновленные права
            print(f"\nОбновленные разрешенные вкладки:")
            for tab in args.tabs:
                print(f"  • {tab}: {AVAILABLE_TABS.get(tab, 'Unknown')}")
            sys.exit(0)
        else:
            print(f"✗ {message}")
            sys.exit(1)
    
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
