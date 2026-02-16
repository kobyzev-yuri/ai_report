#!/usr/bin/env python3
"""
Безопасная рассылка email с задержками

Пример использования:
    python send_emails_safely.py \
        --smtp-server smtp.gmail.com \
        --smtp-port 587 \
        --sender your-email@gmail.com \
        --password your-password \
        --recipients-file data/почты для рассылки MVSAT.txt \
        --subject "Важное сообщение" \
        --body "Текст письма" \
        --delay 3.0 \
        --batch-size 10 \
        --batch-delay 60.0
"""

import sys
import argparse
import logging
from pathlib import Path

# Добавляем путь к модулям проекта
sys.path.insert(0, str(Path(__file__).parent.parent))

from tabs.common import send_bulk_emails_safely, load_emails_from_file


def setup_logging(verbose: bool = False):
    """Настройка логирования"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def progress_callback(email: str, success: bool, error: str):
    """Callback для отслеживания прогресса"""
    if success:
        print(f"✓ {email}")
    else:
        print(f"✗ {email}: {error}")


def main():
    parser = argparse.ArgumentParser(
        description='Безопасная рассылка email с задержками',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Базовая рассылка:
   python send_emails_safely.py \\
       --smtp-server smtp.gmail.com \\
       --smtp-port 587 \\
       --sender your-email@gmail.com \\
       --password your-password \\
       --recipients-file emails.txt \\
       --subject "Тема письма" \\
       --body "Текст письма"

2. С задержками и партиями:
   python send_emails_safely.py \\
       --smtp-server smtp.gmail.com \\
       --smtp-port 587 \\
       --sender your-email@gmail.com \\
       --password your-password \\
       --recipients-file emails.txt \\
       --subject "Тема" \\
       --body "Текст" \\
       --delay 5.0 \\
       --batch-size 15 \\
       --batch-delay 120.0

3. С HTML письмом и вложениями:
   python send_emails_safely.py \\
       --smtp-server smtp.gmail.com \\
       --smtp-port 587 \\
       --sender your-email@gmail.com \\
       --password your-password \\
       --recipients-file emails.txt \\
       --subject "Тема" \\
       --body "Текст" \\
       --html-body "<html><body><h1>Привет!</h1></body></html>" \\
       --attachments file1.pdf file2.docx
        """
    )
    
    parser.add_argument('--smtp-server', required=True,
                       help='SMTP сервер (например, smtp.gmail.com)')
    parser.add_argument('--smtp-port', type=int, default=587,
                       help='Порт SMTP (по умолчанию: 587)')
    parser.add_argument('--sender', required=True,
                       help='Email отправителя')
    parser.add_argument('--password', required=True,
                       help='Пароль отправителя')
    parser.add_argument('--recipients-file', required=True,
                       help='Файл со списком email адресов')
    parser.add_argument('--subject', required=True,
                       help='Тема письма')
    parser.add_argument('--body', required=True,
                       help='Текст письма (можно использовать {email} для подстановки)')
    parser.add_argument('--html-body',
                       help='HTML версия письма (опционально)')
    parser.add_argument('--attachments', nargs='*',
                       help='Пути к файлам для вложения')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Задержка между письмами в секундах (по умолчанию: 2.0)')
    parser.add_argument('--batch-size', type=int, default=10,
                       help='Размер партии перед длительной паузой (по умолчанию: 10)')
    parser.add_argument('--batch-delay', type=float, default=60.0,
                       help='Задержка после партии в секундах (по умолчанию: 60.0)')
    parser.add_argument('--start-from', type=int, default=0,
                       help='Начать с указанного индекса (для возобновления)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Подробный вывод')
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    # Загружаем список получателей
    logging.info(f"Загрузка email адресов из {args.recipients_file}")
    try:
        recipients = load_emails_from_file(args.recipients_file)
        logging.info(f"Загружено {len(recipients)} email адресов")
    except Exception as e:
        logging.error(f"Ошибка при загрузке файла: {e}")
        sys.exit(1)
    
    if not recipients:
        logging.error("Список получателей пуст!")
        sys.exit(1)
    
    # Проверяем вложения
    attachments = []
    if args.attachments:
        for att_path in args.attachments:
            if Path(att_path).exists():
                attachments.append(att_path)
            else:
                logging.warning(f"Файл не найден: {att_path}")
    
    # Запрашиваем подтверждение
    print(f"\n{'='*60}")
    print(f"Параметры рассылки:")
    print(f"  Отправитель: {args.sender}")
    print(f"  Получателей: {len(recipients)}")
    print(f"  Тема: {args.subject}")
    print(f"  Задержка между письмами: {args.delay} сек")
    print(f"  Размер партии: {args.batch_size}")
    print(f"  Задержка после партии: {args.batch_delay} сек")
    print(f"  Начать с индекса: {args.start_from}")
    if attachments:
        print(f"  Вложения: {', '.join(attachments)}")
    print(f"{'='*60}\n")
    
    confirm = input("Продолжить рассылку? (yes/no): ").strip().lower()
    if confirm not in ['yes', 'y', 'да', 'д']:
        print("Рассылка отменена.")
        sys.exit(0)
    
    # Запускаем рассылку
    try:
        results = send_bulk_emails_safely(
            smtp_server=args.smtp_server,
            smtp_port=args.smtp_port,
            sender_email=args.sender,
            sender_password=args.password,
            recipients=recipients,
            subject=args.subject,
            body_text=args.body,
            body_html=args.html_body,
            attachments=attachments if attachments else None,
            delay_between_emails=args.delay,
            delay_after_batch=args.batch_delay,
            batch_size=args.batch_size,
            progress_callback=progress_callback,
            start_from_index=args.start_from
        )
        
        # Выводим итоги
        print(f"\n{'='*60}")
        print(f"Итоги рассылки:")
        print(f"  Всего: {results['total']}")
        print(f"  Успешно: {results['success']}")
        print(f"  Ошибок: {results['failed']}")
        print(f"{'='*60}\n")
        
        if results['failed'] > 0:
            print("Email с ошибками:")
            for email, error in results['errors'].items():
                print(f"  {email}: {error}")
        
        sys.exit(0 if results['failed'] == 0 else 1)
        
    except KeyboardInterrupt:
        print("\n\nРассылка прервана пользователем.")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

