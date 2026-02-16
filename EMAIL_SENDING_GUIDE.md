# Руководство по безопасной рассылке email

## Обзор

Для избежания блокировки как спамер, рассылка настроена с задержками между письмами и дополнительными паузами после партий писем.

## Основные принципы

1. **Задержка между письмами**: 2-5 секунд между каждым письмом
2. **Партии**: После каждых 10-20 писем делается длительная пауза (60+ секунд)
3. **Обработка ошибок**: Автоматические повторы при временных ошибках
4. **Логирование**: Подробные логи для отслеживания прогресса

## Использование

### Через скрипт (рекомендуется)

```bash
python scripts/send_emails_safely.py \
    --smtp-server smtp.gmail.com \
    --smtp-port 587 \
    --sender your-email@gmail.com \
    --password your-app-password \
    --recipients-file "data/почты для рассылки MVSAT.txt" \
    --subject "Тема письма" \
    --body "Текст письма" \
    --delay 3.0 \
    --batch-size 10 \
    --batch-delay 60.0
```

### Параметры

- `--smtp-server`: SMTP сервер (Gmail: `smtp.gmail.com`, Яндекс: `smtp.yandex.ru`)
- `--smtp-port`: Порт (обычно 587 для TLS)
- `--sender`: Email отправителя
- `--password`: Пароль или App Password (для Gmail нужен App Password)
- `--recipients-file`: Файл со списком email адресов
- `--subject`: Тема письма
- `--body`: Текст письма
- `--html-body`: HTML версия письма (опционально)
- `--attachments`: Файлы для вложения (опционально)
- `--delay`: Задержка между письмами в секундах (по умолчанию: 2.0)
- `--batch-size`: Размер партии перед паузой (по умолчанию: 10)
- `--batch-delay`: Задержка после партии в секундах (по умолчанию: 60.0)
- `--start-from`: Начать с указанного индекса (для возобновления)

### Через Python код

```python
from tabs.common import send_bulk_emails_safely, load_emails_from_file

# Загружаем список получателей
recipients = load_emails_from_file("data/почты для рассылки MVSAT.txt")

# Отправляем рассылку
results = send_bulk_emails_safely(
    smtp_server="smtp.gmail.com",
    smtp_port=587,
    sender_email="your-email@gmail.com",
    sender_password="your-password",
    recipients=recipients,
    subject="Тема письма",
    body_text="Текст письма",
    delay_between_emails=3.0,  # 3 секунды между письмами
    delay_after_batch=60.0,    # 60 секунд после каждых 10 писем
    batch_size=10
)

print(f"Успешно: {results['success']}, Ошибок: {results['failed']}")
```

## Настройка для разных почтовых сервисов

### Gmail

1. Включите двухфакторную аутентификацию
2. Создайте App Password: https://myaccount.google.com/apppasswords
3. Используйте App Password вместо обычного пароля

```bash
--smtp-server smtp.gmail.com \
--smtp-port 587 \
--sender your-email@gmail.com \
--password your-app-password
```

### Яндекс

```bash
--smtp-server smtp.yandex.ru \
--smtp-port 587 \
--sender your-email@yandex.ru \
--password your-password
```

### Mail.ru

```bash
--smtp-server smtp.mail.ru \
--smtp-port 587 \
--sender your-email@mail.ru \
--password your-password
```

## Рекомендации по задержкам

| Количество писем | Задержка между письмами | Размер партии | Задержка после партии |
|-----------------|------------------------|---------------|----------------------|
| До 50           | 2-3 сек                | 10            | 30 сек               |
| 50-200          | 3-5 сек                | 10-15         | 60 сек               |
| 200-500         | 5-10 сек               | 10-15         | 120 сек              |
| Более 500       | 10+ сек                | 5-10          | 180+ сек             |

## Возобновление после ошибки

Если рассылка прервалась, можно возобновить с нужного индекса:

```bash
python scripts/send_emails_safely.py \
    ... \
    --start-from 150  # Начнет с 151-го письма
```

## Формат файла с email адресами

Поддерживаются два формата:

1. **Один email на строку:**
```
email1@example.com
email2@example.com
email3@example.com
```

2. **Список через запятую:**
```
email1@example.com, email2@example.com, email3@example.com
```

3. **Комбинация:**
```
email1@example.com
email2@example.com, email3@example.com
email4@example.com
```

Строки, начинающиеся с `#`, игнорируются как комментарии.

## Персонализация

Можно использовать `{email}` в теме и тексте для подстановки адреса получателя:

```bash
--subject "Письмо для {email}" \
--body "Здравствуйте! Это письмо отправлено на {email}"
```

## Обработка ошибок

Скрипт автоматически:
- Повторяет отправку при временных ошибках (до 3 попыток)
- Пропускает неверные email адреса
- Логирует все ошибки
- Продолжает работу при ошибках отдельных писем

## Безопасность

⚠️ **Важно:**
- Не храните пароли в коде
- Используйте переменные окружения для паролей
- Для Gmail обязательно используйте App Password
- Не отправляйте слишком много писем за короткое время

## Примеры использования

### Простая рассылка

```bash
python scripts/send_emails_safely.py \
    --smtp-server smtp.gmail.com \
    --smtp-port 587 \
    --sender sender@gmail.com \
    --password $GMAIL_APP_PASSWORD \
    --recipients-file emails.txt \
    --subject "Важное сообщение" \
    --body "Текст письма"
```

### С HTML и вложениями

```bash
python scripts/send_emails_safely.py \
    --smtp-server smtp.gmail.com \
    --smtp-port 587 \
    --sender sender@gmail.com \
    --password $GMAIL_APP_PASSWORD \
    --recipients-file emails.txt \
    --subject "Отчет" \
    --body "См. вложение" \
    --html-body "<html><body><h1>Отчет</h1><p>См. вложение</p></body></html>" \
    --attachments report.pdf document.docx
```

### Медленная рассылка для большого списка

```bash
python scripts/send_emails_safely.py \
    --smtp-server smtp.gmail.com \
    --smtp-port 587 \
    --sender sender@gmail.com \
    --password $GMAIL_APP_PASSWORD \
    --recipients-file large_list.txt \
    --subject "Рассылка" \
    --body "Текст" \
    --delay 10.0 \
    --batch-size 5 \
    --batch-delay 180.0
```

