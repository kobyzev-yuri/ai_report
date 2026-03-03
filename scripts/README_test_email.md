# Тестирование создания писем с вложениями

## Использование скрипта test_email_with_attachment.py

Скрипт создает тестовое письмо с PDF вложением и сохраняет его в файл `.eml` для проверки.

### Базовое использование:

```bash
python3 scripts/test_email_with_attachment.py \
  --subject "Тестовое письмо" \
  --pdf "data/MVSAT_СТЭККОМ_26.pdf" \
  --output "test_email.eml"
```

### С кастомным текстом:

```bash
python3 scripts/test_email_with_attachment.py \
  --subject "Тестовое письмо" \
  --greeting "Ваш текст письма здесь..." \
  --pdf "data/MVSAT_СТЭККОМ_26.pdf" \
  --output "test_email.eml"
```

### Проверка созданного письма:

1. **Открыть в почтовом клиенте:**
   - Thunderbird: Файл → Открыть → Сохраненное сообщение
   - Outlook: Файл → Открыть → Outlook Data File (или просто перетащить .eml файл)

2. **Просмотреть структуру:**
   ```bash
   python3 -c "
   import email
   from email import policy
   with open('test_email.eml', 'rb') as f:
       msg = email.message_from_bytes(f.read(), policy=policy.default)
       for i, part in enumerate(msg.walk()):
           print(f'{i+1}. {part.get_content_type()} - {part.get_content_disposition()}')
           if part.get_filename():
               print(f'   Файл: {part.get_filename()}')
   "
   ```

### Что проверяет скрипт:

- ✅ Удаление дублирования текста (автоматически)
- ✅ Правильное кодирование имени файла с кириллицей (RFC 2231)
- ✅ Корректная структура MIME (multipart/mixed с HTML и PDF)
- ✅ Правильная сигнатура PDF файла

### Примеры проблем, которые можно обнаружить:

1. **Дублирование текста** - скрипт автоматически удаляет дубликаты
2. **Неправильное имя файла** - проверяется кодирование кириллицы
3. **Неправильный тип вложения** - проверяется Content-Type и сигнатура PDF
4. **Неправильная структура MIME** - проверяется multipart/mixed

