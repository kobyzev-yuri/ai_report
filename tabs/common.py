"""
Общие функции для всех закладок
"""
import streamlit as st
import pandas as pd
import io
import time
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Dict, Optional, Tuple
from pathlib import Path


def export_to_csv(df):
    """Экспорт в CSV"""
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    return output.getvalue()


def export_to_excel(df):
    """Экспорт в Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
        worksheet = writer.sheets['Report']
        from openpyxl.utils import get_column_letter
        for idx, col in enumerate(df.columns, start=1):
            max_length = max(
                df[col].astype(str).map(len).max() if len(df) > 0 else 0,
                len(str(col))
            )
            col_letter = get_column_letter(idx)
            worksheet.column_dimensions[col_letter].width = min(max_length + 2, 50)
    return output.getvalue()


def send_email_safely(
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
    recipient_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    attachments: Optional[List[str]] = None,
    delay_seconds: float = 2.0,
    max_retries: int = 3,
    retry_delay: float = 5.0
) -> Tuple[bool, Optional[str]]:
    """
    Отправка одного email с обработкой ошибок
    
    Args:
        smtp_server: SMTP сервер (например, 'smtp.gmail.com')
        smtp_port: Порт SMTP (обычно 587 для TLS)
        sender_email: Email отправителя
        sender_password: Пароль отправителя
        recipient_email: Email получателя
        subject: Тема письма
        body_text: Текст письма (plain text)
        body_html: HTML версия письма (опционально)
        attachments: Список путей к файлам для вложения (опционально)
        delay_seconds: Задержка после отправки (по умолчанию 2 секунды)
        max_retries: Максимальное количество попыток при ошибке
        retry_delay: Задержка между попытками при ошибке
        
    Returns:
        Tuple[bool, Optional[str]]: (успех, сообщение об ошибке)
    """
    for attempt in range(max_retries):
        try:
            # Создаем сообщение
            msg = MIMEMultipart('alternative')
            msg['From'] = sender_email
            msg['To'] = recipient_email
            msg['Subject'] = subject
            
            # Добавляем текстовую часть
            text_part = MIMEText(body_text, 'plain', 'utf-8')
            msg.attach(text_part)
            
            # Добавляем HTML часть, если есть
            if body_html:
                html_part = MIMEText(body_html, 'html', 'utf-8')
                msg.attach(html_part)
            
            # Добавляем вложения, если есть
            if attachments:
                for file_path in attachments:
                    if Path(file_path).exists():
                        with open(file_path, 'rb') as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {Path(file_path).name}'
                            )
                            msg.attach(part)
            
            # Подключаемся к SMTP серверу и отправляем
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  # Включаем TLS шифрование
                server.login(sender_email, sender_password)
                server.send_message(msg)
            
            # Успешная отправка
            logging.info(f"✓ Email успешно отправлен: {recipient_email}")
            time.sleep(delay_seconds)  # Задержка перед следующим письмом
            return True, None
            
        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"Ошибка аутентификации: {e}"
            logging.error(error_msg)
            return False, error_msg  # Не повторяем при ошибке аутентификации
            
        except smtplib.SMTPRecipientsRefused as e:
            error_msg = f"Получатель отклонен: {recipient_email} - {e}"
            logging.warning(error_msg)
            return False, error_msg  # Не повторяем при неверном адресе
            
        except smtplib.SMTPServerDisconnected as e:
            error_msg = f"Сервер отключился: {e}"
            logging.warning(f"Попытка {attempt + 1}/{max_retries}: {error_msg}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return False, error_msg
            
        except smtplib.SMTPException as e:
            error_msg = f"SMTP ошибка: {e}"
            logging.warning(f"Попытка {attempt + 1}/{max_retries}: {error_msg}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return False, error_msg
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {e}"
            logging.error(f"Попытка {attempt + 1}/{max_retries}: {error_msg}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return False, error_msg
    
    return False, "Превышено максимальное количество попыток"


def send_bulk_emails_safely(
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
    recipients: List[str],
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    attachments: Optional[List[str]] = None,
    delay_between_emails: float = 2.0,
    delay_after_batch: float = 60.0,
    batch_size: int = 10,
    progress_callback: Optional[callable] = None,
    start_from_index: int = 0
) -> Dict[str, any]:
    """
    Безопасная массовая рассылка email с задержками
    
    Отправляет письма по одному с задержками, чтобы избежать блокировки как спам.
    После каждой партии (batch_size писем) делает дополнительную паузу.
    
    Args:
        smtp_server: SMTP сервер
        smtp_port: Порт SMTP
        sender_email: Email отправителя
        sender_password: Пароль отправителя
        recipients: Список email адресов получателей
        subject: Тема письма (может содержать {email} для подстановки)
        body_text: Текст письма (может содержать {email} для подстановки)
        body_html: HTML версия письма (опционально)
        attachments: Список путей к файлам для вложения (опционально)
        delay_between_emails: Задержка между письмами в секундах (рекомендуется 2-5 сек)
        delay_after_batch: Задержка после каждой партии в секундах (рекомендуется 60+ сек)
        batch_size: Размер партии перед длительной паузой (рекомендуется 10-20)
        progress_callback: Функция для отслеживания прогресса (email, success, error)
        start_from_index: Индекс, с которого начать (для возобновления после ошибки)
        
    Returns:
        Dict с результатами:
        {
            'total': общее количество,
            'success': успешно отправлено,
            'failed': не удалось отправить,
            'failed_emails': список email с ошибками,
            'errors': детальные ошибки
        }
    """
    results = {
        'total': len(recipients),
        'success': 0,
        'failed': 0,
        'failed_emails': [],
        'errors': {}
    }
    
    logging.info(f"Начало рассылки на {len(recipients)} адресов")
    logging.info(f"Задержка между письмами: {delay_between_emails} сек")
    logging.info(f"Задержка после партии ({batch_size} писем): {delay_after_batch} сек")
    
    for idx, recipient in enumerate(recipients[start_from_index:], start=start_from_index):
        # Персонализация темы и текста, если нужно
        personalized_subject = subject.format(email=recipient) if '{email}' in subject else subject
        personalized_body_text = body_text.format(email=recipient) if '{email}' in body_text else body_text
        personalized_body_html = None
        if body_html:
            personalized_body_html = body_html.format(email=recipient) if '{email}' in body_html else body_html
        
        # Отправка письма
        success, error = send_email_safely(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            sender_email=sender_email,
            sender_password=sender_password,
            recipient_email=recipient,
            subject=personalized_subject,
            body_text=personalized_body_text,
            body_html=personalized_body_html,
            attachments=attachments,
            delay_seconds=0  # Задержку делаем вручную для контроля
        )
        
        if success:
            results['success'] += 1
            if progress_callback:
                progress_callback(recipient, True, None)
        else:
            results['failed'] += 1
            results['failed_emails'].append(recipient)
            results['errors'][recipient] = error
            if progress_callback:
                progress_callback(recipient, False, error)
        
        # Задержка между письмами
        if idx < len(recipients) - 1:  # Не делаем задержку после последнего письма
            time.sleep(delay_between_emails)
        
        # Дополнительная задержка после каждой партии
        if (idx + 1) % batch_size == 0 and idx < len(recipients) - 1:
            logging.info(f"Отправлено {idx + 1}/{len(recipients)} писем. Пауза {delay_after_batch} сек...")
            time.sleep(delay_after_batch)
    
    logging.info(f"Рассылка завершена. Успешно: {results['success']}, Ошибок: {results['failed']}")
    return results


def load_emails_from_file(file_path: str) -> List[str]:
    """
    Загрузка списка email из файла
    
    Поддерживает форматы:
    - Один email на строку
    - Список через запятую в одной строке
    - Комбинация обоих форматов
    
    Args:
        file_path: Путь к файлу с email адресами
        
    Returns:
        Список валидных email адресов
    """
    emails = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            # Если строка содержит запятые, разделяем
            if ',' in line:
                line_emails = [e.strip() for e in line.split(',')]
                emails.extend(line_emails)
            else:
                emails.append(line)
    
    # Удаляем дубликаты и пустые строки
    emails = list(set([e for e in emails if e]))
    return emails


















