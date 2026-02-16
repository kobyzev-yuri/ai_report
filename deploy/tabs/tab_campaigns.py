"""
Закладка: Рекламные email кампании
"""
import streamlit as st
from pathlib import Path
from datetime import datetime
import io
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import cx_Oracle
from typing import List, Optional, Tuple


def _get_db_connection():
    """Получить подключение к Oracle"""
    from db_connection import get_db_connection
    return get_db_connection()


def _parse_email_list(email_text: str) -> List[str]:
    """
    Парсинг списка email из текста (разделитель - запятая, точка с запятой или новая строка)
    Возвращает список валидных email адресов
    """
    # Заменяем переносы строк и точку с запятой на запятые
    email_text = email_text.replace('\n', ',').replace(';', ',')
    # Разбиваем по запятым
    emails = [e.strip() for e in email_text.split(',')]
    # Фильтруем пустые и валидируем email
    valid_emails = []
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    for email in emails:
        email = email.strip()
        if email and email_pattern.match(email):
            valid_emails.append(email.lower())
    return valid_emails


def _docx_to_html(docx_content: bytes) -> str:
    """
    Конвертация DOCX в HTML для отправки по email
    Использует python-docx для чтения документа
    """
    try:
        from docx import Document
        from docx.oxml import OxmlElement
        from docx.oxml.ns import qn
        
        doc = Document(io.BytesIO(docx_content))
        html_parts = []
        
        for paragraph in doc.paragraphs:
            if paragraph.text.strip():
                # Простое форматирование: жирный текст
                text = paragraph.text
                # Заменяем переносы строк на <br>
                text = text.replace('\n', '<br>')
                html_parts.append(f"<p>{text}</p>")
        
        # Обработка таблиц если есть
        for table in doc.tables:
            html_parts.append("<table border='1' style='border-collapse: collapse; margin: 10px 0;'>")
            for row in table.rows:
                html_parts.append("<tr>")
                for cell in row.cells:
                    html_parts.append(f"<td style='padding: 5px;'>{cell.text}</td>")
                html_parts.append("</tr>")
            html_parts.append("</table>")
        
        return '\n'.join(html_parts)
    except ImportError:
        st.error("⚠️ Библиотека python-docx не установлена. Установите: pip install python-docx")
        return "<p>Ошибка: библиотека python-docx не установлена</p>"
    except Exception as e:
        st.warning(f"⚠️ Ошибка при конвертации DOCX: {e}. Будет отправлен пустой текст.")
        return "<p>Ошибка при обработке документа</p>"


def _docx_to_text(docx_content: bytes) -> str:
    """Конвертация DOCX в простой текст (fallback)"""
    try:
        from docx import Document
        doc = Document(io.BytesIO(docx_content))
        text_parts = []
        for paragraph in doc.paragraphs:
            if paragraph.text.strip():
                text_parts.append(paragraph.text)
        return '\n\n'.join(text_parts)
    except Exception as e:
        return f"Ошибка при обработке документа: {e}"


def _save_campaign_to_db(
    conn,
    campaign_name: str,
    subject: str,
    greeting: str,
    email_list: List[str],
    docx_content: Optional[bytes],
    docx_filename: Optional[str],
    created_by: str,
    test_mode: bool = False,
    test_emails: Optional[List[str]] = None
) -> Optional[int]:
    """
    Сохранить кампанию в базу данных Oracle
    Возвращает CAMPAIGN_ID или None при ошибке
    """
    try:
        cursor = conn.cursor()
        email_list_str = ','.join(email_list)
        test_emails_str = ','.join(test_emails) if test_emails else None
        
        # Создаем переменную для получения ID
        campaign_id_var = cursor.var(cx_Oracle.NUMBER)
        
        if docx_content:
            cursor.execute("""
                INSERT INTO EMAIL_CAMPAIGNS (
                    CAMPAIGN_NAME, SUBJECT, GREETING, EMAIL_LIST,
                    DOCX_CONTENT, DOCX_FILENAME, EMAILS_TOTAL,
                    CREATED_BY, STATUS, TEST_MODE, TEST_EMAILS
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, :8, 'DRAFT', :9, :10
                )
                RETURNING CAMPAIGN_ID INTO :11
            """, (
                campaign_name, subject, greeting, email_list_str,
                cx_Oracle.Binary(docx_content), docx_filename,
                len(email_list), created_by, 1 if test_mode else 0, test_emails_str,
                campaign_id_var
            ))
        else:
            cursor.execute("""
                INSERT INTO EMAIL_CAMPAIGNS (
                    CAMPAIGN_NAME, SUBJECT, GREETING, EMAIL_LIST,
                    EMAILS_TOTAL, CREATED_BY, STATUS, TEST_MODE, TEST_EMAILS
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, 'DRAFT', :7, :8
                )
                RETURNING CAMPAIGN_ID INTO :9
            """, (
                campaign_name, subject, greeting, email_list_str,
                len(email_list), created_by, 1 if test_mode else 0, test_emails_str,
                campaign_id_var
            ))
        
        campaign_id = campaign_id_var.getvalue()[0]
        conn.commit()
        cursor.close()
        return campaign_id
    except Exception as e:
        st.error(f"❌ Ошибка при сохранении кампании в БД: {e}")
        conn.rollback()
        return None


def _send_email_campaign(
    conn,
    campaign_id: int,
    smtp_host: str = 'mail.steccom.ru',
    smtp_port: int = 25,
    from_email: str = 'sales@steccom.ru',
    test_mode: bool = False,
    test_emails: Optional[List[str]] = None,
    sent_by: Optional[str] = None
) -> Tuple[int, int, str]:
    """
    Отправить кампанию по email
    Возвращает (отправлено, ошибок, сообщение)
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT CAMPAIGN_NAME, SUBJECT, GREETING, EMAIL_LIST,
                   DOCX_CONTENT, DOCX_FILENAME, EMAILS_TOTAL
            FROM EMAIL_CAMPAIGNS
            WHERE CAMPAIGN_ID = :1
        """, (campaign_id,))
        
        row = cursor.fetchone()
        if not row:
            return 0, 0, "Кампания не найдена"
        
        campaign_name, subject, greeting, email_list_str, docx_content, docx_filename, emails_total = row
        
        # Парсим список email
        email_list = _parse_email_list(email_list_str)
        if not email_list:
            return 0, 0, "Список email пуст или невалиден"
        
        # Конвертируем DOCX в HTML
        html_body = ""
        if docx_content:
            # Читаем BLOB из Oracle
            if hasattr(docx_content, 'read'):
                docx_bytes = docx_content.read()
            else:
                docx_bytes = docx_content
            html_body = _docx_to_html(docx_bytes)
        
        # Формируем полное тело письма с приветствием
        full_html = f"""
        <html>
        <head>
            <meta charset="UTF-8">
        </head>
        <body>
            <p>{greeting or 'Здравствуйте!'}</p>
            {html_body}
        </body>
        </html>
        """
        
        # Отправляем письма
        sent_count = 0
        failed_count = 0
        error_messages = []
        
        try:
            # Подключение к SMTP серверу
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()  # Используем TLS если поддерживается
            
            for email in email_list:
                try:
                    msg = MIMEMultipart('alternative')
                    msg['From'] = from_email
                    msg['To'] = email
                    msg['Subject'] = subject
                    
                    # Добавляем HTML версию
                    html_part = MIMEText(full_html, 'html', 'utf-8')
                    msg.attach(html_part)
                    
                    # Отправляем
                    server.sendmail(from_email, [email], msg.as_string())
                    sent_count += 1
                except Exception as e:
                    failed_count += 1
                    error_messages.append(f"{email}: {str(e)}")
            
            server.quit()
        except Exception as e:
            return 0, len(email_list), f"Ошибка SMTP подключения: {e}"
        
        # Обновляем статус кампании в БД
        if use_test_mode:
            status = 'TEST_SENT' if failed_count == 0 else ('PARTIAL' if sent_count > 0 else 'FAILED')
        else:
            status = 'SENT' if failed_count == 0 else ('PARTIAL' if sent_count > 0 else 'FAILED')
        error_msg = '; '.join(error_messages[:10]) if error_messages else None  # Первые 10 ошибок
        
        # Получаем текущего пользователя из параметра или из session_state
        if not sent_by:
            sent_by = st.session_state.get('username', 'unknown') if 'username' in st.session_state else 'system'
        
        cursor.execute("""
            UPDATE EMAIL_CAMPAIGNS
            SET STATUS = :1, EMAILS_SENT = :2, EMAILS_FAILED = :3,
                SENT_BY = :4, SENT_AT = SYSDATE, ERROR_MESSAGE = :5
            WHERE CAMPAIGN_ID = :6
        """, (status, sent_count, failed_count, sent_by, error_msg, campaign_id))
        
        conn.commit()
        cursor.close()
        
        result_msg = f"Отправлено: {sent_count}, Ошибок: {failed_count}"
        if error_messages:
            result_msg += f"\nПервые ошибки: {error_messages[:3]}"
        
        return sent_count, failed_count, result_msg
        
    except Exception as e:
        conn.rollback()
        return 0, 0, f"Ошибка при отправке кампании: {e}"


def _get_campaigns_list(conn, limit: int = 50) -> List[dict]:
    """Получить список кампаний из БД"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT CAMPAIGN_ID, CAMPAIGN_NAME, SUBJECT, STATUS,
                   EMAILS_TOTAL, EMAILS_SENT, EMAILS_FAILED,
                   CREATED_BY, CREATED_AT, SENT_AT
            FROM EMAIL_CAMPAIGNS
            ORDER BY CREATED_AT DESC
            FETCH FIRST :1 ROWS ONLY
        """, (limit,))
        
        campaigns = []
        for row in cursor.fetchall():
            campaigns.append({
                'campaign_id': row[0],
                'campaign_name': row[1],
                'subject': row[2],
                'status': row[3],
                'emails_total': row[4] or 0,
                'emails_sent': row[5] or 0,
                'emails_failed': row[6] or 0,
                'created_by': row[7],
                'created_at': row[8],
                'sent_at': row[9]
            })
        
        cursor.close()
        return campaigns
    except Exception as e:
        st.error(f"❌ Ошибка при получении списка кампаний: {e}")
        return []


def _get_campaign_details(conn, campaign_id: int) -> Optional[dict]:
    """Получить детали кампании для повторного использования"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT CAMPAIGN_NAME, SUBJECT, GREETING, EMAIL_LIST,
                   DOCX_FILENAME, CREATED_BY, CREATED_AT, STATUS
            FROM EMAIL_CAMPAIGNS
            WHERE CAMPAIGN_ID = :1
        """, (campaign_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        return {
            'campaign_name': row[0],
            'subject': row[1],
            'greeting': row[2] or '',
            'email_list': row[3] or '',
            'docx_filename': row[4],
            'created_by': row[5],
            'created_at': row[6],
            'status': row[7]
        }
    except Exception as e:
        st.error(f"❌ Ошибка при получении деталей кампании: {e}")
        return None


def show_tab():
    """
    Закладка для создания и отправки рекламных email кампаний
    """
    st.header("📧 Рекламные email кампании")
    st.markdown(
        """
        Эта вкладка позволяет создавать и отправлять рекламные email кампании клиентам.
        
        **Как использовать:**
        1. Загрузите текстовый файл со списком email (разделитель - запятая)
        2. Загрузите файл письма в формате DOCX
        3. Заполните приветствие и тему письма
        4. Сохраните кампанию и отправьте письма
        5. Используйте сохраненные кампании для повторной отправки
        """
    )
    
    # Проверка подключения к БД
    conn = _get_db_connection()
    if not conn:
        st.error("❌ Не удалось подключиться к базе данных Oracle")
        st.info("Проверьте настройки подключения в config.env или переменные окружения")
        return
    
    # Проверка существования таблицы
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM user_tables WHERE table_name = 'EMAIL_CAMPAIGNS'
        """)
        table_exists = cursor.fetchone()[0] > 0
        cursor.close()
        
        if not table_exists:
            st.warning("⚠️ Таблица EMAIL_CAMPAIGNS не найдена в базе данных.")
            st.info("Выполните SQL скрипт: `oracle/tables/05_email_campaigns.sql`")
            conn.close()
            return
    except Exception as e:
        st.error(f"❌ Ошибка при проверке таблицы: {e}")
        conn.close()
        return
    
    # Получаем текущего пользователя
    username = st.session_state.get('username', 'unknown')
    
    # Вкладки для разных функций
    tab_new, tab_list, tab_reuse = st.tabs(["➕ Новая кампания", "📋 Список кампаний", "♻️ Повторное использование"])
    
    with tab_new:
        st.subheader("Создание новой кампании")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            campaign_name = st.text_input(
                "Название кампании",
                value=f"Кампания {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                help="Уникальное название для идентификации кампании"
            )
            
            subject = st.text_input(
                "Тема письма",
                placeholder="Например: Специальное предложение от STECCOM",
                help="Тема email письма"
            )
            
            greeting = st.text_area(
                "Приветствие",
                value="Здравствуйте!",
                placeholder="Текст приветствия, который будет добавлен перед содержимым письма",
                help="Этот текст будет добавлен в начало письма перед содержимым DOCX файла"
            )
        
        with col2:
            from_email = st.text_input(
                "Обратный адрес",
                value="sales@steccom.ru",
                help="Email адрес отправителя"
            )
            
            smtp_host = st.text_input(
                "SMTP сервер",
                value="mail.steccom.ru",
                help="Адрес SMTP сервера"
            )
            
            smtp_port = st.number_input(
                "SMTP порт",
                value=25,
                min_value=1,
                max_value=65535,
                help="Порт SMTP сервера"
            )
        
        st.markdown("---")
        
        # Загрузка списка email
        st.subheader("📧 Список получателей")
        
        # Проверяем наличие готовых файлов в data/
        project_root = Path(__file__).parent.parent
        default_email_file_path = project_root / "data" / "почты для рассылки MVSAT.txt"
        default_docx_file_path = project_root / "data" / "письмо_MVSAT.docx"
        
        use_default_files = False
        if default_email_file_path.exists() or default_docx_file_path.exists():
            col_check1, col_check2 = st.columns(2)
            with col_check1:
                if default_email_file_path.exists():
                    st.info(f"📄 Найден файл: `{default_email_file_path.name}`")
            with col_check2:
                if default_docx_file_path.exists():
                    st.info(f"📄 Найден файл: `{default_docx_file_path.name}`")
            
            use_default_files = st.checkbox(
                "Использовать готовые файлы из data/ (MVSAT)",
                value=False,
                help="Использовать файлы 'почты для рассылки MVSAT.txt' и 'письмо_MVSAT.docx' из директории data/"
            )
        
        email_file = None
        docx_file_default = None
        
        if use_default_files:
            if default_email_file_path.exists():
                with open(default_email_file_path, 'rb') as f:
                    email_content = f.read()
                    class FileLike:
                        def __init__(self, content, name):
                            self._content = content
                            self.name = name
                        def read(self):
                            return self._content
                    email_file = FileLike(email_content, default_email_file_path.name)
                    st.success(f"✅ Используется файл: {default_email_file_path.name}")
            if default_docx_file_path.exists():
                with open(default_docx_file_path, 'rb') as f:
                    docx_content = f.read()
                    class DocxFileLike:
                        def __init__(self, content, name):
                            self._content = content
                            self.name = name
                        def read(self):
                            return self._content
                        def getvalue(self):
                            return self._content
                    docx_file_default = DocxFileLike(docx_content, default_docx_file_path.name)
                    st.success(f"✅ Используется файл: {default_docx_file_path.name}")
        
        uploaded_email_file = None
        if not use_default_files:
            uploaded_email_file = st.file_uploader(
                "Загрузите текстовый файл со списком email",
                type=['txt', 'csv'],
                help="Файл должен содержать email адреса, разделенные запятыми, точкой с запятой или переносами строк"
            )
            if uploaded_email_file:
                email_file = uploaded_email_file
        
        email_text_input = st.text_area(
            "Или введите email адреса вручную",
            placeholder="email1@example.com, email2@example.com, email3@example.com",
            help="Введите email адреса через запятую, точку с запятой или с новой строки",
            disabled=use_default_files
        )
        
        email_list = []
        if email_file:
            try:
                if hasattr(email_file, 'read'):
                    email_text_bytes = email_file.read()
                    if isinstance(email_text_bytes, bytes):
                        email_text = email_text_bytes.decode('utf-8')
                    else:
                        email_text = email_text_bytes
                else:
                    email_text = str(email_file)
                email_list = _parse_email_list(email_text)
                st.info(f"✅ Загружено {len(email_list)} валидных email адресов из файла")
            except Exception as e:
                st.error(f"❌ Ошибка при чтении файла email: {e}")
        elif email_text_input and not use_default_files:
            email_list = _parse_email_list(email_text_input)
            if email_list:
                st.info(f"✅ Найдено {len(email_list)} валидных email адресов")
        
        if email_list:
            with st.expander(f"Просмотр списка получателей ({len(email_list)} адресов)"):
                st.write(', '.join(email_list[:50]))
                if len(email_list) > 50:
                    st.write(f"... и еще {len(email_list) - 50} адресов")
        
        st.markdown("---")
        
        # Загрузка DOCX файла
        st.subheader("📄 Файл письма (DOCX)")
        
        uploaded_docx_file = None
        if not use_default_files:
            uploaded_docx_file = st.file_uploader(
                "Загрузите файл письма в формате DOCX",
                type=['docx'],
                help="Файл письма будет конвертирован в HTML и отправлен получателям"
            )
        
        docx_file = docx_file_default if use_default_files else uploaded_docx_file
        
        if docx_file:
            try:
                if hasattr(docx_file, 'getvalue'):
                    file_size = len(docx_file.getvalue())
                elif hasattr(docx_file, 'read'):
                    content = docx_file.read()
                    file_size = len(content) if isinstance(content, bytes) else 0
                else:
                    file_size = 0
                file_name = getattr(docx_file, 'name', 'unknown')
                st.success(f"✅ Файл загружен: {file_name} ({file_size} байт)")
            except Exception as e:
                st.warning(f"⚠️ Не удалось определить размер файла: {e}")
        
        st.markdown("---")
        
        # Контрольные email для тестовой рассылки
        st.subheader("🧪 Контрольные email для тестовой рассылки")
        st.markdown(
            """
            **Важно:** Укажите контрольные email адреса для проверки рассылки перед отправкой основному списку.
            Эти адреса будут использоваться при тестовой рассылке.
            """
        )
        
        test_emails_input = st.text_area(
            "Контрольные email адреса",
            placeholder="test1@example.com, test2@example.com",
            help="Введите контрольные email адреса через запятую. Они будут использоваться для тестовой рассылки.",
            key="test_emails_input"
        )
        
        test_emails_list = []
        if test_emails_input:
            test_emails_list = _parse_email_list(test_emails_input)
            if test_emails_list:
                st.info(f"✅ Найдено {len(test_emails_list)} валидных контрольных email адресов")
                st.write(', '.join(test_emails_list))
            else:
                st.warning("⚠️ Не найдено валидных email адресов в поле контрольных email")
        
        st.markdown("---")
        
        # Режим рассылки
        st.subheader("📤 Режим рассылки")
        test_mode = st.checkbox(
            "🧪 Тестовая рассылка",
            value=False,
            help="Если включено, рассылка будет отправлена только на контрольные email адреса, указанные выше. "
                 "Основной список получателей будет проигнорирован.",
            key="test_mode_checkbox"
        )
        
        if test_mode:
            if not test_emails_list:
                st.error("❌ Для тестовой рассылки необходимо указать хотя бы один контрольный email адрес!")
            else:
                st.warning(f"⚠️ **РЕЖИМ ТЕСТОВОЙ РАССЫЛКИ АКТИВЕН**")
                st.info(f"📧 Письма будут отправлены только на {len(test_emails_list)} контрольных адресов:")
                st.write(', '.join(test_emails_list))
                
                # Показываем основной список для информации
                if email_list:
                    st.markdown("---")
                    st.subheader("📋 Основной список получателей (для боевой рассылки)")
                    st.info(f"📬 При обычной рассылке письма будут отправлены на {len(email_list)} адресов из основного списка:")
                    with st.expander(f"Просмотр основного списка ({len(email_list)} адресов)", expanded=False):
                        st.write(', '.join(email_list[:100]))
                        if len(email_list) > 100:
                            st.write(f"... и еще {len(email_list) - 100} адресов")
                    st.caption("💡 Этот список будет использован при обычной (боевой) рассылке после проверки тестовой")
                else:
                    st.warning("⚠️ Основной список получателей пуст. При обычной рассылке не будет получателей!")
        else:
            if email_list:
                st.info(f"📧 Обычная рассылка: письма будут отправлены на {len(email_list)} адресов из основного списка")
                with st.expander(f"Просмотр списка получателей ({len(email_list)} адресов)", expanded=False):
                    st.write(', '.join(email_list[:100]))
                    if len(email_list) > 100:
                        st.write(f"... и еще {len(email_list) - 100} адресов")
            else:
                st.warning("⚠️ Основной список получателей пуст")
        
        st.markdown("---")
        
        # Кнопки действий
        col_save, col_send = st.columns(2)
        
        with col_save:
            if st.button("💾 Сохранить кампанию", type="primary", use_container_width=True):
                if not campaign_name:
                    st.error("Введите название кампании")
                elif not subject:
                    st.error("Введите тему письма")
                elif not email_list:
                    st.error("Загрузите список email получателей")
                else:
                    with st.spinner("Сохранение кампании..."):
                        docx_content = None
                        docx_filename = None
                        if docx_file:
                            try:
                                if hasattr(docx_file, 'read'):
                                    docx_content = docx_file.read()
                                    if not isinstance(docx_content, bytes):
                                        docx_content = docx_file.getvalue() if hasattr(docx_file, 'getvalue') else None
                                elif hasattr(docx_file, 'getvalue'):
                                    docx_content = docx_file.getvalue()
                                docx_filename = getattr(docx_file, 'name', 'letter.docx')
                            except Exception as e:
                                st.warning(f"⚠️ Ошибка при чтении DOCX файла: {e}")
                        
                        campaign_id = _save_campaign_to_db(
                            conn,
                            campaign_name,
                            subject,
                            greeting,
                            email_list,
                            docx_content,
                            docx_filename,
                            username,
                            test_mode,
                            test_emails_list if test_emails_list else None
                        )
                        if campaign_id:
                            mode_text = "тестовой" if test_mode else "обычной"
                            st.success(f"✅ Кампания сохранена! ID: {campaign_id} ({mode_text} рассылки)")
                            st.info("Теперь вы можете отправить кампанию или использовать её позже")
                        else:
                            st.error("Не удалось сохранить кампанию")
        
        with col_send:
            if st.button("📤 Сохранить и отправить", type="primary", use_container_width=True):
                if not campaign_name:
                    st.error("Введите название кампании")
                elif not subject:
                    st.error("Введите тему письма")
                elif test_mode and not test_emails_list:
                    st.error("Для тестовой рассылки необходимо указать контрольные email адреса")
                elif not test_mode and not email_list:
                    st.error("Загрузите список email получателей")
                else:
                    with st.spinner("Сохранение и отправка кампании..."):
                        # Сначала сохраняем
                        docx_content = None
                        docx_filename = None
                        if docx_file:
                            try:
                                if hasattr(docx_file, 'read'):
                                    docx_content = docx_file.read()
                                    if not isinstance(docx_content, bytes):
                                        docx_content = docx_file.getvalue() if hasattr(docx_file, 'getvalue') else None
                                elif hasattr(docx_file, 'getvalue'):
                                    docx_content = docx_file.getvalue()
                                docx_filename = getattr(docx_file, 'name', 'letter.docx')
                            except Exception as e:
                                st.warning(f"⚠️ Ошибка при чтении DOCX файла: {e}")
                        
                        campaign_id = _save_campaign_to_db(
                            conn,
                            campaign_name,
                            subject,
                            greeting,
                            email_list,
                            docx_content,
                            docx_filename,
                            username,
                            test_mode,
                            test_emails_list if test_emails_list else None
                        )
                        
                        if campaign_id:
                            st.success(f"✅ Кампания сохранена (ID: {campaign_id})")
                            
                            # Затем отправляем
                            with st.spinner(f"Отправка писем {len(email_list)} получателям..."):
                                sent, failed, msg = _send_email_campaign(
                                    conn,
                                    campaign_id,
                                    smtp_host,
                                    int(smtp_port),
                                    from_email,
                                    test_mode,
                                    test_emails_list if test_emails_list else None,
                                    username
                                )
                                
                                if sent > 0:
                                    st.success(f"✅ {msg}")
                                elif failed > 0:
                                    st.warning(f"⚠️ {msg}")
                                else:
                                    st.error(f"❌ {msg}")
                        else:
                            st.error("Не удалось сохранить кампанию")
    
    with tab_list:
        st.subheader("Список всех кампаний")
        
        campaigns = _get_campaigns_list(conn, limit=100)
        
        if not campaigns:
            st.info("Пока нет сохраненных кампаний")
        else:
            import pandas as pd
            
            df_data = []
            for camp in campaigns:
                status_display = camp['status']
                if camp['test_mode']:
                    status_display = f"🧪 {status_display}"
                df_data.append({
                    'ID': camp['campaign_id'],
                    'Название': camp['campaign_name'],
                    'Тема': camp['subject'],
                    'Статус': status_display,
                    'Режим': 'Тест' if camp['test_mode'] else 'Обычный',
                    'Всего': camp['emails_total'],
                    'Отправлено': camp['emails_sent'],
                    'Ошибок': camp['emails_failed'],
                    'Создал': camp['created_by'],
                    'Создано': camp['created_at'].strftime('%Y-%m-%d %H:%M') if camp['created_at'] else '-',
                    'Отправил': camp['sent_by'] or '-',
                    'Отправлено': camp['sent_at'].strftime('%Y-%m-%d %H:%M') if camp['sent_at'] else '-'
                })
            
            df = pd.DataFrame(df_data)
            st.dataframe(df, use_container_width=True, hide_index=True)
    
    with tab_reuse:
        st.subheader("Повторное использование сохраненной кампании")
        
        campaigns = _get_campaigns_list(conn, limit=50)
        
        if not campaigns:
            st.info("Нет сохраненных кампаний для повторного использования")
        else:
            campaign_options = {f"{c['campaign_id']}: {c['campaign_name']}": c['campaign_id'] 
                               for c in campaigns}
            
            selected_campaign_key = st.selectbox(
                "Выберите кампанию",
                options=list(campaign_options.keys()),
                help="Выберите сохраненную кампанию для повторной отправки"
            )
            
            if selected_campaign_key:
                campaign_id = campaign_options[selected_campaign_key]
                details = _get_campaign_details(conn, campaign_id)
                
                if details:
                    st.markdown("---")
                    st.subheader("Детали кампании")
                    
                    col_d1, col_d2 = st.columns(2)
                    with col_d1:
                        st.write(f"**Название:** {details['campaign_name']}")
                        st.write(f"**Тема:** {details['subject']}")
                        st.write(f"**Приветствие:** {details['greeting']}")
                    with col_d2:
                        st.write(f"**Статус:** {details['status']}")
                        st.write(f"**Создал:** {details['created_by']}")
                        st.write(f"**Создано:** {details['created_at'].strftime('%Y-%m-%d %H:%M') if details['created_at'] else '-'}")
                    
                    st.markdown("---")
                    st.subheader("Список получателей")
                    email_list_reuse = _parse_email_list(details['email_list'])
                    test_emails_reuse = _parse_email_list(details['test_emails']) if details.get('test_emails') else []
                    
                    if details.get('test_mode'):
                        st.warning("🧪 **Это была тестовая кампания**")
                        if test_emails_reuse:
                            st.write(f"Контрольные email: {len(test_emails_reuse)}")
                            with st.expander("Просмотр контрольных email"):
                                st.write(', '.join(test_emails_reuse))
                    else:
                        st.write(f"Основной список: {len(email_list_reuse)} email")
                        with st.expander("Просмотр списка"):
                            st.write(', '.join(email_list_reuse[:50]))
                            if len(email_list_reuse) > 50:
                                st.write(f"... и еще {len(email_list_reuse) - 50} адресов")
                    
                    st.markdown("---")
                    st.subheader("Повторная отправка")
                    
                    test_mode_reuse = st.checkbox(
                        "🧪 Тестовая рассылка",
                        value=details.get('test_mode', False),
                        help="Если включено, рассылка будет отправлена только на контрольные email адреса",
                        key="reuse_test_mode"
                    )
                    
                    test_emails_reuse_input = st.text_area(
                        "Контрольные email (для тестовой рассылки)",
                        value=details.get('test_emails', '') or '',
                        placeholder="test1@example.com, test2@example.com",
                        key="reuse_test_emails"
                    )
                    
                    test_emails_reuse_parsed = []
                    if test_emails_reuse_input:
                        test_emails_reuse_parsed = _parse_email_list(test_emails_reuse_input)
                    
                    col_r1, col_r2 = st.columns(2)
                    with col_r1:
                        smtp_host_reuse = st.text_input("SMTP сервер", value="mail.steccom.ru", key="reuse_smtp_host")
                        smtp_port_reuse = st.number_input("SMTP порт", value=25, min_value=1, max_value=65535, key="reuse_smtp_port")
                    with col_r2:
                        from_email_reuse = st.text_input("Обратный адрес", value="sales@steccom.ru", key="reuse_from_email")
                    
                    if st.button("📤 Отправить кампанию повторно", type="primary", use_container_width=True):
                        if test_mode_reuse and not test_emails_reuse_parsed:
                            st.error("Для тестовой рассылки необходимо указать контрольные email адреса")
                        else:
                            recipients_count = len(test_emails_reuse_parsed) if test_mode_reuse else len(email_list_reuse)
                            mode_text = "тестовой" if test_mode_reuse else "обычной"
                            with st.spinner(f"Отправка писем ({mode_text} режим) {recipients_count} получателям..."):
                                sent, failed, msg = _send_email_campaign(
                                    conn,
                                    campaign_id,
                                    smtp_host_reuse,
                                    int(smtp_port_reuse),
                                    from_email_reuse,
                                    test_mode_reuse,
                                    test_emails_reuse_parsed if test_emails_reuse_parsed else None,
                                    username
                                )
                            
                            if sent > 0:
                                st.success(f"✅ {msg}")
                            elif failed > 0:
                                st.warning(f"⚠️ {msg}")
                            else:
                                st.error(f"❌ {msg}")
    
    conn.close()

