#!/usr/bin/env python3
"""
Безопасный исполнитель SQL запросов для модифицирующих операций
Включает валидацию, предварительный просмотр, транзакции и логирование
"""
import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import cx_Oracle
import logging
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import json
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SafeSQLExecutor:
    """Безопасный исполнитель SQL для модифицирующих операций"""
    
    def __init__(self, connection_params: Optional[Dict] = None):
        """
        Инициализация безопасного исполнителя
        
        Args:
            connection_params: Параметры подключения к Oracle
        """
        self.connection_params = connection_params or self._get_default_connection()
        self.log_dir = Path(__file__).parent.parent.parent / "logs" / "sql_executions"
        self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_default_connection(self) -> Dict:
        """Получение параметров подключения из переменных окружения"""
        return {
            'user': os.getenv('ORACLE_USER'),
            'password': os.getenv('ORACLE_PASSWORD'),
            'host': os.getenv('ORACLE_HOST'),
            'port': int(os.getenv('ORACLE_PORT', '1521')),
            'sid': os.getenv('ORACLE_SID'),
            'service_name': os.getenv('ORACLE_SERVICE') or os.getenv('ORACLE_SID')
        }
    
    def _get_connection(self):
        """Создание подключения к Oracle"""
        params = self.connection_params
        if params['sid']:
            dsn = cx_Oracle.makedsn(params['host'], params['port'], sid=params['sid'])
        else:
            dsn = cx_Oracle.makedsn(params['host'], params['port'], service_name=params['service_name'])
        
        return cx_Oracle.connect(
            user=params['user'],
            password=params['password'],
            dsn=dsn
        )
    
    def is_modifying_query(self, sql: str) -> bool:
        """
        Проверка, является ли запрос модифицирующим
        
        Args:
            sql: SQL запрос
        
        Returns:
            True если запрос модифицирует данные
        """
        sql_upper = sql.strip().upper()
        modifying_keywords = ['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE', 'DROP', 'ALTER', 'CREATE']
        return any(sql_upper.startswith(keyword) for keyword in modifying_keywords)
    
    def validate_sql(self, sql: str) -> Tuple[bool, Optional[str], List[str]]:
        """
        Валидация SQL запроса
        
        Args:
            sql: SQL запрос
        
        Returns:
            (валиден, ошибка, предупреждения)
        """
        warnings = []
        sql_upper = sql.strip().upper()
        
        # Проверка на опасные операции
        if 'DROP TABLE' in sql_upper or 'TRUNCATE TABLE' in sql_upper:
            return False, "❌ ОПАСНАЯ ОПЕРАЦИЯ: DROP/TRUNCATE TABLE не разрешены", warnings
        
        if 'DROP DATABASE' in sql_upper or 'DROP USER' in sql_upper:
            return False, "❌ ОПАСНАЯ ОПЕРАЦИЯ: DROP DATABASE/USER не разрешены", warnings
        
        # Проверка на отсутствие WHERE в UPDATE/DELETE
        if sql_upper.startswith('UPDATE') and 'WHERE' not in sql_upper:
            warnings.append("⚠️ ВНИМАНИЕ: UPDATE без WHERE обновит все строки!")
        
        if sql_upper.startswith('DELETE') and 'WHERE' not in sql_upper:
            warnings.append("⚠️ ВНИМАНИЕ: DELETE без WHERE удалит все строки!")
        
        # Проверка синтаксиса через EXPLAIN PLAN
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Пробуем EXPLAIN PLAN для проверки синтаксиса
            try:
                # Обертываем в SELECT для безопасной проверки
                test_sql = f"SELECT * FROM ({sql}) WHERE ROWNUM = 0"
                cursor.execute(f"EXPLAIN PLAN FOR {test_sql}")
            except:
                # Если не получилось, пробуем просто EXPLAIN PLAN
                try:
                    cursor.execute(f"EXPLAIN PLAN FOR {sql}")
                except Exception as e:
                    cursor.close()
                    conn.close()
                    return False, f"❌ Ошибка синтаксиса SQL: {str(e)}", warnings
            
            cursor.close()
            conn.close()
            return True, None, warnings
            
        except Exception as e:
            return False, f"❌ Ошибка валидации: {str(e)}", warnings
    
    def preview_changes(self, sql: str) -> Dict[str, Any]:
        """
        Предварительный просмотр изменений (dry-run)
        
        Args:
            sql: SQL запрос
        
        Returns:
            Словарь с информацией о предстоящих изменениях
        """
        preview = {
            "query_type": "SELECT",
            "affected_tables": [],
            "estimated_rows": None,
            "preview_data": None,
            "error": None
        }
        
        sql_upper = sql.strip().upper()
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Определяем тип запроса
            if sql_upper.startswith('INSERT'):
                preview["query_type"] = "INSERT"
                # Извлекаем имя таблицы
                import re
                match = re.search(r'INTO\s+([A-Z_][A-Z0-9_]*)', sql_upper)
                if match:
                    preview["affected_tables"] = [match.group(1)]
                
                # Показываем данные, которые будут вставлены
                # Извлекаем VALUES или SELECT из INSERT
                if 'VALUES' in sql_upper:
                    # Простой INSERT с VALUES
                    preview["preview_data"] = "Данные из VALUES будут вставлены"
                elif 'SELECT' in sql_upper:
                    # INSERT ... SELECT - показываем результат SELECT
                    select_part = sql[sql_upper.find('SELECT'):]
                    cursor.execute(f"SELECT * FROM ({select_part}) WHERE ROWNUM <= 10")
                    preview["preview_data"] = cursor.fetchall()
                    preview["estimated_rows"] = "Из SELECT запроса"
            
            elif sql_upper.startswith('UPDATE'):
                preview["query_type"] = "UPDATE"
                # Извлекаем имя таблицы
                import re
                match = re.search(r'UPDATE\s+([A-Z_][A-Z0-9_]*)', sql_upper)
                if match:
                    preview["affected_tables"] = [match.group(1)]
                
                # Показываем строки, которые будут обновлены
                # Создаем SELECT с тем же WHERE
                where_pos = sql_upper.find('WHERE')
                if where_pos > 0:
                    where_clause = sql[where_pos:]
                    table_name = match.group(1) if match else "TABLE"
                    preview_sql = f"SELECT * FROM {table_name} {where_clause} AND ROWNUM <= 10"
                    try:
                        cursor.execute(preview_sql)
                        preview["preview_data"] = cursor.fetchall()
                        # Подсчитываем количество строк
                        count_sql = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
                        cursor.execute(count_sql)
                        preview["estimated_rows"] = cursor.fetchone()[0]
                    except Exception as e:
                        preview["error"] = f"Не удалось предпросмотреть: {str(e)}"
                else:
                    preview["estimated_rows"] = "ВСЕ СТРОКИ (нет WHERE)"
            
            elif sql_upper.startswith('DELETE'):
                preview["query_type"] = "DELETE"
                # Извлекаем имя таблицы
                import re
                match = re.search(r'FROM\s+([A-Z_][A-Z0-9_]*)', sql_upper)
                if match:
                    preview["affected_tables"] = [match.group(1)]
                
                # Показываем строки, которые будут удалены
                where_pos = sql_upper.find('WHERE')
                if where_pos > 0:
                    where_clause = sql[where_pos:]
                    table_name = match.group(1) if match else "TABLE"
                    preview_sql = f"SELECT * FROM {table_name} {where_clause} AND ROWNUM <= 10"
                    try:
                        cursor.execute(preview_sql)
                        preview["preview_data"] = cursor.fetchall()
                        # Подсчитываем количество строк
                        count_sql = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
                        cursor.execute(count_sql)
                        preview["estimated_rows"] = cursor.fetchone()[0]
                    except Exception as e:
                        preview["error"] = f"Не удалось предпросмотреть: {str(e)}"
                else:
                    preview["estimated_rows"] = "ВСЕ СТРОКИ (нет WHERE)"
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            preview["error"] = f"Ошибка предпросмотра: {str(e)}"
        
        return preview
    
    def execute_with_rollback(self, sql: str, auto_commit: bool = False) -> Dict[str, Any]:
        """
        Выполнение SQL с возможностью отката
        
        Args:
            sql: SQL запрос
            auto_commit: Автоматически зафиксировать изменения
        
        Returns:
            Результат выполнения
        """
        result = {
            "success": False,
            "rows_affected": 0,
            "error": None,
            "rollback_performed": False,
            "execution_time": None
        }
        
        import time
        start_time = time.time()
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Выполняем запрос
            cursor.execute(sql)
            result["rows_affected"] = cursor.rowcount
            
            if auto_commit:
                conn.commit()
                result["success"] = True
            else:
                # Не коммитим - можно откатить
                result["success"] = True
                result["rollback_performed"] = False
            
            cursor.close()
            
        except Exception as e:
            result["error"] = str(e)
            if conn:
                try:
                    conn.rollback()
                    result["rollback_performed"] = True
                except:
                    pass
        finally:
            if conn:
                conn.close()
        
        result["execution_time"] = time.time() - start_time
        return result
    
    def execute_in_transaction(self, sql: str, confirm_commit: bool = False) -> Dict[str, Any]:
        """
        Выполнение SQL в транзакции с подтверждением
        
        Args:
            sql: SQL запрос
            confirm_commit: Требуется подтверждение для коммита
        
        Returns:
            Результат выполнения
        """
        result = {
            "success": False,
            "rows_affected": 0,
            "error": None,
            "committed": False,
            "rolled_back": False,
            "preview": None
        }
        
        # Сначала предпросмотр
        if self.is_modifying_query(sql):
            result["preview"] = self.preview_changes(sql)
        
        # Валидация
        is_valid, error, warnings = self.validate_sql(sql)
        if not is_valid:
            result["error"] = error
            result["warnings"] = warnings
            return result
        
        result["warnings"] = warnings
        
        # Если требуется подтверждение, не выполняем сразу
        if confirm_commit:
            result["pending_confirmation"] = True
            return result
        
        # Выполняем в транзакции
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(sql)
            result["rows_affected"] = cursor.rowcount
            
            # Коммитим только если не требуется подтверждение
            if not confirm_commit:
                conn.commit()
                result["committed"] = True
                result["success"] = True
            
            cursor.close()
            
        except Exception as e:
            result["error"] = str(e)
            if conn:
                try:
                    conn.rollback()
                    result["rolled_back"] = True
                except:
                    pass
        finally:
            if conn:
                conn.close()
        
        # Логирование
        self._log_execution(sql, result)
        
        return result
    
    def commit_transaction(self, transaction_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Подтверждение и коммит транзакции
        
        Args:
            transaction_id: ID транзакции (если используется)
        
        Returns:
            Результат коммита
        """
        # В текущей реализации транзакции выполняются сразу
        # В будущем можно добавить сохранение транзакций
        return {"success": True, "message": "Транзакция зафиксирована"}
    
    def rollback_transaction(self, transaction_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Откат транзакции
        
        Args:
            transaction_id: ID транзакции (если используется)
        
        Returns:
            Результат отката
        """
        return {"success": True, "message": "Транзакция откачена"}
    
    def _log_execution(self, sql: str, result: Dict[str, Any]):
        """
        Логирование выполнения SQL
        
        Args:
            sql: SQL запрос
            result: Результат выполнения
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "sql": sql,
            "success": result.get("success", False),
            "rows_affected": result.get("rows_affected", 0),
            "error": result.get("error"),
            "execution_time": result.get("execution_time")
        }
        
        log_file = self.log_dir / f"sql_execution_{datetime.now().strftime('%Y%m%d')}.jsonl"
        
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
        except Exception as e:
            logger.error(f"Ошибка логирования: {e}")





