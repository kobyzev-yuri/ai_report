#!/usr/bin/env python3
"""
Модуль транскрибации голоса для Streamlit ассистента
"""
import os
import io
from typing import Optional, Tuple


def transcribe_audio(
    audio_file: bytes,
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    model: str = "whisper-1"
) -> Tuple[Optional[str], Optional[str]]:
    """
    Транскрибация аудио через OpenAI Whisper API (поддержка proxyapi.ru)
    
    Args:
        audio_file: Байты аудиофайла
        api_key: API ключ OpenAI (если None, берется из OPENAI_API_KEY)
        api_base: Базовый URL API (для прокси, например https://api.proxyapi.ru/openai/v1)
        model: Модель Whisper (по умолчанию whisper-1)
    
    Returns:
        Tuple[transcription, error]: Транскрипция или None и сообщение об ошибке
    """
    try:
        from openai import OpenAI
    except ImportError:
        return None, "Библиотека openai не установлена. Установите: pip install openai"
    
    # Получение API ключа (как в rag_assistant.py)
    if api_key is None:
        api_key = os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        return None, "OPENAI_API_KEY не установлен в config.env"
    
    try:
        # Инициализация клиента OpenAI (стандартный подход как в sql4A)
        client_kwargs = {"api_key": api_key}
        if api_base:
            client_kwargs["base_url"] = api_base
        elif os.getenv("OPENAI_BASE_URL"):  # Поддержка OPENAI_BASE_URL (как в sql4A)
            client_kwargs["base_url"] = os.getenv("OPENAI_BASE_URL")
        elif os.getenv("OPENAI_API_BASE"):
            client_kwargs["base_url"] = os.getenv("OPENAI_API_BASE")
        
        client = OpenAI(**client_kwargs)
        
        # Подготовка файла для API
        audio_io = io.BytesIO(audio_file)
        audio_io.name = "audio.webm"
        
        # Транскрибация через Whisper API (работает через proxyapi.ru)
        transcript = client.audio.transcriptions.create(
            model=model,
            file=audio_io,
            language="ru"
        )
        
        return transcript.text, None
        
    except Exception as e:
        return None, f"Ошибка транскрибации: {str(e)}"


def validate_audio_file(audio_file: bytes, max_size_mb: int = 25) -> Tuple[bool, Optional[str]]:
    """
    Валидация аудиофайла
    
    Args:
        audio_file: Байты аудиофайла
        max_size_mb: Максимальный размер в МБ
    
    Returns:
        Tuple[is_valid, error_message]
    """
    if not audio_file:
        return False, "Файл пуст"
    
    max_size_bytes = max_size_mb * 1024 * 1024
    if len(audio_file) > max_size_bytes:
        return False, f"Файл слишком большой (максимум {max_size_mb} МБ)"
    
    return True, None

