#!/usr/bin/env python3
"""
Тест подключения к LLM (OpenAI API)
"""
import os
import sys
from pathlib import Path

# Загрузка config.env
project_root = Path(__file__).parent
config_env = project_root / "config.env"

if config_env.exists():
    with open(config_env, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

# Проверка переменных окружения
api_key = os.getenv("OPENAI_API_KEY")
# Поддержка обоих вариантов: OPENAI_BASE_URL (как в sql4A) и OPENAI_API_BASE
api_base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")

print("=" * 60)
print("Проверка подключения к LLM")
print("=" * 60)
print(f"OPENAI_API_KEY: {'✅ установлен' if api_key else '❌ не установлен'}")
print(f"OPENAI_API_BASE: {api_base}")

if not api_key:
    print("\n❌ OPENAI_API_KEY не установлен в config.env")
    print("Добавьте в config.env:")
    print("OPENAI_API_KEY=your-api-key")
    print("OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1  # опционально")
    sys.exit(1)

# Проверка библиотеки
try:
    from openai import OpenAI
    print("✅ Библиотека openai установлена")
except ImportError:
    print("❌ Библиотека openai не установлена")
    print("Установите: pip install openai")
    sys.exit(1)

# Тест подключения
print("\nТестирование подключения...")
try:
    client_kwargs = {"api_key": api_key}
    if api_base and api_base != "https://api.openai.com/v1":
        client_kwargs["base_url"] = api_base
    
    client = OpenAI(**client_kwargs)
    
    # Простой тестовый запрос
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": "Скажи 'Привет' одним словом"}
        ],
        max_tokens=10
    )
    
    result = response.choices[0].message.content.strip()
    print(f"✅ Подключение успешно!")
    print(f"Ответ LLM: {result}")
    
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    import traceback
    print("\nДетали ошибки:")
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
print("✅ Все проверки пройдены!")
print("=" * 60)

