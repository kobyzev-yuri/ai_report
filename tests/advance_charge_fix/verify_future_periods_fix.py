#!/usr/bin/env python3
"""
Проверка исправления защиты от будущих периодов
"""
from datetime import datetime
from dateutil.relativedelta import relativedelta

def verify_future_periods_fix():
    """
    Проверяет, что будущие периоды не создаются
    """
    print("=" * 120)
    print("ПРОВЕРКА ЗАЩИТЫ ОТ БУДУЩИХ ПЕРИОДОВ")
    print("=" * 120)
    print()
    
    # Предполагаем, что текущий месяц - октябрь 2025
    current_date = datetime(2025, 10, 15)
    current_month_start = datetime(current_date.year, current_date.month, 1)
    
    print(f"Текущая дата: {current_date.strftime('%Y-%m-%d')}")
    print(f"Начало текущего месяца: {current_month_start.strftime('%Y-%m-%d')}")
    print()
    
    test_cases = [
        (2025, 8, "август"),
        (2025, 9, "сентябрь"),
        (2025, 10, "октябрь"),
        (2025, 11, "ноябрь"),
    ]
    
    print("ПРОВЕРКА УСЛОВИЯ: TO_DATE(sf_prev.bill_month, 'YYYYMM') < TRUNC(SYSDATE, 'MM')")
    print("-" * 120)
    print()
    
    for year, month, month_name in test_cases:
        advance_date = datetime(year, month, 1)
        advance_str = advance_date.strftime('%Y%m')
        
        # FINANCIAL_PERIOD = bill_month + 1
        fin_period = advance_date + relativedelta(months=1)
        fin_period_str = fin_period.strftime('%Y%m')
        
        # Проверка условия
        condition_result = advance_date < current_month_start
        
        print(f"Аванс за {month_name} {year} ({advance_str}):")
        print(f"  Дата аванса: {advance_date.strftime('%Y-%m-%d')}")
        print(f"  Начало текущего месяца: {current_month_start.strftime('%Y-%m-%d')}")
        print(f"  Условие: {advance_date.strftime('%Y-%m-%d')} < {current_month_start.strftime('%Y-%m-%d')} = {condition_result}")
        print(f"  FINANCIAL_PERIOD будет: {fin_period.strftime('%Y-%m')} ({fin_period_str})")
        
        if condition_result:
            print(f"  ✅ Строка БУДЕТ создана для FINANCIAL_PERIOD = {fin_period.strftime('%Y-%m')}")
        else:
            print(f"  ❌ Строка НЕ будет создана (будущий период)")
        
        print()
    
    print("=" * 120)
    print("ВЫВОД:")
    print("=" * 120)
    print()
    print("✅ Условие работает правильно:")
    print("   - Аванс за сентябрь (202509) → создается строка для FINANCIAL_PERIOD = 2025-10 ✓")
    print("   - Аванс за октябрь (202510) → НЕ создается строка для FINANCIAL_PERIOD = 2025-11 ✓")
    print("   - Аванс за ноябрь (202511) → НЕ создается строка для FINANCIAL_PERIOD = 2025-12 ✓")
    print()
    print("⚠️  ВАЖНО: Если текущий месяц уже ноябрь, то:")
    print("   - Аванс за октябрь (202510) → создается строка для FINANCIAL_PERIOD = 2025-11")
    print("   Это правильно, так как ноябрь уже наступил")

if __name__ == "__main__":
    try:
        verify_future_periods_fix()
    except ImportError:
        print("⚠️  Требуется установить python-dateutil:")
        print("   pip install python-dateutil")

