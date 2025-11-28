#!/usr/bin/env python3
"""
Модуль для расчета стоимости превышения трафика SBD
Расчет ведется по ступенчатым тарифам с учетом включенного объема
"""

from typing import Dict, Optional, Tuple
from decimal import Decimal, ROUND_HALF_UP


class TariffPlan:
    """Класс тарифного плана с ценами превышения"""
    
    def __init__(self, plan_name: str, plan_code: str, included_kb: float,
                 tier1_from: float, tier1_to: float, tier1_price: float,
                 tier2_from: float, tier2_to: float, tier2_price: float,
                 tier3_from: float, tier3_price: float):
        """
        Инициализация тарифного плана
        
        Args:
            plan_name: Полное название плана (например, "SBD Tiered 1250 10K")
            plan_code: Короткий код (например, "SBD-10")
            included_kb: Объем включенного трафика в КБ
            tier1_from: Начало первой ступени (КБ)
            tier1_to: Конец первой ступени (КБ)
            tier1_price: Цена за 1 КБ на первой ступени (USD)
            tier2_from: Начало второй ступени (КБ)
            tier2_to: Конец второй ступени (КБ)
            tier2_price: Цена за 1 КБ на второй ступени (USD)
            tier3_from: Начало третьей ступени (КБ)
            tier3_price: Цена за 1 КБ на третьей ступени (USD)
        """
        self.plan_name = plan_name
        self.plan_code = plan_code
        self.included_kb = included_kb
        
        self.tier1_from = tier1_from
        self.tier1_to = tier1_to
        self.tier1_price = Decimal(str(tier1_price))
        
        self.tier2_from = tier2_from
        self.tier2_to = tier2_to
        self.tier2_price = Decimal(str(tier2_price))
        
        self.tier3_from = tier3_from
        self.tier3_price = Decimal(str(tier3_price))
    
    def calculate_overage(self, usage_bytes: float) -> Tuple[Decimal, Dict]:
        """
        Рассчитать стоимость превышения
        
        Args:
            usage_bytes: Использованный трафик в байтах
            
        Returns:
            Tuple[Decimal, Dict]: (итоговая стоимость, детали расчета)
        """
        # Конвертируем байты в килобайты
        usage_kb = Decimal(str(usage_bytes)) / Decimal('1000')
        
        details = {
            'usage_bytes': usage_bytes,
            'usage_kb': float(usage_kb),
            'included_kb': self.included_kb,
            'overage_kb': 0,
            'tier1_kb': 0,
            'tier1_charge': Decimal('0'),
            'tier2_kb': 0,
            'tier2_charge': Decimal('0'),
            'tier3_kb': 0,
            'tier3_charge': Decimal('0'),
            'total_charge': Decimal('0')
        }
        
        # Если использование в пределах включенного объема
        if usage_kb <= Decimal(str(self.included_kb)):
            return Decimal('0'), details
        
        # Рассчитываем превышение
        overage_kb = usage_kb - Decimal(str(self.included_kb))
        details['overage_kb'] = float(overage_kb)
        
        total_charge = Decimal('0')
        
        # Tier 1
        if usage_kb > Decimal(str(self.tier1_from)):
            if usage_kb > Decimal(str(self.tier1_to)):
                tier1_kb = Decimal(str(self.tier1_to)) - Decimal(str(self.tier1_from))
            else:
                tier1_kb = usage_kb - Decimal(str(self.tier1_from))
            
            tier1_charge = tier1_kb * self.tier1_price
            total_charge += tier1_charge
            
            details['tier1_kb'] = float(tier1_kb)
            details['tier1_charge'] = tier1_charge
        
        # Tier 2
        if usage_kb > Decimal(str(self.tier2_from)):
            if usage_kb > Decimal(str(self.tier2_to)):
                tier2_kb = Decimal(str(self.tier2_to)) - Decimal(str(self.tier2_from))
            else:
                tier2_kb = usage_kb - Decimal(str(self.tier2_from))
            
            tier2_charge = tier2_kb * self.tier2_price
            total_charge += tier2_charge
            
            details['tier2_kb'] = float(tier2_kb)
            details['tier2_charge'] = tier2_charge
        
        # Tier 3
        if usage_kb > Decimal(str(self.tier3_from)):
            tier3_kb = usage_kb - Decimal(str(self.tier3_from))
            tier3_charge = tier3_kb * self.tier3_price
            total_charge += tier3_charge
            
            details['tier3_kb'] = float(tier3_kb)
            details['tier3_charge'] = tier3_charge
        
        # Округляем до 2 знаков
        total_charge = total_charge.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        details['total_charge'] = total_charge
        
        return total_charge, details
    
    def __repr__(self):
        return f"TariffPlan('{self.plan_name}', code='{self.plan_code}', included={self.included_kb}KB)"


class TariffCalculator:
    """Калькулятор стоимости превышения для различных тарифных планов"""
    
    def __init__(self):
        """Инициализация калькулятора с предустановленными тарифами"""
        self.tariffs = {}
        self._initialize_tariffs()
    
    def _initialize_tariffs(self):
        """Инициализация тарифных планов"""
        
        # SBD-1: 1 КБ включено
        # Превышение: 1-10 КБ: $1.50/КБ, 10-25 КБ: $0.75/КБ, свыше 25 КБ: $0.50/КБ
        self.tariffs['SBD Tiered 1250 1K'] = TariffPlan(
            plan_name='SBD Tiered 1250 1K',
            plan_code='SBD-1',
            included_kb=1,
            tier1_from=1, tier1_to=10, tier1_price=1.50,
            tier2_from=10, tier2_to=25, tier2_price=0.75,
            tier3_from=25, tier3_price=0.50
        )
        
        # SBD-10: 10 КБ включено
        # Превышение: 10-25 КБ: $0.30/КБ, 25-50 КБ: $0.20/КБ, свыше 50 КБ: $0.10/КБ
        self.tariffs['SBD Tiered 1250 10K'] = TariffPlan(
            plan_name='SBD Tiered 1250 10K',
            plan_code='SBD-10',
            included_kb=10,
            tier1_from=10, tier1_to=25, tier1_price=0.30,
            tier2_from=25, tier2_to=50, tier2_price=0.20,
            tier3_from=50, tier3_price=0.10
        )
    
    def add_tariff(self, tariff: TariffPlan):
        """
        Добавить новый тарифный план
        
        Args:
            tariff: Объект TariffPlan
        """
        self.tariffs[tariff.plan_name] = tariff
    
    def calculate_overage(self, plan_name: str, usage_bytes: float, 
                         detailed: bool = False) -> float:
        """
        Рассчитать стоимость превышения для заданного плана и использования
        
        Args:
            plan_name: Название тарифного плана
            usage_bytes: Использованный трафик в байтах
            detailed: Вернуть детальную информацию о расчете
            
        Returns:
            float: Стоимость превышения в USD (или dict с деталями если detailed=True)
            
        Raises:
            ValueError: Если тарифный план не найден
        """
        if plan_name not in self.tariffs:
            raise ValueError(f"Тарифный план '{plan_name}' не найден")
        
        tariff = self.tariffs[plan_name]
        charge, details = tariff.calculate_overage(usage_bytes)
        
        if detailed:
            return {
                'plan_name': plan_name,
                'plan_code': tariff.plan_code,
                'charge': float(charge),
                'details': details
            }
        
        return float(charge)
    
    def get_tariff_info(self, plan_name: str) -> Optional[Dict]:
        """
        Получить информацию о тарифном плане
        
        Args:
            plan_name: Название тарифного плана
            
        Returns:
            Dict: Информация о тарифе или None если не найден
        """
        if plan_name not in self.tariffs:
            return None
        
        tariff = self.tariffs[plan_name]
        return {
            'plan_name': tariff.plan_name,
            'plan_code': tariff.plan_code,
            'included_kb': tariff.included_kb,
            'tier1': f"{tariff.tier1_from}-{tariff.tier1_to} KB: ${tariff.tier1_price}/KB",
            'tier2': f"{tariff.tier2_from}-{tariff.tier2_to} KB: ${tariff.tier2_price}/KB",
            'tier3': f"{tariff.tier3_from}+ KB: ${tariff.tier3_price}/KB"
        }
    
    def list_tariffs(self) -> list:
        """Получить список всех доступных тарифных планов"""
        return list(self.tariffs.keys())


# Глобальный экземпляр калькулятора
calculator = TariffCalculator()


def calculate_overage(plan_name: str, usage_bytes: float, detailed: bool = False):
    """
    Удобная функция для расчета превышения
    
    Args:
        plan_name: Название тарифного плана
        usage_bytes: Использованный трафик в байтах
        detailed: Вернуть детальную информацию
        
    Returns:
        float или dict: Стоимость превышения (или детали если detailed=True)
    """
    return calculator.calculate_overage(plan_name, usage_bytes, detailed)


def get_tariff_info(plan_name: str):
    """Получить информацию о тарифном плане"""
    return calculator.get_tariff_info(plan_name)


def list_tariffs():
    """Получить список всех тарифных планов"""
    return calculator.list_tariffs()


# =====================================================
# Тесты и примеры использования
# =====================================================

def run_tests():
    """Запуск тестов для проверки правильности расчетов"""
    
    print("="*80)
    print("ТЕСТИРОВАНИЕ РАСЧЕТОВ ПРЕВЫШЕНИЯ")
    print("="*80)
    
    test_cases = [
        {
            'name': 'SBD-1: 30 KB (30000 bytes)',
            'plan': 'SBD Tiered 1250 1K',
            'usage': 30000,
            'expected': 27.25,
            'explanation': '1-10 KB: 9×$1.50=$13.50, 10-25 KB: 15×$0.75=$11.25, 25+ KB: 5×$0.50=$2.50'
        },
        {
            'name': 'SBD-10: 35 KB (35000 bytes)',
            'plan': 'SBD Tiered 1250 10K',
            'usage': 35000,
            'expected': 6.50,
            'explanation': '10-25 KB: 15×$0.30=$4.50, 25-50 KB: 10×$0.20=$2.00'
        },
        {
            'name': 'SBD-10: 6 KB (6000 bytes) - в пределах включенного',
            'plan': 'SBD Tiered 1250 10K',
            'usage': 6000,
            'expected': 0.00,
            'explanation': 'Использовано меньше включенного объема (10 KB)'
        },
        {
            'name': 'SBD-10: 60 KB (60000 bytes) - все три ступени',
            'plan': 'SBD Tiered 1250 10K',
            'usage': 60000,
            'expected': 10.50,
            'explanation': '10-25 KB: 15×$0.30=$4.50, 25-50 KB: 25×$0.20=$5.00, 50+ KB: 10×$0.10=$1.00'
        },
        {
            'name': 'SBD-1: 0.5 KB (500 bytes) - в пределах включенного',
            'plan': 'SBD Tiered 1250 1K',
            'usage': 500,
            'expected': 0.00,
            'explanation': 'Использовано меньше включенного объема (1 KB)'
        }
    ]
    
    print()
    passed = 0
    failed = 0
    
    for test in test_cases:
        print(f"\nТест: {test['name']}")
        print(f"План: {test['plan']}")
        print(f"Использование: {test['usage']} bytes = {test['usage']/1000} KB")
        print(f"Ожидаемый результат: ${test['expected']}")
        print(f"Логика: {test['explanation']}")
        
        result = calculate_overage(test['plan'], test['usage'], detailed=True)
        actual = result['charge']
        
        print(f"Рассчитанный результат: ${actual}")
        
        # Детали расчета
        details = result['details']
        if details['tier1_kb'] > 0:
            print(f"  Tier 1: {details['tier1_kb']} KB × price = ${details['tier1_charge']}")
        if details['tier2_kb'] > 0:
            print(f"  Tier 2: {details['tier2_kb']} KB × price = ${details['tier2_charge']}")
        if details['tier3_kb'] > 0:
            print(f"  Tier 3: {details['tier3_kb']} KB × price = ${details['tier3_charge']}")
        
        if abs(actual - test['expected']) < 0.01:
            print("✓ PASSED")
            passed += 1
        else:
            print(f"✗ FAILED (разница: ${abs(actual - test['expected'])})")
            failed += 1
    
    print("\n" + "="*80)
    print(f"Результаты тестирования: {passed} passed, {failed} failed")
    print("="*80)
    
    return failed == 0


def print_tariff_info():
    """Вывод информации о всех тарифных планах"""
    
    print("\n" + "="*80)
    print("ДОСТУПНЫЕ ТАРИФНЫЕ ПЛАНЫ")
    print("="*80)
    
    for plan_name in list_tariffs():
        info = get_tariff_info(plan_name)
        print(f"\n{info['plan_code']}: {info['plan_name']}")
        print(f"  Включено: {info['included_kb']} KB")
        print(f"  {info['tier1']}")
        print(f"  {info['tier2']}")
        print(f"  {info['tier3']}")
    
    print("\n" + "="*80)


if __name__ == '__main__':
    # Вывод информации о тарифах
    print_tariff_info()
    
    # Запуск тестов
    success = run_tests()
    
    # Примеры использования
    print("\n" + "="*80)
    print("ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ")
    print("="*80)
    
    print("\n# Простой расчет:")
    print("charge = calculate_overage('SBD Tiered 1250 10K', 35000)")
    charge = calculate_overage('SBD Tiered 1250 10K', 35000)
    print(f"Результат: ${charge}")
    
    print("\n# Детальный расчет:")
    print("result = calculate_overage('SBD Tiered 1250 10K', 35000, detailed=True)")
    result = calculate_overage('SBD Tiered 1250 10K', 35000, detailed=True)
    print(f"Результат: {result}")
    
    print("\n# Информация о тарифе:")
    print("info = get_tariff_info('SBD Tiered 1250 10K')")
    info = get_tariff_info('SBD Tiered 1250 10K')
    print(f"Результат: {info}")
    
    print("\n" + "="*80)
    
    import sys
    sys.exit(0 if success else 1)

