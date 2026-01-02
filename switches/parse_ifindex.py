#!/usr/bin/env python3
"""
Парсинг файлов с индексами интерфейсов и создание маппинга для замены MAC адресов
"""

import re
from collections import defaultdict

def parse_ifindex_file(filename):
    """Парсит файл с индексами интерфейсов"""
    ifindex_map = {}
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            # Ищем строки вида: GigabitEthernet0/2.254: Ifindex = 152
            match = re.search(r'(\S+):\s*Ifindex\s*=\s*(\d+)', line)
            if match:
                interface = match.group(1)
                ifindex = int(match.group(2))
                ifindex_map[interface] = ifindex
    
    return ifindex_map

def index_to_mac(index):
    """Преобразует индекс интерфейса в MAC адрес формата 00:00:00:00:XX:XX (наивное преобразование)"""
    # Индекс 254 -> MAC 00:00:00:00:02:54
    # Наивное преобразование: индекс как строка из 4 символов (с ведущими нулями),
    # разбивается на два байта по 2 символа, которые используются как hex строки
    # 254 -> "0254" -> "02" и "54" -> MAC 00:00:00:02:54
    index_str = str(index).zfill(4)  # 254 -> "0254"
    prev_byte_str = index_str[0:2]   # "02"
    last_byte_str = index_str[2:4]   # "54"
    return f"00:00:00:{prev_byte_str}:{last_byte_str}"

def mac_to_index(mac):
    """Преобразует MAC адрес в индекс интерфейса (наивное преобразование)"""
    # MAC 00:00:00:00:02:54 -> индекс 254
    # Берем последние два байта как строки и объединяем их в число
    parts = mac.split(':')
    if len(parts) == 6:
        try:
            # Берем последние два байта как строки
            prev_byte_str = parts[4]  # "02"
            last_byte_str = parts[5]  # "54"
            # Объединяем и преобразуем в число
            index_str = prev_byte_str + last_byte_str  # "0254"
            index = int(index_str)  # 254
            return index
        except ValueError:
            return None
    return None

def main():
    working_file = '7206_ifindex_working.txt'
    spare_file = '7206_ifindex_spare.txt'
    
    print("Парсинг файлов...")
    working_map = parse_ifindex_file(working_file)
    spare_map = parse_ifindex_file(spare_file)
    
    print(f"\nНайдено интерфейсов в working: {len(working_map)}")
    print(f"Найдено интерфейсов в spare: {len(spare_map)}")
    
    # Находим изменения индексов
    changes = []
    for interface in working_map:
        if interface in spare_map:
            working_index = working_map[interface]
            spare_index = spare_map[interface]
            if working_index != spare_index:
                working_mac = index_to_mac(working_index)
                spare_mac = index_to_mac(spare_index)
                changes.append({
                    'interface': interface,
                    'working_index': working_index,
                    'spare_index': spare_index,
                    'working_mac': working_mac,
                    'spare_mac': spare_mac
                })
    
    print(f"\nНайдено изменений индексов: {len(changes)}")
    
    if changes:
        print("\nИзменения индексов:")
        print("-" * 100)
        print(f"{'Интерфейс':<30} {'Working Index':<15} {'Spare Index':<15} {'Working MAC':<20} {'Spare MAC':<20}")
        print("-" * 100)
        for change in changes[:20]:  # Показываем первые 20
            print(f"{change['interface']:<30} {change['working_index']:<15} {change['spare_index']:<15} "
                  f"{change['working_mac']:<20} {change['spare_mac']:<20}")
        if len(changes) > 20:
            print(f"... и еще {len(changes) - 20} изменений")
    
    # Сохраняем маппинг в SQL формат
    print("\n\nГенерация SQL маппинга...")
    with open('ifindex_mapping.sql', 'w', encoding='utf-8') as f:
        f.write("-- Маппинг MAC адресов для замены индексов интерфейсов\n")
        f.write("-- Формат: working_mac -> spare_mac\n\n")
        f.write("CREATE OR REPLACE VIEW V_7206_IFINDEX_MAPPING AS\n")
        f.write("SELECT * FROM (\n")
        f.write("    VALUES\n")
        
        for i, change in enumerate(changes):
            comma = ',' if i < len(changes) - 1 else ''
            f.write(f"        ('{change['working_mac']}', '{change['spare_mac']}', "
                   f"{change['working_index']}, {change['spare_index']}, '{change['interface']}'){comma}\n")
        
        f.write(") AS t(working_mac, spare_mac, working_index, spare_index, interface);\n")
    
    print("Маппинг сохранен в ifindex_mapping.sql")
    
    # Создаем маппинг для замены в VALUE
    print("\nГенерация маппинга для замены в VALUE...")
    with open('mac_replacement_mapping.txt', 'w', encoding='utf-8') as f:
        f.write("# Маппинг MAC адресов для замены в services_ext.VALUE\n")
        f.write("# Формат: working_mac -> spare_mac\n\n")
        for change in changes:
            f.write(f"{change['working_mac']} -> {change['spare_mac']}\n")
    
    print("Маппинг для замены сохранен в mac_replacement_mapping.txt")

if __name__ == '__main__':
    main()

