-- ============================================================================
-- V_7206_IFINDEX_REPLACEMENT
-- VIEW для операторов: показывает сервисы, которые нужно обновить
-- Оптимизированная версия для производительности
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

-- Функция для замены всех MAC адресов в строке
CREATE OR REPLACE FUNCTION REPLACE_ALL_MACS(p_value IN VARCHAR2, p_services_ext_id IN NUMBER) RETURN VARCHAR2 IS
    v_result VARCHAR2(4000) := p_value;
    v_working_mac VARCHAR2(20);
    v_spare_mac VARCHAR2(20);
    CURSOR c_macs IS
        SELECT working_mac, spare_mac
        FROM V_7206_IFINDEX_MAPPING
        WHERE p_value LIKE '%mac ' || working_mac || '%'
        ORDER BY working_index;
BEGIN
    FOR rec IN c_macs LOOP
        v_result := REPLACE(v_result, 'mac ' || rec.working_mac, 'mac ' || rec.spare_mac);
    END LOOP;
    RETURN v_result;
END;
/

-- VIEW с маппингом индексов (только десятичные индексы, MAC адреса вычисляются динамически)
CREATE OR REPLACE VIEW V_7206_IFINDEX_MAPPING AS
SELECT 
    working_index,
    spare_index,
    interface,
    -- Преобразуем индексы в MAC адреса наивным способом: индекс 254 -> MAC 00:00:00:00:02:54
    -- MAC формат: 00:00:00:00:XX:XX (первые 4 байта всегда 00:00:00:00)
    '00:00:00:00:' || LPAD(SUBSTR(LPAD(TO_CHAR(working_index), 4, '0'), 1, 2), 2, '0') || ':' || 
    LPAD(SUBSTR(LPAD(TO_CHAR(working_index), 4, '0'), 3, 2), 2, '0') AS working_mac,
    '00:00:00:00:' || LPAD(SUBSTR(LPAD(TO_CHAR(spare_index), 4, '0'), 1, 2), 2, '0') || ':' || 
    LPAD(SUBSTR(LPAD(TO_CHAR(spare_index), 4, '0'), 3, 2), 2, '0') AS spare_mac
FROM (
    SELECT 291 AS working_index, 119 AS spare_index, 'GigabitEthernet0/2.2114' AS interface FROM DUAL UNION ALL
    SELECT 134, 70, 'GigabitEthernet0/2.240' FROM DUAL UNION ALL
    SELECT 310, 105, 'GigabitEthernet0/2.1521' FROM DUAL UNION ALL
    SELECT 293, 143, 'GigabitEthernet0/2.4000' FROM DUAL UNION ALL
    SELECT 150, 78, 'GigabitEthernet0/2.253' FROM DUAL UNION ALL
    SELECT 280, 114, 'GigabitEthernet0/2.2107' FROM DUAL UNION ALL
    SELECT 113, 58, 'GigabitEthernet0/2.215' FROM DUAL UNION ALL
    SELECT 294, 121, 'GigabitEthernet0/2.2116' FROM DUAL UNION ALL
    SELECT 103, 50, 'GigabitEthernet0/2.204' FROM DUAL UNION ALL
    SELECT 240, 109, 'GigabitEthernet0/2.1855' FROM DUAL UNION ALL
    SELECT 278, 112, 'GigabitEthernet0/2.2105' FROM DUAL UNION ALL
    SELECT 146, 76, 'GigabitEthernet0/2.251' FROM DUAL UNION ALL
    SELECT 122, 63, 'GigabitEthernet0/2.226' FROM DUAL UNION ALL
    SELECT 5, 4, 'GigabitEthernet0/3' FROM DUAL UNION ALL
    SELECT 163, 82, 'GigabitEthernet0/2.260' FROM DUAL UNION ALL
    SELECT 309, 101, 'GigabitEthernet0/2.1501' FROM DUAL UNION ALL
    SELECT 322, 128, 'GigabitEthernet0/2.2125' FROM DUAL UNION ALL
    SELECT 171, 83, 'GigabitEthernet0/2.262' FROM DUAL UNION ALL
    SELECT 2, 1, 'GigabitEthernet0/1' FROM DUAL UNION ALL
    SELECT 83, 49, 'GigabitEthernet0/2.202' FROM DUAL UNION ALL
    SELECT 332, 107, 'GigabitEthernet0/2.1525' FROM DUAL UNION ALL
    SELECT 158, 80, 'GigabitEthernet0/2.257' FROM DUAL UNION ALL
    SELECT 110, 55, 'GigabitEthernet0/2.211' FROM DUAL UNION ALL
    SELECT 285, 117, 'GigabitEthernet0/2.2112' FROM DUAL UNION ALL
    SELECT 12, 144, 'Tunnel0' FROM DUAL UNION ALL
    SELECT 139, 73, 'GigabitEthernet0/2.246' FROM DUAL UNION ALL
    SELECT 300, 48, 'GigabitEthernet0/2.69' FROM DUAL UNION ALL
    SELECT 111, 56, 'GigabitEthernet0/2.213' FROM DUAL UNION ALL
    SELECT 154, 79, 'GigabitEthernet0/2.255' FROM DUAL UNION ALL
    SELECT 269, 110, 'GigabitEthernet0/2.2101' FROM DUAL UNION ALL
    SELECT 306, 126, 'GigabitEthernet0/2.2123' FROM DUAL UNION ALL
    SELECT 221, 92, 'GigabitEthernet0/2.277' FROM DUAL UNION ALL
    SELECT 119, 62, 'GigabitEthernet0/2.222' FROM DUAL UNION ALL
    SELECT 333, 131, 'GigabitEthernet0/2.2130' FROM DUAL UNION ALL
    SELECT 127, 67, 'GigabitEthernet0/2.233' FROM DUAL UNION ALL
    SELECT 204, 90, 'GigabitEthernet0/2.275' FROM DUAL UNION ALL
    SELECT 302, 124, 'GigabitEthernet0/2.2121' FROM DUAL UNION ALL
    SELECT 337, 133, 'GigabitEthernet0/2.2132' FROM DUAL UNION ALL
    SELECT 177, 85, 'GigabitEthernet0/2.266' FROM DUAL UNION ALL
    SELECT 117, 61, 'GigabitEthernet0/2.220' FROM DUAL UNION ALL
    SELECT 344, 103, 'GigabitEthernet0/2.1507' FROM DUAL UNION ALL
    SELECT 338, 141, 'GigabitEthernet0/2.2507' FROM DUAL UNION ALL
    SELECT 220, 42, 'GigabitEthernet0/2.123' FROM DUAL UNION ALL
    SELECT 33, 99, 'GigabitEthernet0/2.999' FROM DUAL UNION ALL
    SELECT 331, 139, 'GigabitEthernet0/2.2505' FROM DUAL UNION ALL
    SELECT 215, 40, 'GigabitEthernet0/2.121' FROM DUAL UNION ALL
    SELECT 202, 87, 'GigabitEthernet0/2.268' FROM DUAL UNION ALL
    SELECT 90, 31, 'GigabitEthernet0/2.101' FROM DUAL UNION ALL
    SELECT 326, 137, 'GigabitEthernet0/2.2503' FROM DUAL UNION ALL
    SELECT 298, 123, 'GigabitEthernet0/2.2118' FROM DUAL UNION ALL
    SELECT 107, 52, 'GigabitEthernet0/2.208' FROM DUAL UNION ALL
    SELECT 321, 135, 'GigabitEthernet0/2.2501' FROM DUAL UNION ALL
    SELECT 231, 43, 'GigabitEthernet0/2.125' FROM DUAL UNION ALL
    SELECT 342, 28, 'GigabitEthernet0/2.50' FROM DUAL UNION ALL
    SELECT 68, 5, 'VoIP-Null0' FROM DUAL UNION ALL
    SELECT 217, 23, 'GigabitEthernet0/2.16' FROM DUAL UNION ALL
    SELECT 133, 69, 'GigabitEthernet0/2.239' FROM DUAL UNION ALL
    SELECT 101, 34, 'GigabitEthernet0/2.107' FROM DUAL UNION ALL
    SELECT 191, 37, 'GigabitEthernet0/2.116' FROM DUAL UNION ALL
    SELECT 210, 94, 'GigabitEthernet0/2.280' FROM DUAL UNION ALL
    SELECT 329, 130, 'GigabitEthernet0/2.2129' FROM DUAL UNION ALL
    SELECT 124, 64, 'GigabitEthernet0/2.228' FROM DUAL UNION ALL
    SELECT 98, 32, 'GigabitEthernet0/2.105' FROM DUAL UNION ALL
    SELECT 11, 7, 'Loopback0' FROM DUAL UNION ALL
    SELECT 148, 77, 'GigabitEthernet0/2.252' FROM DUAL UNION ALL
    SELECT 279, 113, 'GigabitEthernet0/2.2106' FROM DUAL UNION ALL
    SELECT 112, 57, 'GigabitEthernet0/2.214' FROM DUAL UNION ALL
    SELECT 292, 120, 'GigabitEthernet0/2.2115' FROM DUAL UNION ALL
    SELECT 135, 71, 'GigabitEthernet0/2.241' FROM DUAL UNION ALL
    SELECT 308, 104, 'GigabitEthernet0/2.1520' FROM DUAL UNION ALL
    SELECT 114, 59, 'GigabitEthernet0/2.216' FROM DUAL UNION ALL
    SELECT 147, 75, 'GigabitEthernet0/2.250' FROM DUAL UNION ALL
    SELECT 137, 72, 'GigabitEthernet0/2.243' FROM DUAL UNION ALL
    SELECT 295, 122, 'GigabitEthernet0/2.2117' FROM DUAL UNION ALL
    SELECT 104, 51, 'GigabitEthernet0/2.205' FROM DUAL UNION ALL
    SELECT 314, 106, 'GigabitEthernet0/2.1522' FROM DUAL UNION ALL
    SELECT 4, 3, 'GigabitEthernet0/2' FROM DUAL UNION ALL
    SELECT 307, 100, 'GigabitEthernet0/2.1500' FROM DUAL UNION ALL
    SELECT 197, 88, 'GigabitEthernet0/2.272' FROM DUAL UNION ALL
    SELECT 174, 84, 'GigabitEthernet0/2.263' FROM DUAL UNION ALL
    SELECT 160, 35, 'GigabitEthernet0/2.108' FROM DUAL UNION ALL
    SELECT 320, 127, 'GigabitEthernet0/2.2124' FROM DUAL UNION ALL
    SELECT 270, 111, 'GigabitEthernet0/2.2102' FROM DUAL UNION ALL
    SELECT 109, 54, 'GigabitEthernet0/2.210' FROM DUAL UNION ALL
    SELECT 282, 116, 'GigabitEthernet0/2.2111' FROM DUAL UNION ALL
    SELECT 345, 142, 'GigabitEthernet0/2.2508' FROM DUAL UNION ALL
    SELECT 13, 145, 'Tunnel1' FROM DUAL UNION ALL
    SELECT 287, 118, 'GigabitEthernet0/2.2113' FROM DUAL UNION ALL
    SELECT 140, 74, 'GigabitEthernet0/2.247' FROM DUAL UNION ALL
    SELECT 30, 48, 'GigabitEthernet0/2.201' FROM DUAL UNION ALL
    SELECT 339, 108, 'GigabitEthernet0/2.1526' FROM DUAL UNION ALL
    SELECT 335, 132, 'GigabitEthernet0/2.2131' FROM DUAL UNION ALL
    SELECT 254, 98, 'GigabitEthernet0/2.515' FROM DUAL UNION ALL
    SELECT 303, 125, 'GigabitEthernet0/2.2122' FROM DUAL UNION ALL
    SELECT 205, 91, 'GigabitEthernet0/2.276' FROM DUAL UNION ALL
    SELECT 126, 65, 'GigabitEthernet0/2.230' FROM DUAL UNION ALL
    SELECT 341, 134, 'GigabitEthernet0/2.2133' FROM DUAL UNION ALL
    SELECT 178, 86, 'GigabitEthernet0/2.267' FROM DUAL UNION ALL
    SELECT 340, 102, 'GigabitEthernet0/2.1506' FROM DUAL UNION ALL
    SELECT 199, 47, 'GigabitEthernet0/2.159' FROM DUAL UNION ALL
    SELECT 151, 66, 'GigabitEthernet0/2.232' FROM DUAL UNION ALL
    SELECT 200, 89, 'GigabitEthernet0/2.274' FROM DUAL UNION ALL
    SELECT 336, 140, 'GigabitEthernet0/2.2506' FROM DUAL UNION ALL
    SELECT 24, 25, 'GigabitEthernet0/2.20' FROM DUAL UNION ALL
    SELECT 226, 41, 'GigabitEthernet0/2.122' FROM DUAL UNION ALL
    SELECT 346, 17, 'Tunnel208' FROM DUAL UNION ALL
    SELECT 162, 81, 'GigabitEthernet0/2.258' FROM DUAL UNION ALL
    SELECT 330, 138, 'GigabitEthernet0/2.2504' FROM DUAL UNION ALL
    SELECT 334, 18, 'Tunnel450' FROM DUAL UNION ALL
    SELECT 207, 39, 'GigabitEthernet0/2.120' FROM DUAL UNION ALL
    SELECT 3, 2, 'FastEthernet0/2' FROM DUAL UNION ALL
    SELECT 81, 22, 'GigabitEthernet0/2.13' FROM DUAL UNION ALL
    SELECT 179, 36, 'GigabitEthernet0/2.113' FROM DUAL UNION ALL
    SELECT 159, 46, 'GigabitEthernet0/2.155' FROM DUAL UNION ALL
    SELECT 234, 15, 'GigabitEthernet0/3.24' FROM DUAL UNION ALL
    SELECT 239, 96, 'GigabitEthernet0/2.285' FROM DUAL UNION ALL
    SELECT 219, 93, 'GigabitEthernet0/2.278' FROM DUAL UNION ALL
    SELECT 31, 60, 'GigabitEthernet0/2.218' FROM DUAL UNION ALL
    SELECT 325, 136, 'GigabitEthernet0/2.2502' FROM DUAL UNION ALL
    SELECT 102, 26, 'GigabitEthernet0/2.24' FROM DUAL UNION ALL
    SELECT 232, 44, 'GigabitEthernet0/2.126' FROM DUAL UNION ALL
    SELECT 281, 115, 'GigabitEthernet0/2.2108' FROM DUAL UNION ALL
    SELECT 108, 53, 'GigabitEthernet0/2.209' FROM DUAL UNION ALL
    SELECT 26, 27, 'GigabitEthernet0/2.26' FROM DUAL UNION ALL
    SELECT 100, 33, 'GigabitEthernet0/2.106' FROM DUAL UNION ALL
    SELECT 244, 97, 'GigabitEthernet0/2.290' FROM DUAL UNION ALL
    SELECT 214, 95, 'GigabitEthernet0/2.283' FROM DUAL UNION ALL
    SELECT 230, 24, 'GigabitEthernet0/2.17' FROM DUAL UNION ALL
    SELECT 132, 68, 'GigabitEthernet0/2.238' FROM DUAL UNION ALL
    SELECT 195, 38, 'GigabitEthernet0/2.117' FROM DUAL UNION ALL
    SELECT 92, 45, 'GigabitEthernet0/2.151' FROM DUAL UNION ALL
    SELECT 328, 129, 'GigabitEthernet0/2.2128' FROM DUAL
);

-- Основной VIEW для операторов: максимально оптимизированная версия
-- Убраны рекурсивные CTE, множественные подзапросы заменены на JOIN
CREATE OR REPLACE VIEW V_7206_IFINDEX_REPLACEMENT AS
WITH base_services AS (
    -- Базовый набор сервисов для обновления (используем индексы для быстрого поиска)
    SELECT 
        se.SERVICES_EXT_ID,
        se.SERVICE_ID,
        se.VALUE AS original_value,
        s.CUSTOMER_ID,
        s.ACCOUNT_ID,
        se.DATE_BEG,
        se.DICT_ID
    FROM SERVICES_EXT se
    JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
    WHERE se.DATE_END IS NULL
      AND se.DICT_ID = 14009
      AND s.TYPE_ID = 10
),
services_with_mac AS (
    -- Находим сервисы, которые содержат MAC адреса для замены
    SELECT DISTINCT
        bs.SERVICES_EXT_ID,
        bs.SERVICE_ID,
        bs.original_value,
        bs.CUSTOMER_ID,
        bs.ACCOUNT_ID,
        bs.DATE_BEG,
        bs.DICT_ID
    FROM base_services bs
    WHERE EXISTS (
        SELECT 1 FROM V_7206_IFINDEX_MAPPING m 
        WHERE bs.original_value LIKE '%mac ' || m.working_mac || '%'
    )
),
mac_replacements AS (
    -- Находим все MAC адреса для каждой записи (только первый для отображения)
    SELECT 
        swm.SERVICES_EXT_ID,
        m.working_index,
        m.spare_index,
        m.working_mac,
        m.spare_mac,
        m.interface,
        ROW_NUMBER() OVER (PARTITION BY swm.SERVICES_EXT_ID ORDER BY m.working_index) AS rn
    FROM services_with_mac swm
    JOIN V_7206_IFINDEX_MAPPING m ON swm.original_value LIKE '%mac ' || m.working_mac || '%'
),
index_changes AS (
    -- Схема замены индексов в формате "330->138,336->140"
    SELECT 
        SERVICES_EXT_ID,
        LISTAGG(working_index || '->' || spare_index, ', ') 
            WITHIN GROUP (ORDER BY working_index) AS INDEX_CHANGES
    FROM mac_replacements
    GROUP BY SERVICES_EXT_ID
),
-- Применяем замены через функцию для замены ВСЕХ MAC адресов
replaced_value AS (
    SELECT 
        swm.SERVICES_EXT_ID,
        REPLACE_ALL_MACS(swm.original_value, swm.SERVICES_EXT_ID) AS NEW_VALUE
    FROM services_with_mac swm
),
customer_info AS (
    -- Получаем информацию о клиентах только для нужных записей
    SELECT 
        swm.CUSTOMER_ID,
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END) AS ORGANIZATION_NAME,
        TRIM(
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
        ) AS PERSON_NAME
    FROM services_with_mac swm
    LEFT JOIN BM_CUSTOMER_CONTACT cc ON cc.CUSTOMER_ID = swm.CUSTOMER_ID
    LEFT JOIN BM_CONTACT_DICT cd ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
    GROUP BY swm.CUSTOMER_ID
)
SELECT 
    swm.SERVICES_EXT_ID,
    swm.SERVICE_ID,
    swm.CUSTOMER_ID,
    swm.ACCOUNT_ID,
    COALESCE(ci.ORGANIZATION_NAME, ci.PERSON_NAME, 'Не указано') AS CUSTOMER_NAME,
    swm.original_value AS OLD_VALUE,
    COALESCE(rv.NEW_VALUE, swm.original_value) AS NEW_VALUE,
    ic.INDEX_CHANGES,
    swm.DATE_BEG,
    swm.DICT_ID
FROM services_with_mac swm
LEFT JOIN replaced_value rv ON swm.SERVICES_EXT_ID = rv.SERVICES_EXT_ID
LEFT JOIN index_changes ic ON swm.SERVICES_EXT_ID = ic.SERVICES_EXT_ID
LEFT JOIN customer_info ci ON swm.CUSTOMER_ID = ci.CUSTOMER_ID;
