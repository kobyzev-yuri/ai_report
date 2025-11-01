cd oracle/test
PGPASSWORD=1234 python import_iridium.py \
     --input V_IRIDIUM_SERVICES_INFO.txt \
     --dsn "host=localhost dbname=billing user=postgres password=1234" \
     --truncate
