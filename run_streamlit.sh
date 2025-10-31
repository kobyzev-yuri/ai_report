#!/bin/bash
# Запуск Streamlit приложения для отчета по Iridium M2M

cd /mnt/ai/cnn/ai_report

echo "========================================"
echo "Запуск Streamlit приложения"
echo "========================================"
echo ""
echo "URL: http://localhost:8502"
echo ""

streamlit run streamlit_report.py --server.port 8502



