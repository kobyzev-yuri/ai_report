# Быстрое развертывание RAG системы

## 🚀 Вариант 1: Docker Compose (1 команда)

```bash
cd deploy
cp config.env.example config.env
nano config.env  # Заполните Oracle настройки
./deploy.sh docker
```

**Готово!** Приложение доступно на `http://localhost:8504`

---

## 📋 Вариант 2: Ручное развертывание

```bash
cd deploy

# 1. Настройка
cp config.env.example config.env
nano config.env

# 2. Зависимости
pip install -r requirements.txt

# 3. Qdrant
docker run -d --name ai_report_qdrant -p 6333:6333 qdrant/qdrant

# 4. KB
./init_kb.sh

# 5. Streamlit
./run_streamlit_background.sh
```

---

## ✅ Проверка

```bash
# Статус всех сервисов
./status_all.sh

# Qdrant
curl http://localhost:6333/health

# Streamlit
curl http://localhost:8504/_stcore/health
```

---

## 📚 Документация

- Полная: `DEPLOYMENT_RAG.md`
- Резюме: `DEPLOYMENT_SUMMARY.md`
- Быстрый старт: `README_DEPLOYMENT.md`


