# 🏔️ Delta Lake Medallion Architecture | Modern Data Lakehouse

Production-ready реализация архитектуры Medallion (Bronze → Silver → Gold) на базе Delta Lake. Демонстрирует ACID-транзакции, time travel, оптимизацию хранения и сквозной Data Quality для аналитики и ML.

## 🎯 Что решает

- 🧱 **Medallion Architecture**: чёткое разделение слоёв (raw → cleaned → aggregated)
-  **Delta Lake Features**: `MERGE` (upsert), `TIME TRAVEL`, `VACUUM`, `OPTIMIZE`, `ZORDER`
- 🛡️ **Data Quality**: валидация схем, дедупликация, маскировка PII, автоматические тесты
- 📊 **Business Marts**: Star Schema для BI и Feature Store для ML
- 🚀 **Orchestration**: Airflow DAG с зависимостями слоёв и алертингом

##  Технологический стек

- **Storage:** Delta Lake, MinIO (S3-compatible)
- **Processing:** PySpark 3.4+, SQL
- **Orchestration:** Apache Airflow 2.8+
- **Quality:** Great Expectations, custom Delta checks
- **Infrastructure:** Docker, Docker Compose

##  Структура проекта
```
delta_lake_medallion/
├── dags/ # Airflow orchestration
├── scripts/ # Bronze/Silver/Gold transformations
├── tests/ # DQ & Delta validation
── config/ # Lakehouse configuration
├── docker-compose.yml # Local environment
└── README.md
```

## 🚀 Быстрый старт

### 1. **Клонируй репозиторий:**
```bash
git clone https://github.com/Giganmama/delta_lake_medallion.git
cd delta_lake_medallion
```
### 2. **Запусти инфраструктуру:**
```
docker-compose up -d
```
### 3. **Установи зависимости:**
```
pip install -r requirements.txt
```
### 4. **Запусти пайплайн:**
```
airflow dags trigger medallion_pipeline
```

## 📊 Архитектура слоёв
```
Raw Sources (CSV/JSON/API)
    ↓
🥉 Bronze: Raw Delta tables (append-only, partitioned by date)
    ↓
 Silver: Cleaned, deduplicated, PII-masked, type-casted
    ↓
🥇 Gold: Business aggregates, Star Schema, ML features
```

## 🔍 Delta Lake Features в проекте

| Фича | Применение |
|------|------------|
| `MERGE INTO` | Инкрементальное обновление Silver/Gold |
| `TIME TRAVEL` | Откат к предыдущей версии данных |
| `OPTIMIZE + ZORDER` | Ускорение запросов на 10-50x |
| `VACUUM` | Очистка старых версий файлов |
| `schemaEvolution` | Автоматическое добавление новых колонок |

## ️ Data Quality & Testing

- ✅ Валидация схем на каждом слое
- ✅ Дедупликация по бизнес-ключам
- ✅ Маскировка PII (email, phone, name)
- ✅ Great Expectations: `not_null`, `unique`, `accepted_values`
- ✅ Алёрты в Slack/Telegram при падении DQ
