# ETL Project Module 3
**Кононенко Иван**

## Описание проекта
Полный цикл ETL-процесса:
1. Генерация данных в MongoDB
2. Репликация и трансформация данных в PostgreSQL (Apache Airflow)
3. Построение аналитических витрин

## Как запустить

   ```bash
   docker compose build --no-cache
   docker compose up -d

   source venv/bin/activate
   python scripts/generate_data.py
   ```

   И выполнить SQL запросы из init_tables.sql