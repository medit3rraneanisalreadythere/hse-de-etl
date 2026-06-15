# Практическая работа. Модуль 4 (экзамен)

## Кононенко Иван

---

## Задание 1: Работа с Yandex DataTransfer

Была успешно сгенерирована таблица по запросу в задании (450.000 строк, не менее 30 Мб) скриптом `./data_generation/generate_transactions_v2.py` 
![Скрипт](screenshots/05_task1_genscript.png)

и заполнена данными (`./generated_data/transactions_v2.csv`)

| call_id             | call_time            | client_id     | region_code | campaign_type   | call_status | client_response | duration_sec | follow_up_required |
|---------------------|----------------------|---------------|-------------|-----------------|-------------|-----------------|--------------|--------------------|
| call_202605_0000000 | 2026-05-10T21:54:31Z | client_691977 | DE-HB       | cash_loan_offer | missed      | no_response     | 288          | true               |
| call_202605_0000001 | 2026-05-31T01:55:08Z | client_832997 | DE-HH       | deposit_offer   | busy        | interested      | 882          | true               |
| call_202605_0000002 | 2026-05-14T04:24:10Z | client_553517 | DE-HE       | cash_loan_offer | busy        | no_response     | 186          | true               |

Также была успешно создана база данных Managed Service for YDB, а сгенерированные данные загружены в таблицу в базу данных с помощью скрипта `./upload_transactions_to_ydb.py`
![База данных](screenshots/01_task1_data.png)

Был также успешно создан пустой бакет Object Storage, затем были созданы эндпоинты источника из БД и приёмника из бакета
![Эндпоинты](screenshots/02_task1_endpoints.png)

Был успешно создан и активирован трансфер копирования с новыми эндпоинтами
![Трансфер](screenshots/03_task1_transfer.png)

Данные успешно появились в бакете в виде CSV файла
![Данные](screenshots/04_task1_data_in_bucket.png)

---

## Задание 2: Автоматизация работы с Yandex Data Processing при помощи Apache AirFlow

Для второго задания был подготовлен CSV-файл `applications.csv` с помощью скрипта `./data_generation/generate_applications.py`, содержащий данные о кредитных заявках. Объём файла составил более 50 Мб.
![Данные](screenshots/06_task2_genscript.png)

| application_id       | event_time          | customer_id | region_code | product_type | requested_amount | term_months | credit_score | risk_level | decision_status | approved_amount | channel     | employee_review_flag | processing_time_sec |
|----------------------|---------------------|-------------|-------------|--------------|------------------|-------------|--------------|------------|-----------------|-----------------|-------------|----------------------|---------------------|
| app_20260501_0000000 | 2026-05-15 00:06:15 | cust_263612 | DE-HH       | cash_loan    | 49206            | 36          | 493          | medium     | rejected        | 0               | mobile      | true                 | 45                  |
| app_20260501_0000001 | 2026-05-03 02:32:51 | cust_558506 | DE-NW       | mortgage     | 31967            | 48          | 717          | high       | manual_review   | 11540           | mobile      | false                | 159                 |
| app_20260501_0000002 | 2026-05-09 21:52:12 | cust_569264 | DE-HE       | credit_card  | 3407             | 60          | 535          | high       | rejected        | 0               | call_center | true                 | 136                 |

Файл был загружен в Object Storage по пути `s3a://etl-module4-kononenko/input/applications.csv`
![Данные2](screenshots/07_task2_data_in_bucket.png)

Для обработки данных было разработано PySpark-задание `pyspark/process_applications.py`. Задание выполняет чтение CSV-файла, преобразование типов и агрегацию данных по региону, продукту, статусу решения и уровню риска. Оно было загружено в бакет. Результат обработки сохраняется в Object Storage в директорию `s3a://etl-module4-kononenko/output/applications_agg`

Был создан кластер Managed Service for Apache AirFlow
![Кластер](screenshots/10_task2_airflow_cluster.png)

Для автоматизации процесса был подготовлен DAG Apache Airflow (`airflow/dataproc_airflow_dag.py`). DAG выполняет три последовательных шага:

1. создание временного кластера Yandex Data Processing;
2. запуск PySpark-задания;
3. удаление временного кластера после завершения обработки.

Такой подход позволяет не держать вычислительный кластер постоянно включённым и снижает расход облачных ресурсов.

В Airflow был запущен DAG, результат выполнения - успех.
![DAG](screenshots/08_task2_airflow_dags.png)

После выполнения данные появились в бакете:
![Результаты](screenshots/09_task2_results_in_bucket.png)


---

## Задание 3: Работа с топиками Apache Kafka с помощью PySpark-заданий в Yandex Data Processing

Для третьего задания был создан кластер Managed Service for Apache Kafka `kafka-etl-module4`. 
![Кластер](screenshots/15_task3_cluster.png)
В кластере был создан topic `loan_applications`.
![Топик](screenshots/16_task3_topic.png)
Также в кластере был создан пользователь `kafka_user`.
![Пользователь](screenshots/17_task3_user.png)

Для topic были сгенерированы скриптом `data_generation/generate_kafka_messages.py` JSON-сообщения, описывающие кредитные заявки. Каждое сообщение содержит вложенные структуры `customer`, `loan`, `scoring`, а также массив `documents`.
Общий объём отправленных сообщений составил более 20 МБ.
![Скрипт](screenshots/12_task3_genscript.png)

Сообщения были отправлены через kcat:
![Скрипт](screenshots/11_task3_kcat.png)


Для обработки сообщений было разработано PySpark-задание `kafka_flatten.py`. Задание читает сообщения из Kafka, преобразует поле `value` в строку, применяет JSON schema, разбирает вложенные поля и преобразует массив документов через `explode`.

В результате была получена плоская таблица со следующими полями:

- `application_id`;
- `customer_id`;
- `region_code`;
- `loan_amount`;
- `term_months`;
- `credit_score`;
- `risk_level`;
- `document_type`;
- `document_status`;
- `decision_status`;
- `submitted_at`.

Для запуска был создан DataProc кластер, в котором было запущено PySpark задание:
![Задание](screenshots/13_task3_jobdone.png)

Результат был сохранён в Object Storage.
![Задание](screenshots/14_task3_data.png)

---

## Задание 4: Визуализация в DataLens

С помощью Yandex DataLens построены дашборды для визуализации полученных данных
![Графики](screenshots/18_task4.png)