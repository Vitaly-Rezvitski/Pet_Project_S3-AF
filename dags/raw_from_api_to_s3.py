import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import requests
import json
import tempfile
import os

# Конфигурация DAG
OWNER = "vitaly"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "NBRB_currency"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# raw_from_api_to_s3
"""

SHORT_DESCRIPTION = "raw_from_api_to_s3"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 11, 11, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    # URL API
    api_url = 'https://api.nbrb.by/exrates/currencies'

    try:
        # Запрос к API
        response = requests.get(api_url)
        response.raise_for_status()

        # Проверка, что ответ в формате JSON
        if response.headers.get('content-type', '').startswith('application/json'):
            data = response.json()
        else:
            raise ValueError("API response is not in JSON format")

        # Проверка, что данные не пустые
        if not data:
            raise ValueError("API returned empty data")

        logging.info(f"✅ API data received successfully: {len(data) if isinstance(data, list) else 'object'}")

        # Сохранение данных во временный JSON-файл
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8') as temp_file:
            json.dump(data, temp_file)
            temp_file_path = temp_file.name

        try:
            con = duckdb.connect()

            # Чтение данных из временного файла и сохранение на S3
            con.sql(f"""
                SET TIMEZONE='UTC';
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_url_style = 'path';
                SET s3_endpoint = 'minio:9000';
                SET s3_access_key_id = '{ACCESS_KEY}';
                SET s3_secret_access_key = '{SECRET_KEY}';
                SET s3_use_ssl = FALSE;

                -- Чтение JSON из временного файла
                COPY (
                    SELECT * FROM read_json_auto('{temp_file_path}')
                ) TO 's3://pet-project/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
            """)

            con.close()
            logging.info(f"✅ Data saved to S3 successfully: {start_date}")

        finally:
            # Удаление временного файла
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error during API request: {e}")
        raise
    except ValueError as e:
        logging.error(f"❌ Error with API response: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}")
        raise


with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end
