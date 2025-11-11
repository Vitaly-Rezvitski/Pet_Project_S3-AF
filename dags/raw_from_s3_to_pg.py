import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "vitaly"
DAG_ID = "raw_from_s3_to_pg"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "NBRB_currency"
SCHEMA = "ods"
TARGET_TABLE = "currency"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# raw_from_s3_to_pg
"""

SHORT_DESCRIPTION = "raw_from_s3_to_pg"

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


def get_and_transfer_raw_data_to_ods_pg(**context):
    """"""

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            Cur_ID,
            Cur_ParentID,
            Cur_Code,
            Cur_Abbreviation,
            Cur_Name,
            Cur_Name_Bel,
            Cur_Name_Eng,
            Cur_QuotName,
            Cur_QuotName_Bel,
            Cur_QuotName_Eng,
            Cur_NameMulti,
            Cur_Name_BelMulti,
            Cur_Name_EngMulti,
            Cur_Scale,
            Cur_Periodicity,
            Cur_DateStart,
            Cur_DateEnd
        )
        SELECT
            Cur_ID,
            Cur_ParentID,
            Cur_Code,
            Cur_Abbreviation,
            Cur_Name,
            Cur_Name_Bel,
            Cur_Name_Eng,
            Cur_QuotName,
            Cur_QuotName_Bel,
            Cur_QuotName_Eng,
            Cur_NameMulti,
            Cur_Name_BelMulti,
            Cur_Name_EngMulti,
            Cur_Scale,
            Cur_Periodicity,
            Cur_DateStart,
            Cur_DateEnd
        FROM 's3://pet-project/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end
