"""Database Ingestion Workflow

Based on: https://github.com/enroliv/adios/blob/main/dags/ingest_to_db_from_gcs.py

Description: Ingests the data from a GCS bucket into a postgres table.
"""

import os
import logging
import requests
import tempfile
from datetime import datetime as dt
from pathlib import Path
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


# General constants
DAG_ID = "wizeline_capstone_end_to_end"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"
LOCAL_DATA_PATH = "/usr/local/airflow/dags/files/"

# GCP constants
GCP_CONN_ID = "gcp_default"
GCS_BUCKET_NAME = "wizeline_bootcamp_bucket"
GCS_PGS_KEY_NAME = "user_purchase.csv"

# Postgres constants
POSTGRES_CONN_ID = "capstone_postgres"
POSTGRES_TABLE_NAME = "capstone.user_purchase"

file_urls = {
    "user_purchase": "https://drive.google.com/file/d/1p8Q_QoCqdxnkKcpPLp15FDTFjSdTE_zk/view?usp=drive_web&authuser=0",
    "log_reviews": "https://drive.google.com/file/d/1MfTRyyiMMGLuZ-hOysmjiATCUEHkQoE6/view?usp=drive_web&authuser=0",
    "movie_reviews": "https://drive.google.com/file/d/1q-9kHMPHzyJhx-GpP93BqaLbW-pFRfGX/view?usp=drive_web&authuser=0"
}

def download_data(urls=file_urls, path=LOCAL_DATA_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    for f_name, url in urls.items():  
        response = requests.request("GET", url)

        file_path = f"{path}{f_name}.csv"

        with open(file_path, "w") as local_file:
            local_file.write(response.text)



def ingest_data_to_postgres(
    path: str,
    file: str,
    postgres_table: str = POSTGRES_TABLE_NAME,
    postgres_conn_id: str = POSTGRES_CONN_ID,
):
    """Ingest data from an local storage into a postgres table.

    Args:
        path (str): Path in local file storage.
        file (str): Name of the file to be ingested.
        postgres_table (str): Name of the postgres table.
        postgres_conn_id (str): Name of the postgres connection ID.
    """  
    # gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(f"{path}{file}.csv", "r") as local_user_purchase_file:
        cur.copy_expert(
            f"COPY {postgres_table} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
            local_user_purchase_file,
        )
    conn.commit()



with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, STABILITY_STATE],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=download_data,
        op_kwargs={
            "urls": file_urls,
            "path": LOCAL_DATA_PATH,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    upload_movie_reviews_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_movie_reviews_to_gcs",
        src=f"{LOCAL_DATA_PATH}movie_reviews.csv",  
        dst="RAW/movie_reviews.csv", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )

    upload_log_reviews_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_log_reviews_to_gcs",
        src=f"{LOCAL_DATA_PATH}log_reviews.csv",  
        dst="RAW/log_reviews.csv", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )


    # verify_key_existence = GCSObjectExistenceSensor(
    #     task_id="verify_key_existence",
    #     google_cloud_conn_id=GCP_CONN_ID,
    #     bucket=GCS_BUCKET_NAME,
    #     object=GCS_PGS_KEY_NAME,
    # )

    create_user_purchase_table = PostgresOperator(
        task_id="create_user_purchase_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_NAME} (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            );""",
    )

    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"DELETE FROM {POSTGRES_TABLE_NAME}",
    )
    continue_process = DummyOperator(task_id="continue_process")

    validate_data = BranchSQLOperator(
        task_id="validate_data",
        conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM {POSTGRES_TABLE_NAME}",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[clear_table.task_id],
    )

    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_to_postgres,
        op_kwargs={
            "postgres_conn_id": POSTGRES_CONN_ID,
            "path": GCS_BUCKET_NAME,
            "file": GCS_PGS_KEY_NAME,
            "postgres_table": POSTGRES_TABLE_NAME,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    (
        start_workflow
        >> get_data
        # >> verify_key_existence
        >> [upload_log_reviews_to_gcs, upload_movie_reviews_to_gcs]
        >> create_user_purchase_table
        >> validate_data
    )
    validate_data >> [clear_table, continue_process] >> ingest_data
    ingest_data >> end_workflow

    dag.doc_md = __doc__
