"""Database Ingestion Workflow

Based on: https://github.com/enroliv/adios/blob/main/dags/ingest_to_db_from_gcs.py

Description: Ingests the data from a GCS bucket into a postgres table.
"""

import os
import gdown
import requests
from datetime import datetime as dt
from pathlib import Path
from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator 

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


# General constants
DAG_ID = "wizeline_capstone_end_to_end_v2"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"
LOCAL_DATA_PATH = "/usr/local/airflow/dags/files/"

# GCP constants
GCP_CONN_ID = "gcp_default"
GCS_BUCKET_NAME = "wizeline_bootcamp_bucket"
GCS_LOG_FILE_PATH = 'RAW/log_reviews.csv'
GCS_MOVIE_FILE_PATH = 'RAW/movie_reviews.csv'

# Postgres constants
POSTGRES_CONN_ID = "capstone_postgres"
POSTGRES_TABLE_NAME = "user_purchase"

# GDRIVE
GDRIVE_DATA_FOLDER = "1Ob14UnJL4EIoZQQlLxUFj5nUkJ4cegFA" # "1H0-oGRRtlcDWmIsreeV5c8pxwuFXeXtj"

file_urls = Variable.get("capstone_files_urls_public", deserialize_json=True)

def download_data(urls=file_urls, path=LOCAL_DATA_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    for key, url in urls.items():
        # Extract the file ID from the Google Drive URL
        file_id = url.split("/")[-2]
        
        download_url = f"https://drive.google.com/uc?id={file_id}"
        local_file_name = f"{key}.csv"
        local_file_path = os.path.join(path, local_file_name)
        
        # Download the file and save it locally
        gdown.download(download_url, local_file_path, quiet=False)
        print(f"Downloaded {local_file_name} to {local_file_path}")


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

    # upload_movie_reviews_to_gcs = LocalFilesystemToGCSOperator(
    #     task_id="upload_movie_reviews_to_gcs",
    #     src=f"{LOCAL_DATA_PATH}movie_reviews.csv",  
    #     dst="RAW/movie_reviews.csv", 
    #     bucket=GCS_BUCKET_NAME,
    #     gcp_conn_id=GCP_CONN_ID, 
    # )

    upload_movie_reviews_to_gcs = GoogleDriveToGCSOperator (
        task_id="upload_movie_reviews_to_gcs",
        bucket_name=GCS_BUCKET_NAME,
        object_name=GCS_MOVIE_FILE_PATH,
        file_name="movie_reviews.csv",
        folder_id=GDRIVE_DATA_FOLDER,
        gcp_conn_id=GCP_CONN_ID,
    )

    upload_log_reviews_to_gcs = GoogleDriveToGCSOperator (
        task_id="upload_log_reviews_to_gcs",
        bucket_name=GCS_BUCKET_NAME,
        object_name=GCS_LOG_FILE_PATH,
        file_name="log_reviews.csv",
        folder_id=GDRIVE_DATA_FOLDER,
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # upload_log_reviews_to_gcs = LocalFilesystemToGCSOperator(
    #     task_id="upload_log_reviews_to_gcs",
    #     src=f"{LOCAL_DATA_PATH}log_reviews.csv",  
    #     dst="RAW/log_reviews.csv", 
    #     bucket=GCS_BUCKET_NAME,
    #     gcp_conn_id=GCP_CONN_ID, 
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
            "path": LOCAL_DATA_PATH,
            "file": "user_purchase",
            "postgres_table": POSTGRES_TABLE_NAME,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_log_file_sensor = GoogleCloudStorageObjectSensor(
        task_id='check_log_file',
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=GCS_LOG_FILE_PATH,
        mode='poke',  # Use 'poke' mode to continuously check until the file exists
        timeout=60 * 2.5,  # Timeout in seconds (adjust as needed)
        poke_interval=30,  # Polling interval in seconds (adjust as needed)
    )

    check_movie_file_sensor = GoogleCloudStorageObjectSensor(
        task_id='check_movie_file',
        bucket=GCS_BUCKET_NAME,
        object=GCS_MOVIE_FILE_PATH,
        google_cloud_conn_id=GCP_CONN_ID,
        mode='poke',  # Use 'poke' mode to continuously check until the file exists
        timeout=60 * 2.5,  # Timeout in seconds (adjust as needed)
        poke_interval=30,  # Polling interval in seconds (adjust as needed)
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    (
        start_workflow
        >> get_data
        >> [ upload_log_reviews_to_gcs, upload_movie_reviews_to_gcs, create_user_purchase_table ] 
    )
    upload_log_reviews_to_gcs >> check_log_file_sensor
    upload_movie_reviews_to_gcs >> check_movie_file_sensor
    create_user_purchase_table >> validate_data >> [clear_table, continue_process] >> ingest_data
    [check_log_file_sensor, check_movie_file_sensor, ingest_data] >> end_workflow    
    

    dag.doc_md = __doc__
