"""Database Ingestion Workflow

Based on: https://github.com/enroliv/adios/blob/main/dags/ingest_to_db_from_gcs.py

Dataproc example documentation - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html
Dataproc operators - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html#airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator

BigQuery example documentation - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#upsert-table
BigQuery operators - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/bigquery/index.html#airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator


Description: Ingests the data from a Google Drive to a GCS bucket and a postgres table then 
"""

import os
import gdown

from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
# from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator 

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# RAW to STAGE
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator

# BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator, BigQueryDeleteDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


# General constants
DAG_ID = "wizeline_capstone_end_to_end_v3"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"
LOCAL_DATA_PATH = "/usr/local/airflow/dags/files/"
LOCAL_PYSPARK_PATH = "/usr/local/airflow/dags/files/pyspark/"
file_urls = Variable.get("capstone_files_urls_public", deserialize_json=True)

# GCP constants
GCP_CONN_ID = "gcp_default"
GCP_PROJECT_ID = 'wizeline-deb-capstone'
GCS_BUCKET_NAME = "wizeline_bootcamp_bucket"
GCS_LOG_FILE_PATH = 'RAW/log_reviews.csv'
GCS_MOVIE_FILE_PATH = 'RAW/movie_reviews.csv'

# Postgres constants
POSTGRES_CONN_ID = "capstone_postgres"
POSTGRES_TABLE_NAME = "user_purchase"

# GDRIVE
GDRIVE_DATA_FOLDER = "1Ob14UnJL4EIoZQQlLxUFj5nUkJ4cegFA" # "1H0-oGRRtlcDWmIsreeV5c8pxwuFXeXtj"

# Dataproc constants
CLUSTER_NAME = "wizeline-deb-dataproc"
pyspark_file_urls = Variable.get("capstone_pyspark_files_urls_public", deserialize_json=True)


# URIs examples
# Local files = "file:///usr/lib/spark/examples/jars/spark-examples.jar"
# GCS files = f"gs://{BUCKET_NAME}/{INIT_FILE}"
MOVIE_PYSPARK_FILE_URI = f"gs://{GCS_BUCKET_NAME}/SPARK_JOB/movie_review_positive_sentiment.py"
LOG_PYSPARK_FILE_URI = f"gs://{GCS_BUCKET_NAME}/SPARK_JOB/log_review_processing.py"
REGION = "us-central1"
gcs_csv_files = {
    'log_reviews_transformed' :'STAGE/log_reviews_transformed.csv',
    'classified_movie_reviews' : 'STAGE/classified_movie_reviews.csv',
    'user_purchase' : 'STAGE/user_purchase.csv'
}

# Bigquery constants
DATASET_NAME = "capstone_dataset"

table_schemas = {
    'dim_date': [
        {'name': 'id_dim_date', 'type': 'INTEGER'},
        {'name': 'log_date', 'type': 'DATE'},
        {'name': 'day', 'type': 'STRING'},
        {'name': 'month', 'type': 'STRING'},
        {'name': 'year', 'type': 'STRING'},
        {'name': 'season', 'type': 'STRING'},
    ],
    'dim_devices': [
        {'name': 'id_dim_devices', 'type': 'INTEGER'},
        {'name': 'device', 'type': 'STRING'},
    ],
    'dim_location': [
        {'name': 'id_dim_location', 'type': 'INTEGER'},
        {'name': 'location', 'type': 'STRING'},
    ],
    'dim_os': [
        {'name': 'id_dim_os', 'type': 'INTEGER'},
        {'name': 'os', 'type': 'STRING'},
    ],
    'dim_browser': [
        {'name': 'id_dim_browser', 'type': 'INTEGER'},
        {'name': 'browser', 'type': 'STRING'},
    ],
    'fact_movie_analytics': [
        {'name': 'customerid', 'type': 'INTEGER'}, # user_purchase.CustomerID
        {'name': 'id_dim_date', 'type': 'INTEGER'},
        {'name': 'id_dim_devices', 'type': 'INTEGER'}, # dim_devices.id_dim_devices
        {'name': 'id_dim_location', 'type': 'INTEGER'}, # dim_location.id_dim_location
        {'name': 'id_dim_os', 'type': 'INTEGER'}, # dim_os.id_dim_os
        {'name': 'amount_spent', 'type': 'NUMERIC', 'mode': 'NULLABLE', 'precision': 18, 'scale': 5}, # SUM(user_purchase.quantity * user_purchase.unit_price) group by user_purchase.customer_id
        {'name': 'review_score', 'type': 'INTEGER'}, # SUM(movie_reviews.positive_review) group by user_purchase.customer_id
        {'name': 'review_count', 'type': 'INTEGER'}, # COUNT(movie_reviews.review_id) group by user_purchase.customer_id
        {'name': 'insert_date', 'type': 'DATE'}
    ],
}

# SQL Queries
table_insert_queries = {
    'dim_date': f"""
            INSERT INTO `{DATASET_NAME}.dim_date` (id_dim_date, log_date, day, month, year, season)
            SELECT
                ROW_NUMBER() OVER () AS id_dim_date,
                log_date,
                FORMAT_DATE('%d', log_date) AS day,
                FORMAT_DATE('%b', log_date) AS month,
                FORMAT_DATE('%Y', log_date) AS year,
                CASE
                    WHEN EXTRACT(MONTH FROM log_date) IN (12, 1, 2) THEN 'Winter'
                    WHEN EXTRACT(MONTH FROM log_date) IN (3, 4, 5) THEN 'Spring'
                    WHEN EXTRACT(MONTH FROM log_date) IN (6, 7, 8) THEN 'Summer'
                    ELSE 'Fall'
                END AS season
            FROM (
                SELECT DISTINCT log_date FROM `{DATASET_NAME}.log_reviews_transformed`
                ORDER BY log_date
            ) unique_dates
        """,
    'dim_devices': f"""
            INSERT INTO `{DATASET_NAME}.dim_devices` (id_dim_devices, device)
            SELECT 
                ROW_NUMBER() OVER () AS id_dim_devices,
                device
            FROM (
                SELECT DISTINCT device 
                FROM `{DATASET_NAME}.log_reviews_transformed`
                ORDER BY device
            ) unique_devices
        """,
    'dim_location': f"""
            INSERT INTO `{DATASET_NAME}.dim_location` (id_dim_location, location)
            SELECT 
                ROW_NUMBER() OVER () AS id_dim_location,
                location
            FROM (
                SELECT DISTINCT location 
                FROM `{DATASET_NAME}.log_reviews_transformed`
                ORDER BY location
            ) unique_locations
        """,
    'dim_os': f"""
            INSERT INTO `{DATASET_NAME}.dim_os` (id_dim_os, os)
            SELECT 
                ROW_NUMBER() OVER () AS id_dim_os,
                os
            FROM (
                SELECT DISTINCT os 
                FROM `{DATASET_NAME}.log_reviews_transformed`
                ORDER BY os
            ) unique_os
        """,
    'fact_movie_analytics': f"""
            INSERT INTO `{GCP_PROJECT_ID}.{DATASET_NAME}.fact_movie_analytics` (
                customerid,
                id_dim_date, 
                id_dim_devices, 
                id_dim_location, 	
                id_dim_os, 
                amount_spent, 
                review_score, 
                review_count, 
                insert_date
            )
            WITH 
                mr as (
                    SELECT user_id, SUM(positive_review) review_score, COUNT(review_id) review_count
                    FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.classified_movie_reviews`
                    GROUP BY user_id
                ),
                up AS (
                    SELECT customer_id, SUM(quantity * unit_price) amount_spent
                    FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.user_purchase` 
                    GROUP BY customer_id
                )
            SELECT 
                up.customer_id AS customerid,
                (SELECT ddt.id_dim_date FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_date ddt WHERE ddt.log_date = lr.log_date) id_dim_date,
                (SELECT dd.id_dim_devices FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_devices dd WHERE dd.device = lr.device) id_dim_devices,
                (SELECT dl.id_dim_location FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_location dl WHERE dl.location = lr.location) id_dim_location,
                (SELECT dos.id_dim_os FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_os dos WHERE dos.os = lr.os) id_dim_os,
                CAST(up.amount_spent AS NUMERIC) amount_spent,
                mr.review_score,
                mr.review_count,
                CURRENT_DATE() AS insert_date
            FROM up
            INNER JOIN mr ON up.customer_id = mr.user_id
            INNER JOIN {GCP_PROJECT_ID}.{DATASET_NAME}.classified_movie_reviews mr2 ON up.customer_id = mr2.user_id
            INNER JOIN {GCP_PROJECT_ID}.{DATASET_NAME}.log_reviews_transformed lr ON mr2.review_id = lr.log_id
        """,
}

# Dataproc Configs
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}

MOVIE_PYSPARK_JOB = {
    "reference": {
        "project_id": GCP_PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": MOVIE_PYSPARK_FILE_URI
    }
}

LOG_PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": LOG_PYSPARK_FILE_URI,
        "python_file_uris": [LOG_PYSPARK_FILE_URI],
        },
}


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


# Get .py files and store them locally in the airflow server
def get_pyspark_file(urls=pyspark_file_urls, path=LOCAL_PYSPARK_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    for key, url in urls.items():
        # Extract the file ID from the Google Drive URL
        file_id = url.split("/")[-2]
        
        download_url = f"https://drive.google.com/uc?id={file_id}"
        local_file_name = f"{key}.py"
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

    # Branch Options:
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

    raw_ready = DummyOperator(task_id="raw_ready", trigger_rule=TriggerRule.ALL_SUCCESS)

    get_pyspark_files = PythonOperator(
        task_id="get_pyspark_files",
        python_callable=get_pyspark_file,
        op_kwargs={
            "urls": pyspark_file_urls,
            "path": LOCAL_PYSPARK_PATH,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    upload_movie_pyspark_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_movie_pyspark_to_gcs",
        src=f"{LOCAL_PYSPARK_PATH}movie_reviews.py",  
        dst="SPARK_JOB/movie_review_positive_sentiment.py", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )

    upload_log_pyspark_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_log_pyspark_to_gcs",
        src=f"{LOCAL_PYSPARK_PATH}log_reviews.py",  
        dst="SPARK_JOB/log_review_processing.py", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        delete_on_error=True, 
        use_if_exists=True,
        gcp_conn_id=GCP_CONN_ID
    )

    movie_pyspark_task = DataprocSubmitJobOperator(
        task_id="movie_pyspark_task", 
        job=MOVIE_PYSPARK_JOB, 
        region=REGION, 
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    log_pyspark_task = DataprocSubmitJobOperator(
        task_id="log_pyspark_task", 
        job=LOG_PYSPARK_JOB, 
        region=REGION, 
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    extract_user_purchase_data_postgres = PostgresToGCSOperator(
        task_id='extract_user_purchase_data_postgres',
        postgres_conn_id=POSTGRES_CONN_ID, 
        gcp_conn_id=GCP_CONN_ID,
        sql=f'SELECT * FROM {POSTGRES_TABLE_NAME}', 
        bucket=GCS_BUCKET_NAME,  
        filename='STAGE/user_purchase.csv', 
        export_format='CSV',
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID,      
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Use the GoogleCloudStorageListOperator to list files with a specific prefix
    list_movie_files = GCSListObjectsOperator(
        task_id='list_movie_files',
        bucket=GCS_BUCKET_NAME,
        prefix='STAGE/classified_movie_reviews.csv/part-',  # Prefix to match files
        delimiter='.csv',
        gcp_conn_id=GCP_CONN_ID,  # Your GCP connection ID
    )

    # Use the GoogleCloudStorageToGoogleCloudStorageOperator to move and rename the file
    rename_movie_csv = GCSToGCSOperator(
        task_id='rename_movie_csv',
        source_bucket=GCS_BUCKET_NAME,
        source_object="{{ task_instance.xcom_pull(task_ids='list_movie_files')[0] }}",  # Get the first file from the list
        destination_bucket=GCS_BUCKET_NAME,
        destination_object='STAGE/classified_movie_reviews.csv',
        replace=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    list_log_files = GCSListObjectsOperator(
        task_id='list_log_files',
        bucket=GCS_BUCKET_NAME,
        prefix='STAGE/log_reviews_transformed.csv/part-',  # Prefix to match files
        delimiter='.csv',
        gcp_conn_id=GCP_CONN_ID,  # Your GCP connection ID
    )

    rename_log_csv = GCSToGCSOperator(
        task_id='rename_log_csv',
        source_bucket=GCS_BUCKET_NAME,
        source_object="{{ task_instance.xcom_pull(task_ids='list_log_files')[0] }}",  # Get the first file from the list
        destination_bucket=GCS_BUCKET_NAME,
        destination_object='STAGE/log_reviews_transformed.csv',
        replace=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    stage_ready = DummyOperator(task_id="stage_ready", trigger_rule=TriggerRule.ALL_SUCCESS)

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", 
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=DATASET_NAME,
        location='US',
        if_exists="ignore"
    )

    upload_gcs_data_tasks = []
    for table_name, gcs_csv_file in gcs_csv_files.items():
        task_id = f'upload_{table_name}_csv'  # Unique task ID for each file
        table_id = f'{table_name}'  # Unique target table name

        upload_gcs_data = GCSToBigQueryOperator(
            task_id=task_id,
            bucket=GCS_BUCKET_NAME,
            source_objects=[gcs_csv_file],
            destination_project_dataset_table=f'{GCP_PROJECT_ID}.{DATASET_NAME}.{table_id}',
            source_format = "CSV",
            external_table = False,
            # create_disposition = "CREATE_IF_NEEDED",  # You can change this depending on your requirements
            write_disposition='WRITE_TRUNCATE',
            skip_leading_rows=1,
            field_delimiter=',',  # Modify this if your CSV files use a different delimiter
            gcp_conn_id=GCP_CONN_ID
        ) 
        upload_gcs_data_tasks.append(upload_gcs_data)

    # parallel_upload_gcs_data = BitShift(task_id="parallel_upload_gcs_data", tasks=upload_gcs_data_tasks)
        
    gcs_data_uploaded = DummyOperator(task_id="gcs_data_uploaded", trigger_rule=TriggerRule.ALL_SUCCESS)

    create_bq_table_tasks = []
    for table_id, columns in table_schemas.items():
        create_bq_table = BigQueryCreateEmptyTableOperator(
            task_id=f'create_{table_id}_table',
            table_id=f'{table_id}',
            schema_fields=columns,
            gcp_conn_id=GCP_CONN_ID,
            dataset_id=DATASET_NAME,
            project_id=GCP_PROJECT_ID,      
            location='US',
        )
        create_bq_table_tasks.append(create_bq_table)
        
    bq_tables_created = DummyOperator(task_id="bq_tables_created", trigger_rule=TriggerRule.ALL_SUCCESS)

    # Create a BigQueryInsertJobOperator to execute the insert query
    insert_dim_data_tasks=[]
    for table_id, insert_query in table_insert_queries.items():
        if table_id != 'fact_movie_analytics':
            insert_dim_data = BigQueryInsertJobOperator(
                task_id=f'insert_{table_id}_data',
                configuration={
                    'query': {      # Which is the best configuration option: "query" or "load"
                        'query': insert_query,
                        'useLegacySql': False,  # Use standard SQL
                    }
                },
                project_id=GCP_PROJECT_ID,
                location='US',  # Set the appropriate location
                gcp_conn_id=GCP_CONN_ID,
            )
            insert_dim_data_tasks.append(insert_dim_data)

    dim_tables_populated = DummyOperator(task_id="dim_tables_populated", trigger_rule=TriggerRule.ALL_SUCCESS)
    
    insert_fact_data = BigQueryInsertJobOperator(
        task_id=f'insert_fact_data',
        configuration={
            'query': {      # Which is the best configuration option: "query" or "load"
                'query': table_insert_queries['fact_movie_analytics'],
                'useLegacySql': False,  # Use standard SQL
            }
        },
        project_id=GCP_PROJECT_ID,
        location='US',  # Set the appropriate location
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end_workflow = DummyOperator(task_id="end_workflow", trigger_rule=TriggerRule.ONE_SUCCESS)


    # Task Flow 1: Ingestion
    (
        start_workflow
        >> get_data
        >> [ upload_log_reviews_to_gcs, upload_movie_reviews_to_gcs, create_user_purchase_table ] 
    )
    upload_log_reviews_to_gcs >> check_log_file_sensor
    upload_movie_reviews_to_gcs >> check_movie_file_sensor
    create_user_purchase_table >> validate_data >> [clear_table, continue_process] >> ingest_data
    [check_log_file_sensor, check_movie_file_sensor, ingest_data] >> raw_ready


    # Task Flow 2: Transformation
    raw_ready >> get_pyspark_files >> [upload_movie_pyspark_to_gcs, upload_log_pyspark_to_gcs] >> create_cluster
    create_cluster >> [ movie_pyspark_task, log_pyspark_task, extract_user_purchase_data_postgres ] >> delete_cluster
    delete_cluster >> [list_movie_files, list_log_files] 
    list_movie_files >> rename_movie_csv 
    list_log_files >> rename_log_csv
    [rename_movie_csv, rename_log_csv] >> stage_ready


    # Task Flow 3: Data Warehousing
    stage_ready >> create_dataset >> upload_gcs_data_tasks >> gcs_data_uploaded >> create_bq_table_tasks >> bq_tables_created >> insert_dim_data_tasks >> dim_tables_populated >> insert_fact_data >> end_workflow


    dag.doc_md = __doc__