
# Dataproc example documentation - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html
# Dataproc operators - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html#airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator

import os
import gdown


from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
# from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago


# General constants
DAG_ID = "dataproc_raw_to_stage_v2"
LOCAL_DATA_PATH = "/usr/local/airflow/dags/files/pyspark/"

# GCP constants
GCP_CONN_ID = "gcp_default"
GCS_BUCKET_NAME = "wizeline_bootcamp_bucket"

# Files
GCS_LOG_FILE_PATH = 'RAW/log_reviews.csv'
GCS_MOVIE_FILE_PATH = 'RAW/movie_reviews.csv'

# Dataproc constants
PROJECT_ID = "wizeline-deb-capstone"
CLUSTER_NAME = "wizeline-deb-dataproc"
pyspark_file_urls = Variable.get("capstone_pyspark_files_urls_public", deserialize_json=True)


# URIs examples
# Local files = "file:///usr/lib/spark/examples/jars/spark-examples.jar"
# GCS files = f"gs://{BUCKET_NAME}/{INIT_FILE}"
MOVIE_PYSPARK_FILE_URI = f"gs://{GCS_BUCKET_NAME}/SPARK_JOB/movie_review_positive_sentiment.py"
LOG_PYSPARK_FILE_URI = f"gs://{GCS_BUCKET_NAME}/SPARK_JOB/log_review_processing.py"
REGION = "us-central1"

# Postgres constants
POSTGRES_CONN_ID = "capstone_postgres"
POSTGRES_TABLE_NAME = "user_purchase"

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
        "project_id": PROJECT_ID
    },
    "placement": {
        "cluster_name": CLUSTER_NAME
    },
    "pyspark_job": {
        "main_python_file_uri": MOVIE_PYSPARK_FILE_URI
    }
}

LOG_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": LOG_PYSPARK_FILE_URI,
        "python_file_uris": [LOG_PYSPARK_FILE_URI],
        },
}

# Get .py files and store them locally in the airflow server
def get_pyspark_files(urls=pyspark_file_urls, path=LOCAL_DATA_PATH) -> None:
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


def combine_gcs_files(GCS_BUCKET=GCS_BUCKET_NAME, GCS_PREFIX = None, OUTPUT_PATH = None):
    from google.cloud import storage
    # Create a GCS client using the specified connection ID
    client = storage.Client(project=GCP_CONN_ID.split('.')[0])

    # Get the GCS bucket and blob (output file)
    bucket = client.get_bucket(GCS_BUCKET)
    blob = bucket.blob(OUTPUT_PATH)

    # List all the GCS objects in the specified folder prefix
    objects = list(bucket.list_blobs(prefix=GCS_PREFIX))

    # Initialize a flag to track whether the header has been written
    header_written = False

    # Combine the content of the part files into a single output file with a single header
    combined_content = b''
    for obj in objects:
        part_content = obj.download_as_text()

        # If the header has not been written yet, write it
        if not header_written:
            combined_content += part_content.split('\n', 1)[0] + '\n'  # Extract the first line (header)
            header_written = True

        # Append the rest of the content
        combined_content += part_content.split('\n', 1)[1]

    # Upload the combined content to the output file in GCS
    blob.upload_from_string(combined_content)

    return OUTPUT_PATH


with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=["gcp", "dataproc"],
) as dag:
    
    start_workflow = DummyOperator(task_id="start_workflow")

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=get_pyspark_files,
        op_kwargs={
            "urls": pyspark_file_urls,
            "path": LOCAL_DATA_PATH,
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    upload_movie_pyspark_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_movie_pyspark_to_gcs",
        src=f"{LOCAL_DATA_PATH}movie_reviews.py",  
        dst="SPARK_JOB/movie_review_positive_sentiment.py", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )

    upload_log_pyspark_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_log_pyspark_to_gcs",
        src=f"{LOCAL_DATA_PATH}log_reviews.py",  
        dst="SPARK_JOB/log_review_processing.py", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
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
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    log_pyspark_task = DataprocSubmitJobOperator(
        task_id="log_pyspark_task", 
        job=LOG_PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID,
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
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id=GCP_CONN_ID,      
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # for key, value in part_files.items():
    #     combines_part_files = PythonOperator(
    #         task_id="combines_part_files",
    #         python_callable=get_pyspark_files,
    #         op_kwargs={
    #             "GCS_PREFIX": f'STAGE/{key}',
    #             "OUTPUT_PATH": LOCAL_DATA_PATH,
    #         },
    #         trigger_rule=TriggerRule.ONE_SUCCESS,
    #     )

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

    end_workflow = DummyOperator(task_id="end_workflow", trigger_rule=TriggerRule.ONE_SUCCESS)

    start_workflow >> get_files >> [upload_movie_pyspark_to_gcs, upload_log_pyspark_to_gcs] >> create_cluster
    create_cluster >> [ movie_pyspark_task, log_pyspark_task, extract_user_purchase_data_postgres ] >> delete_cluster
    delete_cluster >> list_movie_files >> rename_movie_csv >> end_workflow


    # (
    #     start_workflow
    #     # >> get_files
    #     >> [upload_movie_pyspark_to_gcs, upload_log_pyspark_to_gcs]
    #     >> [check_gcs_uri_task, check_gdrive_uri_task]
    #     # >> create_cluster
    #     # >> [ movie_pyspark_task, log_pyspark_task ]  
    #     # >> delete_cluster
    #     >> end_workflow
    # )
