
# Dataproc example documentation - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html
# Dataproc operators - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html#airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator

import os
import gdown

from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.transfers.dataproc_to_gcs import DataprocJobToGCSOperator
# from airflow.providers.google.cloud.transfers. import 


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

# URIs
# Local files = "file:///usr/lib/spark/examples/jars/spark-examples.jar"
# GCS files = f"gs://{BUCKET_NAME}/{INIT_FILE}"
MOVIE_PYSPARK_FILE_URI = f"gs://{GCS_BUCKET_NAME}/SPARK_JOB/movie_review_positive_sentiment.py"
LOG_PYSPARK_FILE_URI = f"gs://{GCS_BUCKET_NAME}/SPARK_JOB/log_review_processing.py"
REGION = "us-central1"
# ZONE = "us-central1-b"

# Configs
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
    # "secondary_worker_config": {
    #     "num_instances": 1,
    #     "machine_type_uri": "n1-standard-4",
    #     "disk_config": {
    #         "boot_disk_type": "pd-standard",
    #         "boot_disk_size_gb": 32,
    #     },
    #     "is_preemptible": True,
    #     "preemptibility": "PREEMPTIBLE",
    # },
}

# # Configuration for a PySpark Job:
# MOVIE_PYSPARK_JOB = {
#     "reference": {"project_id": PROJECT_ID},
#     "placement": {"cluster_name": CLUSTER_NAME},
#     "pysparkJob": {
#         "main_python_file_uri": MOVIE_PYSPARK_FILE_URI,
#         "python_file_uris": [MOVIE_PYSPARK_FILE_URI],
#         },
# }

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

# Spark Job Template - would replace the job parameter in the above pyspark job configs if you require more config
spark_job = {
    "mainPythonFileUri": "gs://your-bucket/your-main.py",  # HCFS URI of the main Python file
    "args": ["arg1", "arg2"],  # Optional arguments to pass to the driver
    "pythonFileUris": ["gs://your-bucket/your-python-file.py"],  # Optional Python file URIs
    "jarFileUris": ["gs://your-bucket/your-jar-file.jar"],  # Optional jar file URIs
    "fileUris": ["gs://your-bucket/your-file.txt"],  # Optional file URIs
    "archiveUris": ["gs://your-bucket/your-archive.zip"],  # Optional archive URIs
    "properties": {
        "spark.some.property": "value",  # Optional Spark properties
        # Add any additional Spark properties here
    },
    "loggingConfig": {
        # Optional runtime log config
        # Example: {"driverLogLevels": {"root": "INFO", "org": "DEBUG"}}
    }
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
        src=f"{LOCAL_DATA_PATH}movie_review_positive_sentiment.py",  
        dst="SPARK_JOB/movie_review_positive_sentiment.py", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )

    upload_log_pyspark_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_log_pyspark_to_gcs",
        src=f"{LOCAL_DATA_PATH}log_review_processing.py",  
        dst="SPARK_JOB/log_review_processing.py", 
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID, 
    )

    checks_start = DummyOperator(task_id="checks_start")

    check_gcs_uri_task = HttpSensor(
        task_id="check_gcs_uri_task",
        http_conn_id="http_gcs_default",  # Use an HTTP connection ID defined in Airflow
        endpoint=LOG_PYSPARK_FILE_URI
    )

    check_gdrive_uri_task = HttpSensor(
        task_id="check_gdrive_uri_task",
        http_conn_id="http_gdrive_default",  # Use an HTTP connection ID defined in Airflow
        endpoint=pyspark_file_urls['log_reviews']
    )


    # create_cluster = DataprocCreateClusterOperator(
    #     task_id="create_cluster",
    #     project_id=PROJECT_ID,
    #     cluster_config=CLUSTER_CONFIG,
    #     region=REGION,
    #     cluster_name=CLUSTER_NAME,
    #     delete_on_error=True, 
    #     use_if_exists=True,
    #     gcp_conn_id=GCP_CONN_ID
    # )

    # movie_pyspark_task = DataprocSubmitJobOperator(
    #     task_id="movie_pyspark_task", 
    #     job=MOVIE_PYSPARK_JOB, 
    #     region=REGION, 
    #     project_id=PROJECT_ID,
    #     gcp_conn_id=GCP_CONN_ID
    # )

    # log_pyspark_task = DataprocSubmitJobOperator(
    #     task_id="log_pyspark_task", 
    #     job=LOG_PYSPARK_JOB, 
    #     region=REGION, 
    #     project_id=PROJECT_ID,
    #     gcp_conn_id=GCP_CONN_ID
    # )

    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id="delete_cluster",
    #     project_id=PROJECT_ID,
    #     cluster_name=CLUSTER_NAME,
    #     region=REGION,
    #     gcp_conn_id=GCP_CONN_ID,      
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )

    end_workflow = DummyOperator(task_id="end_workflow", trigger_rule=TriggerRule.ONE_SUCCESS)

    (
        start_workflow
        >> get_files
        >> [upload_movie_pyspark_to_gcs, upload_log_pyspark_to_gcs]
        >> checks_start
    )
    checks_start >> [check_gcs_uri_task, check_gdrive_uri_task] >> end_workflow
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
