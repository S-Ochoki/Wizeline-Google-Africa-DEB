
# BigQuery example documentation - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#upsert-table
# BigQuery operators - https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/bigquery/index.html#airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator


from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
# from airflow.operators.python import BitShift
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


# General constants
DAG_ID = "capstone_stage_to_bigquery"
GCP_PROJECT_ID = 'wizeline-deb-capstone'

# GCP constants
GCP_CONN_ID = "gcp_default"
GCS_BUCKET_NAME = "wizeline_bootcamp_bucket"
REGION = "us-central1"
LOCATION = "US"

# Files
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
    # 'dim_browser': [
    #     {'name': 'id_dim_browser', 'type': 'INTEGER'},
    #     {'name': 'browser', 'type': 'STRING'},
    # ],
    'fact_movie_analytics': [
        {'name': 'customerid', 'type': 'INTEGER'}, # user_purchase.CustomerID
        {'name': 'id_dim_devices', 'type': 'INTEGER'}, # dim_devices.id_dim_devices
        {'name': 'id_dim_location', 'type': 'INTEGER'}, # dim_location.id_dim_location
        {'name': 'id_dim_os', 'type': 'INTEGER'}, # dim_os.id_dim_os
        {'name': 'amount_spent', 'type': 'NUMERIC', 'mode': 'NULLABLE', 'precision': 18, 'scale': 5}, # SUM(user_purchase.quantity * user_purchase.unit_price) group by user_purchase.CustomerID
        {'name': 'review_score', 'type': 'INTEGER'}, # SUM(movie_reviews.positive_review) group by user_purchase.CustomerID
        {'name': 'review_count', 'type': 'INTEGER'}, # COUNT(movie_reviews.review_id) group by user_purchase.CustomerID
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
                    SELECT CustomerID, SUM(Quantity * UnitPrice) amount_spent
                    FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.user_purchase` 
                    GROUP BY CustomerID
                )
            SELECT 
                up.CustomerID AS customerid,
                (SELECT ddt.id_dim_date FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_date ddt WHERE ddt.log_date = lr.logDate) id_dim_date,
                (SELECT dd.id_dim_devices FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_devices dd WHERE dd.device = lr.device) id_dim_devices,
                (SELECT dl.id_dim_location FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_location dl WHERE dl.location = lr.location) id_dim_location,
                (SELECT dos.id_dim_os FROM {GCP_PROJECT_ID}.{DATASET_NAME}.dim_os dos WHERE dos.os = lr.os) id_dim_os,
                CAST(up.amount_spent AS NUMERIC) amount_spent,
                mr.review_score,
                mr.review_count,
                CURRENT_DATE() AS insert_date
            FROM up
            INNER JOIN mr ON up.CustomerID = mr.user_id
            INNER JOIN {GCP_PROJECT_ID}.{DATASET_NAME}.classified_movie_reviews mr2 ON up.CustomerID = mr2.user_id
            INNER JOIN {GCP_PROJECT_ID}.{DATASET_NAME}.log_reviews_transformed lr ON mr2.review_id = lr.id_review
        """,
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=["gcp", "bigquery"],
) as dag:
    
    start_workflow = DummyOperator(task_id="start_workflow")

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

    start_workflow >> create_dataset >> upload_gcs_data_tasks >> gcs_data_uploaded >> create_bq_table_tasks >> bq_tables_created >> insert_dim_data_tasks >> dim_tables_populated >> insert_fact_data >> end_workflow
