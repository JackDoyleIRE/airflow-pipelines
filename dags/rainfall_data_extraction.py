import os
from airflow import DAG
from modules.api_tasks import ApiCallOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

###############
# Environment #
###############
PROJECT_ID = os.getenv("gcp_project_name")
BUCKET_NAME = os.getenv("gcp_rainfall_bucket_name")
OBJECT_NAME = os.getenv("gcp_rainfall_object_name")
BQ_TABLE = os.getenv("bq_project_table_name")

#############
# Variables #
#############
START_DATE = "2023-01-01"
END_DATE = "2023-01-01"
STATION_ID = ["SDF"]

STATION_ID_STR = ",".join(STATION_ID)

#################
# Initilise DAG #
#################

with DAG(
    dag_id="rainfall_extraction",
    description="""
        DAG for extracting rainfall data
    """,
    default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
},
    schedule_interval=timedelta(days=1),  
    start_date=datetime(2023, 7, 1),
    is_paused_upon_creation=True
) as rainfall_data:

    # start task operator
    start_task = DummyOperator(task_id="start")

    # complete task operator
    complete_task = DummyOperator(
        task_id = "complete"
    )

    # make api call to fetch the data
    api_call_task = ApiCallOperator(
    task_id='call_api',
    endpoint='/dynamicapp/req/JSONDataServlet',
    destination_bucket_name=BUCKET_NAME,
    params={
        'Stations': STATION_ID_STR,
        'SensorNums': '2',
        'dur_code': 'D',
        'Start': START_DATE,
        'End': END_DATE
    },
    
)

# Send the data to bigquery
    load_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_gcs_to_bq',
    bucket=BUCKET_NAME,
    source_objects=['test_data_json_2'],
    destination_project_dataset_table=BQ_TABLE,
    schema_fields=[
        {'name': 'field1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'field2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    source_format='NEWLINE_DELIMITED_JSON',
    project_id=PROJECT_ID

    )


    


start_task >> api_call_task >> load_gcs_to_bq >> complete_task