from airflow import DAG
from airflow.operators.python import PythonOperator

import datetime
from dotenv import load_dotenv
import sys
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/Service_Account_Credentials.json"

load_dotenv("/opt/airflow")
sys.path.append("/opt/airflow")

from helper.helper import _extract_covid_data, _pre_process, _load_to_bigQuery


default_args = {
    'start_date':datetime.datetime(2023,8,30)
}

with DAG(dag_id = "load_covid_data", catchup = False, schedule_interval = "@daily", default_args=default_args) as dag:
    

    # Task #1 - Extract data from API
    extract_data = PythonOperator(
        task_id = "extract_data",
        python_callable=_extract_covid_data
    )

    # Task #2 - Clean data
    pre_process = PythonOperator(
        task_id = "pre_process",
        python_callable=_pre_process
    )

    #Task #4 - Load to bigquery
    load_to_bigquery = PythonOperator(
        task_id = "load_to_bigquery",
        python_callable=_load_to_bigQuery,
        op_kwargs={
            "dataset_name": "covid_analysis",
            "table_name": "covid_data"
        }
    )

    # Dependencies
    extract_data >> pre_process >> load_to_bigquery
    

