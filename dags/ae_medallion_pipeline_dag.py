from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator
from datetime import datetime, timedelta
import sys

#sys.path.append("/opt/airflow/python_scripts")

from python_scripts.patient_ae_journey import insert_patient_ae
from python_scripts.ward_occupancy import insert_ward_occupancy

default_args = {
    "owner": "manjunatha",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def generate_patient_ae_wrapper() -> None:
    insert_patient_ae(20)

def generate_ward_occupancy_wrapper() -> None:
    insert_ward_occupancy()

with DAG(
    dag_id="nhs_ae_etl_pipeline",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    t1_generate_patients = PythonOperator(
        task_id="generate_patient_ae_journeys",
        python_callable=generate_patient_ae_wrapper,
    )

    t2_generate_wards = PythonOperator(
        task_id="generate_ward_occupancy",
        python_callable=generate_ward_occupancy_wrapper,
    )

    t3_bronze_to_silver = SQLExecuteQueryOperator(
        task_id="bronze_to_silver_transform",
        conn_id="mssql_local",     # make sure this matches your MSSQL connection in Airflow
        sql="bronze_to_silver.sql",
        autocommit=True,           
    )

    t4_silver_to_gold = SQLExecuteQueryOperator(
        task_id="silver_to_gold_transform",
        conn_id="mssql_local",
        sql="silver_to_gold.sql",
        autocommit=True,
    )

    # t5_powerbi_refresh = PowerBIDatasetRefreshOperator(
    #     task_id="refresh_powerbi",
    #     dataset_id="your-dataset-id",
    #     workspace_id="your-workspace-id",
    #     client_id="your-azure-client-id",
    #     client_secret="your-azure-client-secret",
    #     tenant_id="your-azure-tenant-id",
    # )

    # Define task order
    [t1_generate_patients, t2_generate_wards] >> t3_bronze_to_silver >> t4_silver_to_gold #>> t5_powerbi_refresh
