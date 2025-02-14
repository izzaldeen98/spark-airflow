from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import time
from helper import run_portainer_container

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
    "config_arguemnts": "dwh_date",
}


def get_execution_date(**kwargs):
    execution_date = kwargs["ds"]
    kwargs["ti"].xcom_push(key="dwh_date", value=execution_date)

    print(f"Execution Date : {execution_date}")


def run_ingestion(**kwargs):
    dwh_date = kwargs["ti"].xcom_pull(key="dwh_date")
    payload = {
        "Image": "nba_ingestion:latest",
        "Cmd": ["--dwh_date", f"{dwh_date}"],  # Pass the parameter here
        "HostConfig": {
            "AutoRemove": True  # Automatically remove the container after execution
        },
    }
    run_portainer_container(payload=payload)


with DAG(
    dag_id="nba_date_ingest",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["nba", "ingestion"],
) as dag:

    dag.doc_md = """
    ### **DAG Details**

This DAG ingests **NBA** data from **Kaggle** and writes it to **MinIO**, performing the necessary ETL operations.

#### **Manual Run Configuration**
- **dwh_date**: The date of the run (yyyy-mm-dd).

#### **Execution Steps**
1. Retrieve the execution date.
2. Execute the Docker image to fetch and write data to MinIO.
3. Trigger the next DAG, **`nba_raw`**.

#### **Owner Information**
- **Name:** Izzaldeen Radaideh  
- **Email:** izzaldeen_98@hotmail.com
    """

    get_execution_date_task = PythonOperator(
        task_id="get_execution_date",
        python_callable=get_execution_date,
        provide_context=True,
    )

    run_ingestion_image = PythonOperator(
        task_id="run_ingestion", python_callable=run_ingestion, provide_context=True
    )

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="nba_raw",  # The DAG to trigger
        conf={"dwh_date": "{{ ds }}"},  # Pass date as 'yyyy-MM-dd'
    )

    get_execution_date_task >> run_ingestion_image >> trigger_dag
