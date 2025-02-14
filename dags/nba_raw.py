from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from custom_operators import spark_iceberg_nessie_op


config = {
    "tables": [
        "Games",
        "Players",
        "TeamStatistics",
        "TeamHistories",
        "PlayerStatistics",
    ]
}


def process_xcom(**kwargs):
    dag_run = kwargs["dag_run"]
    dwh_date = dag_run.conf.get("dwh_date", "No date provided")
    return dwh_date


with DAG(
    dag_id="nba_raw",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    tags=["raw", "nba"],
) as dag:

    dag.doc_md = """
#### **DAG Details** ###

This DAG reads data from a **raw file on MinIO** and writes the transformed data to **Nessie**, performing the necessary ETL operations.

#### **Manual Run Configuration**
- This DAG **should not** be triggered manually; it must be triggered by the preceding DAG, **`nba_raw`**.

#### **Execution Steps**
1. Retrieve the execution date.
2. Execute the Spark job **`nba_raw_spark_job`** to perform ETL operations.
3. Trigger the next DAG, **`nba_staging`**.

#### **Owner Information**
- **Name:** Izzaldeen Radaideh  
- **Email:** izzaldeen_98@hotmail.com

    """

    get_date = PythonOperator(
        task_id="get_date",
        python_callable=process_xcom,
        provide_context=True,
    )

    # Step 2: Submit Spark job with JAR files from MinIO
    tables_tasks = []

    for table in config.get("tables"):
        task = spark_iceberg_nessie_op(
            task_name=table.lower(),
            spark_job_file_name="nba_raw_spark_job",
            arguments={
                "dwh_date": "{{ task_instance.xcom_pull(task_ids='get_date') }}",
                "table": table,
            },
        )
        tables_tasks.append(task)
    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="nba_staging",  # The DAG to trigger
        conf={"dwh_date": "{{ ds }}"},  # Pass date as 'yyyy-MM-dd'
    )

    get_date >> tables_tasks >> trigger_dag
