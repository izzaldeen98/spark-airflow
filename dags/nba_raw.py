from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


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

    get_date = PythonOperator(
        task_id="get_date",
        python_callable=process_xcom,
        provide_context=True,
    )

    # Step 2: Submit Spark job with JAR files from MinIO
    tables_tasks = []

    for table in config.get("tables"):
        print(table)
        task = SparkSubmitOperator(
            task_id=f"{table.lower()}",
            application="/opt/airflow/jobs/nba_raw_spark_job.py",
            conn_id="spark-cluster",  # Ensure this connection points to your Spark cluster
            jars="/opt/airflow/jars/aws-java-sdk-bundle-1.12.426.jar,/opt/airflow/jars/hadoop-aws-3.3.2.jar,/opt/airflow/jars/iceberg-spark-runtime-3.3_2.12-1.7.1.jar,/opt/airflow/jars/nessie-spark-extensions-3.3_2.12-0.63.0.jar",
            verbose=True,
            # Pass arguments as a list
            application_args=[
                "--dwh_date",
                "{{ task_instance.xcom_pull(task_ids='get_date') }}",  # Dynamically pull dwh_date from XCom
                "--table",
                f"{table}",  # Pass the current table name
            ],
        )
        tables_tasks.append(task)
    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="nba_staging",  # The DAG to trigger
        conf={"dwh_date": "{{ ds }}"},  # Pass date as 'yyyy-MM-dd'
    )

    get_date >> tables_tasks >> trigger_dag
