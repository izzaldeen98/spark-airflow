from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


config = {
    "source_schema": "staging",
    "target_schema": "prod",
    "tables": [
        {
            "target_table": "nba_team",
            "source_table": "nba_teams_stg",
            "pk": "team_id",
        },
        {
            "target_table": "nba_game",
            "source_table": "nba_games_stg",
            "pk": "game_id",
        },
        {
            "target_table": "nba_games_stat",
            "source_table": "nba_games_number_stg",
            "pk": "id",
        },
        {
            "target_table": "nba_player",
            "source_table": "nba_players_stg",
            "pk": "player_id",
        },
    ],
}


def process_xcom(**kwargs):
    dag_run = kwargs["dag_run"]
    dwh_date = dag_run.conf.get("dwh_date", "No date provided")
    return dwh_date


with DAG(
    dag_id="nba_prod",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    tags=["prod", "nba"],
) as dag:

    get_date = PythonOperator(
        task_id="get_date",
        python_callable=process_xcom,
        provide_context=True,
    )

    # Step 2: Submit Spark job with JAR files from MinIO
    tables_tasks = []

    for table in config.get("tables"):
        source_table = table.get("source_table")
        target_table = table.get("target_table")

        source_schema = config.get("source_schema")
        target_schema = config.get("target_schema")
        pk = table.get("pk")

        task = SparkSubmitOperator(
            task_id=f"{target_table}",
            application="/opt/airflow/jobs/nba_prod_spark_job.py",
            conn_id="spark-cluster",  # Ensure this connection points to your Spark cluster
            jars="/opt/airflow/jars/aws-java-sdk-bundle-1.12.426.jar,/opt/airflow/jars/hadoop-aws-3.3.2.jar,/opt/airflow/jars/iceberg-spark-runtime-3.3_2.12-1.7.1.jar,/opt/airflow/jars/nessie-spark-extensions-3.3_2.12-0.63.0.jar",
            verbose=True,
            # Pass arguments as a list
            application_args=[
                "--dwh_date",
                "{{ task_instance.xcom_pull(task_ids='get_date') }}",  # Dynamically pull dwh_date from XCom
                "--source_table",
                f"{source_table}",
                "--target_table",
                f"{target_table}",
                "--source_schema",
                f"{source_schema}",
                "--target_schema",
                f"{target_schema}",
                "--pk",
                f"{pk}",
            ],
        )
        tables_tasks.append(task)

    get_date >> tables_tasks
