from airflow import DAG
from custom_operators import spark_iceberg_nessie_op
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
        
        task = spark_iceberg_nessie_op(
            task_name=target_table,
            spark_job_file_name="nba_prod_spark_job",
            arguments={
                "dwh_date" : "{{ task_instance.xcom_pull(task_ids='get_date') }}",
                "source_table" : source_table,
                "target_table" : target_table,
                "source_schema" :source_schema,
                "target_schema" : target_schema,
                "pk" : pk
                
            }
        )

    get_date >> tables_tasks
