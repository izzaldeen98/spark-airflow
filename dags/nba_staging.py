from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


config = {
    "source_schema": "nba_raw",
    "target_schema": "staging",
    "tables": [
        {
            "target_table": "nba_teams_stg",
            "source_table": "TeamStatistics",
            "query": "select DISTINCT teamId as team_number , HASH(teamId) as team_id , teamName as team_name , teamCity as team_city , dwh_date from {schema}.TeamStatistics where dwh_date = date'{dwh_date}'",
        },
        {
            "target_table": "nba_games_stg",
            "source_table": "Games",
            "query": "select HASH(gameid) as game_id , cast(gameDate as date) as game_date , hash(hometeamid) as home_team_id , hash(awayteamId) as away_team_id , gameType as game_type , cast(cast(attendance as DOUBLE) as int) as attendance, dwh_date from {schema}.Games where dwh_date = '{dwh_date}' ",
        },
        {
            "target_table": "nba_games_number_stg",
            "source_table": "PlayerStatistics",
            "query": "select hash(concat(personid , ':' , gameid)) as id, HASH(personid) as player_id , HASH(gameid) as game_id , case when win = '1' THEN True else False end as is_win , cast(numMinutes as DOUBLE) as played_minutes, cast(cast(points as double) as int) as points, cast(cast(assists as double) as int) as assists, cast(cast(blocks as double) as int) as blocks, cast(cast(steals as double) as int) as steals, cast(cast(fieldGoalsAttempted as double) as int) as score_attempts, cast(cast(fieldGoalsMade as double) as int) as score_mades, cast(cast(threePointersAttempted as double) as int) as three_points_attempts, cast(cast(threePointersMade as double) as int) as three_points_made, cast(cast(freeThrowsAttempted as double) as int) as free_throw_attempts, cast(cast(freeThrowsMade as double) as int) as free_throw_made, cast(cast(reboundsDefensive as double) as int) as defence_rebounds, cast(cast(reboundsOffensive as double) as int) as offense_rebounds, cast(cast(foulsPersonal as double) as int) as fouls, cast(cast(turnovers as double) as int) as turnovers, dwh_date from {schema}.PlayerStatistics where dwh_date = date'{dwh_date}' ",
        },
        {
            "target_table": "nba_players_stg",
            "source_table": "Players",
            "query": "SELECT  HASH(personid) as player_id, personId as player_number, firstName as first_name , lastName as last_name , CONCAT(firstname , ' ' , lastname) as full_name , cast(birthdate as date) as birth_date , country as home_country, cast(height as double) as height_inches, CASE WHEN height IS NULL THEN NULL ELSE CONCAT( CAST(FLOOR(CAST(height AS DOUBLE) / 12) AS INT), ',', CAST(MOD(CAST(height AS DOUBLE), 12) AS INT)) END AS height_foot_inches, round(CAST(height AS double) * 2.54 , 2) as height_cm, CAst(bodyweight as double) as weight_pound , round(cast(bodyweight as DOUBLE) * 0.45359237 , 2) as weight_kg , cast(guard as BOOLEAN) as is_guard, cast(forward as BOOLEAN) as is_forward, cast(center as BOOLEAN) as is_center, CONCAT_WS(',', case when guard = 'True' then 'guard'end, case when forward = 'True' then 'forward'end , case when center = 'True' then 'center'end ) as playing_positions , dwh_date from {schema}.Players where dwh_date = date'{dwh_date}' ",
        },
    ],
}


def process_xcom(**kwargs):
    dag_run = kwargs["dag_run"]
    dwh_date = dag_run.conf.get("dwh_date", "No date provided")
    return dwh_date


with DAG(
    dag_id="nba_staging",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    tags=["staging", "nba"],
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
        query = table.get("query")

        source_schema = config.get("source_schema")
        target_schema = config.get("target_schema")

        task = SparkSubmitOperator(
            task_id=f"{target_table}",
            application="/opt/airflow/jobs/nba_staging_spark_job.py",
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
                "--query",
                f"{query}",
            ],
        )
        tables_tasks.append(task)

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="nba_prod",  # The DAG to trigger
        conf={"dwh_date": "{{ ds }}"},  # Pass date as 'yyyy-MM-dd'
    )

    get_date >> tables_tasks >> trigger_dag
