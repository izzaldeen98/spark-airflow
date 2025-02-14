from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def spark_iceberg_nessie_op(
    task_name : str ,
    spark_job_file_name : str,
    arguments : dict,
    connection = "spark-cluster",
    verbose = True):

    arg_list = []
    for key , value in arguments.items():
        arg_list.append(f"--{key}")
        arg_list.append(value)
    return SparkSubmitOperator(
            task_id=task_name,
            application=f"/opt/airflow/jobs/{spark_job_file_name}.py",
            conn_id=connection,  # Ensure this connection points to your Spark cluster
            jars="/opt/airflow/jars/aws-java-sdk-bundle-1.12.426.jar,/opt/airflow/jars/hadoop-aws-3.3.2.jar,/opt/airflow/jars/iceberg-spark-runtime-3.3_2.12-1.7.1.jar,/opt/airflow/jars/nessie-spark-extensions-3.3_2.12-0.70.0.jar",
            verbose=verbose,
            # Pass arguments as a list
            application_args=arg_list,
        )