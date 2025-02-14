from pyspark.sql import SparkSession
import argparse

# Parse command-line arguments


def iceberg_table_exists(spark, schema, table):
    result = spark.sql(f"SHOW TABLES IN nessie.{schema}").filter(
        f"tableName = '{table}'"
    )
    return result.count() > 0


parser = argparse.ArgumentParser()
parser.add_argument("--dwh_date", type=str, required=True)
parser.add_argument("--source_table", type=str, required=True)
parser.add_argument("--target_table", type=str, required=True)
parser.add_argument("--source_schema", type=str, required=True)
parser.add_argument("--target_schema", type=str, required=True)
parser.add_argument("--pk", type=str, required=True)
args = parser.parse_args()

# Assign variables
dwh_date = args.dwh_date
source_table = args.source_table
target_table = args.target_table
source_schema = args.source_schema
target_schema = args.target_schema
primary_key = args.pk

# Initialize Spark session with Iceberg & Nessie configurations
spark = (
    SparkSession.builder.appName(f"NBA {target_table} PROD")
    .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    .config("spark.sql.catalog.nessie.uri", "http://host.docker.internal:19120/api/v1")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config(
        "spark.sql.catalog.nessie.warehouse", "s3a://izzaldeen-analytics-v1/warehouse"
    )
    .getOrCreate()
)

# Define Iceberg source and target table names
iceberg_source_table = f"nessie.{source_schema}.{source_table}"
iceberg_target_table = f"nessie.{target_schema}.{target_table}"


# Read data from Iceberg table
df = spark.sql(f"SELECT * FROM {iceberg_source_table} WHERE dwh_date = '{dwh_date}'")
print(f"Reading data from Iceberg table: {iceberg_source_table}")

# Ensure the target schema exists
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{target_schema}")

# Ensure the target table exists with partitioning
if not iceberg_table_exists(spark, target_schema, target_table):
    print(
        f"Table `{target_table}` doesn't exists , creating and new table with name {target_table}"
    )
    df.writeTo(iceberg_target_table).using("iceberg").partitionedBy(
        "dwh_date"
    ).createOrReplace()
    spark.stop()
else:
    # Register the DataFrame as a temporary view for merging
    df.createOrReplaceTempView("source_temp_table")

    # Perform the **MERGE INTO** operation using Iceberg SQL
    merge_query = f"""
        MERGE INTO {iceberg_target_table} AS target
        USING source_temp_table AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN 
            UPDATE SET *
        WHEN NOT MATCHED THEN 
            INSERT *
    """
    spark.sql(merge_query)
    print(f"Upsert completed for Iceberg table: {iceberg_target_table}")

    # Stop the Spark session
    spark.stop()
