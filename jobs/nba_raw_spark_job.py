from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date
import argparse
from os import environ

minio_url = environ["MINIO_URL"]
minio_access_key = environ["MINIO_ACCESS_KEY"]
minio_secret_key = environ["MINIO_SECRET_KEY"]

nessie_url = environ["NESSIE_URL"]
nessie_data_warehouse_path = environ["NESSIE_DATA_WAREHOUSE_PATH"]


# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--dwh_date", type=str, required=True)
parser.add_argument("--table", type=str, required=True)
args = parser.parse_args()
dwh_date = args.dwh_date
table = args.table

# Initialize Spark session with Iceberg and Nessie configurations
spark = (
    SparkSession.builder.appName(f"NBA {table} RAW")
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

# Define the S3 path for the CSV file
s3_path = (
    f"s3a://izzaldeen-analytics-v1/landing/nba/{dwh_date.replace('-', '')}/{table}.csv"
)

# Read the CSV file from the S3 path
df = spark.read.csv(s3_path, header=True)
df.show()

# Define the Iceberg table name in Nessie
namespace = "nba_raw"
iceberg_table_name = f"nessie.{namespace}.{table}"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")

df = df.withColumn("dwh_date", to_date(lit(dwh_date), "yyyy-MM-dd"))
# Write the DataFrame as an Iceberg table
df.writeTo(iceberg_table_name).partitionedBy("dwh_date").using(
    "iceberg"
).createOrReplace()

# Stop the Spark session
spark.stop()
