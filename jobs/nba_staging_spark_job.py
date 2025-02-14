import logging
from pyspark.sql import SparkSession
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--dwh_date", type=str, required=True)
parser.add_argument("--source_table", type=str, required=True)
parser.add_argument("--target_table", type=str, required=True)
parser.add_argument("--source_schema", type=str, required=True)
parser.add_argument("--target_schema", type=str, required=True)
parser.add_argument("--query", type=str, required=True)
args = parser.parse_args()

# Assign arguments to variables
dwh_date = args.dwh_date
source_table = args.source_table
target_table = args.target_table
source_schema = args.source_schema
target_schema = args.target_schema
query = args.query

# Initialize Spark session with Iceberg and Nessie configurations
spark = (
    SparkSession.builder.appName(f"NBA {target_table} STAGING")
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

# Define the Iceberg table name in Nessie
iceberg_source_table = f"nessie.{source_schema}.{source_table}"

# Format query and log it
query = query.format(schema=f"nessie.{source_schema}", dwh_date=dwh_date)
logging.info(f"Executing Query: {query}")

# Read the Iceberg table
df = spark.sql(query)

# Log the data read
logging.info(f"Reading data from Iceberg table: {iceberg_source_table}")

# Define target table
target_table_name = f"nessie.{target_schema}.{target_table}"

# Ensure target namespace exists
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{target_schema}")

# Write data to Iceberg table with partitioning
df.writeTo(target_table_name).using("iceberg").partitionedBy(
    "dwh_date"
).createOrReplace()

# Log write operation
logging.info(f"Filtered data written to Iceberg table: {target_table_name}")

# Stop the Spark session
spark.stop()
