import os
import shutil
import kagglehub
from minio import Minio
from datetime import datetime
import argparse

ap = argparse.ArgumentParser()

ap.add_argument("--dwh_date" , type=str)
args = ap.parse_args()

dwh_date = args.dwh_date



# Download the dataset
dataset_id = "eoinamoore/historical-nba-data-and-player-box-scores"
path = kagglehub.dataset_download(dataset_id)

print("Downloaded dataset path:", path)

# Ensure the current script directory is the target directory
script_directory = os.getcwd()
target_directory = os.path.join(script_directory, "downloaded_data")
os.makedirs(target_directory, exist_ok=True)

# Move downloaded files to the target directory
for file_name in os.listdir(path):
    source_file = os.path.join(path, file_name)
    target_file = os.path.join(target_directory, file_name)
    shutil.move(source_file, target_file)

print("Files moved to:", target_directory)

# Configure MinIO client
minio_client = Minio(
    "host.docker.internal:9000",
    access_key='admin',
    secret_key='admin123',
    secure=False
)

# Generate file name and upload to MinIO
file_name = dwh_date.replace("-" , "")
bucket_name = "izzaldeen-analytics-v1"
object_name = f"landing/nba/{file_name}"

for file_name in os.listdir(target_directory):
    file_path = os.path.join(target_directory, file_name)
    minio_client.fput_object(bucket_name, f"{object_name}/{file_name}", file_path)
    print(file_name)

print(f"Files uploaded to s3a://{bucket_name}/{object_name} from {target_directory}")
