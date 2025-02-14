from os import environ

minio_url = environ["MINIO_URL"]
minio_access_key = environ["MINIO_ACCESS_KEY"]
minio_secret_key = environ["MINIO_SECRET_KEY"]

nessie_url = environ["NESSIE_URL"]
nessie_data_warehouse_path = environ["NESSIE_DATA_WAREHOUSE_PATH"]

portinar_username = environ["PORTINAR_USERNAME"]
portinar_password = environ["PORTINAR_PASSWORD"]
portinar_url = environ["PORTINAR_URL"]