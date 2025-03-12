# Download file
from minio import Minio
from minio.error import S3Error


download_path = "test.png"
try:
    minio_client.fget_object(bucket_name, object_name, download_path)
    print(f"File '{object_name}' downloaded successfully as '{download_path}'")
except S3Error as err:
    print(f"Error downloading file: {err}")
