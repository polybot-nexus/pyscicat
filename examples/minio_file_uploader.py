# file_uploader.py MinIO Python SDK example
from minio import Minio
from minio.error import S3Error

def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        "192.168.4.150:9000",  # MinIO server URL
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  # Set to True if using HTTPS
    )


    # The file to upload, change this path if needed
    source_file = "/Users/dozgulbas/scicat/test.png"

    # The destination bucket and filename on the MinIO server
    bucket_name = "mybucket"
    destination_file = "test.png"

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    client.fput_object(
        bucket_name, destination_file, source_file,
    )
    print(
        source_file, "successfully uploaded as object",
        destination_file, "to bucket", bucket_name,
    )
    
    download_path = "test.png"
    try:
        client.fget_object(bucket_name, destination_file, download_path)
        print(f"File '{destination_file}' downloaded successfully as '{download_path}'")
    except S3Error as err:
        print(f"Error downloading file: {err}")

    objects = client.list_objects(bucket_name)
    for obj in objects:
        print(obj.object_name)

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)