import os
import json
import pandas as pd
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from pyscicat.client import ScicatClient
from pyscicat.model import Dataset, OrigDatablock, DataFile, Ownable

# MinIO Configuration
MINIO_ENDPOINT = "192.168.4.150:9000"
MINIO_ACCESS_KEY = "rpl"
MINIO_SECRET_KEY = "rplrplrpl"
MINIO_BUCKET = "scicat-data"

# SciCat Configuration
SCICAT_URL = "http://192.168.4.150:3000/api/v3"
SCICAT_USERNAME = "ingestor"
SCICAT_PASSWORD = "aman"

# File to ingest
FILE_PATH = "/Users/dozgulbas/Desktop/pedot_pss_all_data_set/Train_9_2022-02-22_19-33-07_e35a902a26.json"  # Change this to the actual file path

# Initialize MinIO Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # Change to True if using HTTPS
)

# Initialize SciCat Client
scicat_client = ScicatClient(
    base_url=SCICAT_URL, username=SCICAT_USERNAME, password=SCICAT_PASSWORD
)

# Ensure MinIO bucket exists
def ensure_minio_bucket():
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
    except S3Error as err:
        print(f"Error checking/creating bucket: {err}")

# Upload file to MinIO
def upload_to_minio(file_path: str) -> str:
    file_name = Path(file_path).name
    minio_client.fput_object(MINIO_BUCKET, file_name, file_path)
    file_url = f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{file_name}"
    return file_url

# Extract metadata from JSON/CSV
def extract_metadata(file_path: str) -> dict:
    file_size = os.path.getsize(file_path)
    file_type = "json" if file_path.endswith(".json") else "csv"
    
    metadata = {"size": file_size, "file_type": file_type}
    
    if file_type == "json":
        with open(file_path, "r") as f:
            data = json.load(f)
        
        # Extract essential fields
        metadata.update({
            "source": data.get("source", "unknown"),
            "trial": data.get("trial", "N/A"),
            "ID": data.get("ID", "N/A"),
            "score": data.get("score", None),
            "status": data.get("status", "unknown"),
            "location": data.get("location", "unknown"),
            "workflow_file_hash": data.get("workflow_file_hash", "N/A"),
        })
        
        # Store summary of timestamps and workflow
        metadata["timestamp_summary"] = data.get("timestamp", [])[:5]  # First 5 timestamps
        metadata["workflow_todo_count"] = len(data.get("workflow_todo", []))
        
        # Flatten and summarize inputs
        metadata["inputs_summary"] = {key: data["inputs"][key] for key in list(data.get("inputs", {}).keys())[:5]}
        
        # Summarize ML outputs (avoid full arrays)
        ml_outputs = data.get("ml_outputs", {})
        metadata["ml_outputs_summary"] = {k: v for k, v in ml_outputs.items() if isinstance(v, (int, float, str))}
    
    else:
        df = pd.read_csv(file_path)
        metadata["columns"] = list(df.columns)
        metadata["sample_content"] = df.head(1).to_dict()
    
    return metadata

# Register dataset in SciCat
def register_in_scicat(file_path: str, file_url: str):
    metadata = extract_metadata(file_path)
    ownable = Ownable(ownerGroup="rpl-team", accessGroups=["rpl"])
    dataset = Dataset(
        owner="data_ingestor",
        contactEmail="data@lab.org",
        creationLocation="Lab Server",
        creationTime=str(pd.Timestamp.now()),
        type="raw",
        proposalId="experiment-001",
        dataFormat=metadata["file_type"],
        sourceFolder=file_url,
        scientificMetadata=metadata,
        **ownable.model_dump()  # ✅ Use `model_dump()` instead of `.dict()`
    )
    dataset_id = scicat_client.upload_new_dataset(dataset)
    print("Sending dataset to SciCat:")
    print(dataset.model_dump_json(indent=2))  # Debug output
    # Register OrigDatablock
    data_file = DataFile(path=file_url, size=metadata["size"])
    datablock = OrigDatablock(
        datasetId=dataset_id,
        dataFileList=[data_file],
        size=metadata["size"],
        version="1",
        **ownable.model_dump()  # ✅ Fix ownerGroup duplication issue
    )
    scicat_client.upload_dataset_origdatablock(dataset_id, datablock)
    print(f"Registered dataset {dataset_id} with metadata in SciCat.")


if __name__ == "__main__":
    ensure_minio_bucket()
    
    if os.path.exists(FILE_PATH) and (FILE_PATH.endswith(".csv") or FILE_PATH.endswith(".json")):
        file_url = upload_to_minio(FILE_PATH)
        register_in_scicat(FILE_PATH, file_url)
        print(f"File {FILE_PATH} successfully ingested into MinIO and SciCat.")
    else:
        print("Invalid file path or unsupported file format. Please provide a valid JSON/CSV file.")
