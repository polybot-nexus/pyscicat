import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from minio import Minio
from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import Dataset, OrigDatablock, DataFile, Ownable, Attachment
from dotenv import load_dotenv
import logging

load_dotenv()

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_FRONTEND_ENDPOINT = os.getenv("MINIO_FRONTEND_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

SCICAT_URL = os.getenv("SCICAT_URL")
SCICAT_USERNAME = os.getenv("SCICAT_USERNAME")
SCICAT_PASSWORD = os.getenv("SCICAT_PASSWORD")

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize MinIO Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Initialize SciCat Client
scicat_client = ScicatClient(
    base_url=SCICAT_URL, username=SCICAT_USERNAME, password=SCICAT_PASSWORD
)


def ensure_minio_bucket():
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
        logging.info(f"MinIO bucket '{MINIO_BUCKET}' verified/created.")
    except Exception as err:
        logging.error(f"Error checking/creating bucket: {err}")


def upload_to_minio(file_path: str) -> str:
    try:
        file_name = Path(file_path).name
        minio_client.fput_object(MINIO_BUCKET, file_name, file_path)
        file_url = f"http://{MINIO_FRONTEND_ENDPOINT}/{MINIO_BUCKET}/{file_name}"
        logging.info(f"File '{file_name}' uploaded to MinIO: {file_url}")
        return file_url
    except Exception as e:
        logging.error(f"Error uploading to MinIO: {e}")
        return ""


def extract_metadata(file_path: str) -> dict:
    try:
        file_size = os.path.getsize(file_path)
        file_type = "json" if file_path.endswith(".json") else "csv"
        metadata = {"size": file_size, "file_type": file_type}

        if file_type == "json":
            with open(file_path, "r") as f:
                data = json.load(f)
            metadata.update({
                "source": data.get("source", "unknown"),
                "trial": data.get("trial", "N/A"),
                "ID": data.get("ID", "N/A"),
                "score": data.get("score", None),
                "status": data.get("status", "unknown"),
                "location": data.get("location", "unknown"),
                "workflow_file_hash": data.get("workflow_file_hash", "N/A"),
            })
        else:
            df = pd.read_csv(file_path)
            metadata["columns"] = list(df.columns)
            metadata["sample_content"] = df.head(1).to_dict()

        logging.info(f"Metadata extracted for '{file_path}': {metadata}")
        return metadata
    except Exception as e:
        logging.error(f"Error extracting metadata: {e}")
        return {}


def register_in_scicat(file_path: str, file_url: str, orcid_id: str, thumbnail_path):
    try:
        if not orcid_id:
            logging.warning("ORCID ID is missing. Cannot register dataset.")
            return

        metadata = extract_metadata(file_path)
        ownable = Ownable(ownerGroup="rpl-team", accessGroups=["rpl", "public"])
        dataset = Dataset(
            path=file_path,
            size=metadata.get("size", 0),
            owner=orcid_id,
            contactEmail=f"{orcid_id}@orcid.org",
            creationLocation="RPL Server",
            creationTime=datetime.now().astimezone().isoformat(),
            type="raw",
            proposalId="experiment-001",
            dataFormat=metadata.get("file_type", "unknown"),
            sourceFolder=file_url,
            scientificMetadata=metadata,
            isPublished=True,
            **ownable.model_dump()
        )
        dataset_id = scicat_client.upload_new_dataset(dataset)
        logging.info(f"Dataset registered in SciCat with ID: {dataset_id}")

        data_file = DataFile(path=file_url, size=metadata.get("size", 0))
        datablock = OrigDatablock(
            datasetId=dataset_id,
            dataFileList=[data_file],
            size=metadata.get("size", 0),
            version="1",
            **ownable.model_dump()
        )
        scicat_client.upload_dataset_origdatablock(dataset_id, datablock)
        logging.info(f"OrigDatablock registered for dataset {dataset_id}")

        upload_thumbnail(dataset_id, thumbnail_path)
    except Exception as e:
        logging.error(f"Error registering in SciCat: {e}")


def upload_thumbnail(dataset_id: str, path: str, caption="Thumbnail Image"):
    try:
        if not os.path.exists(path):
            logging.warning(f"Thumbnail path '{path}' does not exist.")
            return
        encoded = encode_thumbnail(Path(path))
        attachment = Attachment(datasetId=dataset_id, thumbnail=encoded, caption=caption,
                                ownerGroup="rpl-team", accessGroups=["rpl", "public"])
        scicat_client.upload_attachment(attachment)
        logging.info(f"Thumbnail uploaded for dataset {dataset_id}")
    except Exception as e:
        logging.error(f"Error uploading thumbnail: {e}")
