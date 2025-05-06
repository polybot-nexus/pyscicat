import os
import json
from datetime import datetime
from pathlib import Path
import urllib.parse

import pandas as pd
from minio import Minio
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import httpx

from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import Dataset, OrigDatablock, DataFile, Ownable, Attachment

# === Configuration ===
MINIO_ENDPOINT = "192.168.4.150:9000"
MINIO_FRONTEND_ENDPOINT = "192.168.4.150:9001"
MINIO_ACCESS_KEY = "rpl"
MINIO_SECRET_KEY = "rplrplrpl"
MINIO_BUCKET = "scicat-data"

SCICAT_URL = "http://192.168.4.150:3000/api/v3"
SCICAT_USERNAME = "ingestor"
SCICAT_PASSWORD = "aman"

ORCID_CLIENT_ID = "APP-Z7EPO5WHGU2XEWPJ"
ORCID_CLIENT_SECRET = "c86d92d8-b809-426c-a7a0-36d42a8c9f50"
ORCID_REDIRECT_URI = "https://caea-47-152-133-226.ngrok-free.app/auth/callback"
ORCID_AUTH_URL = "https://orcid.org/oauth/authorize"
ORCID_TOKEN_URL = "https://orcid.org/oauth/token"

FILE_PATH = "/Users/dozgulbas/scicat/pedot_pss_all_data_set/Train_6_2022-01-25_14-25-53_c0f0998bd8.json"
THUMBNAIL_PATH = "/Users/dozgulbas/scicat/test.png"

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="your_session_secret")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

scicat_client = ScicatClient(
    base_url=SCICAT_URL, username=SCICAT_USERNAME, password=SCICAT_PASSWORD
)

@app.get("/login")
async def login():
    params = {
        "client_id": ORCID_CLIENT_ID,
        "response_type": "code",
        "scope": "/authenticate",
        "redirect_uri": ORCID_REDIRECT_URI,
    }
    query = urllib.parse.urlencode(params)
    url = f"{ORCID_AUTH_URL}?{query}"
    return RedirectResponse(url)

@app.get("/auth/callback")
async def auth_callback(request: Request, code: str):
    async with httpx.AsyncClient() as client:
        response = await client.post(ORCID_TOKEN_URL, data={
            "client_id": ORCID_CLIENT_ID,
            "client_secret": ORCID_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": ORCID_REDIRECT_URI,
        })
        token_data = response.json()
        request.session["orcid_id"] = token_data.get("orcid")
        request.session["access_token"] = token_data.get("access_token")
    return RedirectResponse("/ingest")

@app.get("/ingest")
async def ingest(request: Request):
    orcid_id = request.session.get("orcid_id")
    if not orcid_id:
        return RedirectResponse("/login")

    ensure_minio_bucket()
    if os.path.exists(FILE_PATH) and (FILE_PATH.endswith(".csv") or FILE_PATH.endswith(".json")):
        file_url = upload_to_minio(FILE_PATH)
        register_in_scicat(FILE_PATH, file_url, orcid_id)
        return {"message": f"File {FILE_PATH} successfully ingested into MinIO and SciCat by {orcid_id}."}
    return {"error": "Invalid file path or format."}

def ensure_minio_bucket():
    try:
        exists = minio_client.bucket_exists(MINIO_BUCKET)
        if not exists:
            minio_client.make_bucket(MINIO_BUCKET)
    except Exception as err:
        print(f"Error checking/creating bucket: {err}")

def upload_to_minio(file_path: str) -> str:
    file_name = Path(file_path).name
    minio_client.fput_object(MINIO_BUCKET, file_name, file_path)
    return f"http://{MINIO_FRONTEND_ENDPOINT}/{MINIO_BUCKET}/{file_name}"

def sanitize_metadata(metadata: dict) -> dict:
    cleaned_metadata = {}
    for key, value in metadata.items():
        if isinstance(value, list) or isinstance(value, dict):
            cleaned_metadata[key] = json.dumps(value)
        else:
            cleaned_metadata[key] = value
    return cleaned_metadata

def extract_metadata(file_path: str) -> dict:
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
            "timestamp_summary": data.get("timestamp", [])[:5],
            "workflow_todo_count": len(data.get("workflow_todo", [])),
            "inputs_summary": {key: data["inputs"][key] for key in list(data.get("inputs", {}).keys())[:5]},
            "ml_outputs_summary": {k: v for k, v in data.get("ml_outputs", {}).items() if isinstance(v, (int, float, str))},
        })
    else:
        df = pd.read_csv(file_path)
        metadata["columns"] = list(df.columns)
        metadata["sample_content"] = df.head(1).to_dict()

    return sanitize_metadata(metadata)

def upload_thumbnail(dataset_id: str, path: str, caption="Thumbnail Image"):
    if not os.path.exists(path):
        return
    encoded = encode_thumbnail(Path(path))
    attach = Attachment(datasetId=dataset_id, thumbnail=encoded, caption=caption,
                        ownerGroup="rpl-team", accessGroups=["rpl", "public"])
    scicat_client.upload_attachment(attach)

def register_in_scicat(file_path: str, file_url: str, orcid_id: str):
    metadata = extract_metadata(file_path)
    ownable = Ownable(ownerGroup="rpl-team", accessGroups=["rpl", "public"])
    dataset = Dataset(
        path=file_path,
        size=metadata["size"],
        owner=orcid_id,
        contactEmail=f"{orcid_id}@orcid.org",
        creationLocation="RPL Server",
        creationTime=datetime.now().astimezone().isoformat(),
        type="raw",
        proposalId="experiment-001",
        dataFormat=metadata["file_type"],
        sourceFolder=file_url,
        scientificMetadata=metadata,
        isPublished=True,
        **ownable.model_dump()
    )
    dataset_id = scicat_client.upload_new_dataset(dataset)
    data_file = DataFile(path=file_url, size=metadata["size"])
    datablock = OrigDatablock(
        datasetId=dataset_id,
        dataFileList=[data_file],
        size=metadata["size"],
        version="1",
        **ownable.model_dump()
    )
    scicat_client.upload_dataset_origdatablock(dataset_id, datablock)
    upload_thumbnail(dataset_id, THUMBNAIL_PATH)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orcid_minio:app", host="0.0.0.0", port=8000, reload=True)
