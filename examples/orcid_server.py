import os
import urllib.parse
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv
import threading
import uvicorn
import httpx
from datetime import datetime, timedelta
import logging
import uuid
import json

from ingestors import register_in_scicat, ensure_minio_bucket, upload_to_minio

# Load environment variables
load_dotenv()

# === Configuration ===
ORCID_CLIENT_ID = os.getenv("ORCID_CLIENT_ID")
ORCID_CLIENT_SECRET = os.getenv("ORCID_CLIENT_SECRET")
ORCID_REDIRECT_URI = os.getenv("ORCID_REDIRECT_URI")
ORCID_AUTH_URL = "https://orcid.org/oauth/authorize"
ORCID_TOKEN_URL = "https://orcid.org/oauth/token"
AUTHORIZED_ORCIDS = set(os.getenv("AUTHORIZED_ORCIDS", "").split(","))
AUTH_EXPIRY_MINUTES = int(os.getenv("AUTH_EXPIRY_MINUTES", 30))
SESSION_SECRET_KEY = os.getenv("SESSION_SECRET_KEY")

# In-memory session store
SESSIONS = {}

# Configure logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY, max_age=AUTH_EXPIRY_MINUTES * 60)

@app.get("/login")
async def login(request: Request):
    transaction_id = str(uuid.uuid4())
    request.session["transaction_id"] = transaction_id

    params = {
        "client_id": ORCID_CLIENT_ID,
        "response_type": "code",
        "scope": "/authenticate",
        "redirect_uri": ORCID_REDIRECT_URI,
        "state": transaction_id
    }
    query = urllib.parse.urlencode(params)
    url = f"{ORCID_AUTH_URL}?{query}"

    # Return the transaction ID to the client
    response = JSONResponse({"transaction_id": transaction_id, "auth_url": url})
    logging.info(f"Generated transaction ID: {transaction_id}")
    return response


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str, state: str):
    async with httpx.AsyncClient() as client:
        response = await client.post(ORCID_TOKEN_URL, data={
            "client_id": ORCID_CLIENT_ID,
            "client_secret": ORCID_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": ORCID_REDIRECT_URI,
        })
        token_data = response.json()
        orcid_id = token_data.get("orcid")
        access_token = token_data.get("access_token")

        logging.info(f"Received callback with state: {state}")

        if orcid_id and access_token:
            # Store session data using the transaction ID
            SESSIONS[state] = {
                "orcid_id": orcid_id,
                "authenticated": True,
                "timestamp": datetime.utcnow()
            }
            logging.info(f"Transaction ID {state} authenticated for ORCID ID {orcid_id}")
            return JSONResponse({"message": "Authentication successful", "transaction_id": state})

    return JSONResponse(status_code=403, content={"error": "Authentication failed."})


@app.get("/verify")
async def verify(request: Request):
    transaction_id = request.query_params.get("transaction_id")
    session_data = SESSIONS.get(transaction_id)

    logging.info(f"Verify Endpoint - Transaction ID: {transaction_id}")
    logging.info(f"Session Data: {session_data}")

    if session_data and session_data.get("authenticated"):
        return JSONResponse({"orcid_id": session_data.get("orcid_id"), "authenticated": True})

    return JSONResponse(status_code=403, content={"error": "User not authenticated or session expired."})


@app.post("/ingest")
async def ingest(request: Request):
    transaction_id = request.query_params.get("transaction_id")
    session_data = SESSIONS.get(transaction_id)

    if not session_data:
        return JSONResponse(status_code=401, content={"error": "User not authenticated."})

    orcid_id = session_data.get("orcid_id")

    if orcid_id not in AUTHORIZED_ORCIDS:
        return JSONResponse(status_code=403, content={"error": "Unauthorized ORCID iD."})

    data = await request.json()
    file_path = data.get("file_path")
    thumbnail_path = data.get("thumbnail_path")

    if not file_path or not thumbnail_path:
        return JSONResponse(status_code=400, content={"error": "File path and thumbnail path are required."})

    ensure_minio_bucket()

    if os.path.exists(file_path) and os.path.exists(thumbnail_path):
        try:
            file_url = upload_to_minio(file_path)
            register_in_scicat(file_path, file_url, orcid_id, thumbnail_path)
            return JSONResponse(content={"message": "Data ingestion successful."})
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    return JSONResponse(status_code=400, content={"error": "Invalid file path or format."})


if __name__ == "__main__":
    uvicorn.run("orcid_server:app", host="0.0.0.0", port=8000, reload=True)
