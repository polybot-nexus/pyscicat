import os
import json
import requests
import webbrowser
import time

# Configuration
AUTH_SERVER_URL = "http://localhost:8000"
VERIFY_ENDPOINT = f"{AUTH_SERVER_URL}/verify"
LOGIN_ENDPOINT = f"{AUTH_SERVER_URL}/login"
INGESTION_ENDPOINT = f"{AUTH_SERVER_URL}/ingest"

FILE_PATH = "/Users/dozgulbas/scicat/pedot_pss_all_data_set/Train_6_2022-01-25_14-25-53_c0f0998bd8.json"
THUMBNAIL_PATH = "/Users/dozgulbas/scicat/test.png"


def initiate_login() -> str:
    # Check existing transaction ID first
    last_transaction_id = get_transaction_id()
    if last_transaction_id:
        print("Checking existing transaction ID...")
        if verify_authentication(last_transaction_id):
            print("User already authenticated.")
            return last_transaction_id
        print("Existing session expired or invalid. Proceeding with new login.")
    try:
        response = requests.get(LOGIN_ENDPOINT)
        if response.status_code == 200:
            data = response.json()
            transaction_id = data.get("transaction_id")
            auth_url = data.get("auth_url")
            print(f"Transaction ID: {transaction_id}")
            print(f"Open the following URL to authenticate: {auth_url}")
            webbrowser.open(auth_url)
            return transaction_id
    except Exception as e:
        print(f"Error initiating login: {e}")
    return ""


def verify_authentication(transaction_id: str) -> bool:
    try:
        response = requests.get(VERIFY_ENDPOINT, params={"transaction_id": transaction_id})
        print("Verify response status:", response.status_code)
        print("Response data:", response.json())

        if response.status_code == 200:
            verify_data = response.json()
            return verify_data.get("authenticated", False)
    except Exception as e:
        print(f"Error during verification: {e}")
    return False


def wait_for_authentication(transaction_id: str, timeout: int = 60) -> bool:
    elapsed = 0
    while elapsed < timeout:
        authenticated = verify_authentication(transaction_id)
        if authenticated:
            print("User authenticated successfully.")
            return True
        time.sleep(2)
        elapsed += 2
        print(f"Waiting for authentication... ({elapsed}/{timeout}) seconds")
    print("Authentication timed out.")
    return False


# Removed file-based storage; now using in-memory storage for transaction ID
transaction_id_memory = ""

def get_transaction_id() -> str:
    return transaction_id_memory


def set_transaction_id(transaction_id: str):
    global transaction_id_memory
    transaction_id_memory = transaction_id


def save_transaction_id(transaction_id: str):
    try:
        with open("transaction.json", "w") as file:
            json.dump({"transaction_id": transaction_id}, file)
    except Exception as e:
        print(f"Error saving transaction ID: {e}")


def ingest_data(transaction_id: str):
    if not os.path.exists(FILE_PATH) or not os.path.exists(THUMBNAIL_PATH):
        print("File or thumbnail does not exist.")
        return

    try:
        payload = {
            "file_path": FILE_PATH,
            "thumbnail_path": THUMBNAIL_PATH
        }
        response = requests.post(
            INGESTION_ENDPOINT,
            json=payload,
            params={"transaction_id": transaction_id}
        )
        print("Ingest response:", response.json())

    except Exception as e:
        print(f"Error during data ingestion: {e}")


if __name__ == "__main__":
    transaction_id = initiate_login()
    if transaction_id:
        save_transaction_id(transaction_id)
        if wait_for_authentication(transaction_id):
            ingest_data(transaction_id)
        else:
            print("Authentication failed or timed out.")
    else:
        print("Login initiation failed.")
