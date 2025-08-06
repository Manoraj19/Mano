import os
import time
import csv
import requests
import logging
import datetime
from dotenv import load_dotenv

load_dotenv()

# Zoho environment setup (same as before)
CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
CREATOR_APP_ID = os.getenv("ZOHO_CREATOR_APP_ID")
APP_LINK_NAME = os.getenv("APP_LINK_NAME")
BASE_URL = "https://creator.zoho.in"
ACCOUNT_OWNER_NAME = os.getenv("ACCOUNT_OWNER_NAME")
LEAD_DATA_FORM = "Lead_Data"  
OAUTH_URL = "https://accounts.zoho.in/oauth/v2/token"

# ── Set up logging ──────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Token management ───────────────────────────────────────────────────────
def get_access_token():
    url = OAUTH_URL
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":"ZohoCreator.form.CREATE,ZohoCreator.form.READ,ZohoCreator.report.CREATE,ZohoCreator.report.READ,ZohoCreator.report.UPDATE"
    }
    try:
        logger.info(f"Fetching access token with client_id: {CLIENT_ID}, client_secret: {CLIENT_SECRET}")
        response = requests.post(url, data=payload)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Token response data: {data}")
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 0)  # Token expiry time in seconds
        expiry_time = time.time() + expires_in  # Calculate when token will expire
        logger.debug(f"Access token retrieved: {access_token}")
        if access_token:
            return access_token, expiry_time
        else:
            logger.error("Access token not found in response.")
            return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching access token: {e}")
        return None, None

_ZOHO_TOKEN: str = None
_ZOHO_EXPIRY: float = 0.0

def get_access_token_with_refresh_token(refresh_token):
    """
    Exchange a long‐lived refresh token for a new access token.
    Returns (access_token, expiry_timestamp).
    """
    try:
        resp = requests.post(
            OAUTH_URL,
            data={
                "grant_type":    "refresh_token",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": refresh_token
            }
        )
        resp.raise_for_status()
        data = resp.json()
        access_token = data["access_token"]
        expires_in   = data.get("expires_in", 3600)
        return access_token, time.time() + expires_in
    except requests.exceptions.RequestException as e:
        logger.error(f"Error refreshing access token: {e}")
        return None, None

def _headers() -> dict:
    """
    Returns headers for Zoho Creator API calls, automatically caching
    the access token until shortly before it expires.
    """
    global _ZOHO_TOKEN, _ZOHO_EXPIRY

    now = time.time()
    if not _ZOHO_TOKEN or now + 60 > _ZOHO_EXPIRY:
        refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
        token, expiry = get_access_token_with_refresh_token(refresh_token)
        if token:
            _ZOHO_TOKEN  = token
            _ZOHO_EXPIRY = expiry
        else:
            logger.error("Failed to retrieve a valid access token.")
            return {}

    return {
        "Authorization": f"Zoho-oauthtoken {_ZOHO_TOKEN}",
        "Content-Type":  "application/json"
    }

# ── Function to create record in Zoho Creator ──────────────────────────────
def create_record(form_name, data):
    url = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{CREATOR_APP_ID}/form/{form_name}"
    headers = _headers()
    if not headers:
        logger.error("Missing authorization headers, skipping record creation.")
        return {"error": "Authorization failed"}
    try:
        response = requests.post(url, headers=headers, json={"data": data})
        response.raise_for_status()
        return response.json()  
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating record in form {form_name}: {e}")
        return {"error": str(e)}

# ── Read CSV and push data to Zoho Creator ───────────────────────────────
def process_csv_and_push_to_zoho(csv_file_path):
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data = {
                    "First_Name": row["First Name"],
                    "Last_Name": row["Last Name"],
                    "Full_Name": row["Full Name"],
                    "Job_Title": row["Job Title"],
                    "Location": row["Location"],
                    "Company_Domain": row["Company Domain"],
                    "LinkedIn_Profile": row["LinkedIn Profile"],
                    "Email": row["Email"],
                    "Date_field": datetime.datetime.now().strftime("%Y-%m-%d")
                }
                # Create record for each row in Zoho Creator
                result = create_record(LEAD_DATA_FORM, data)
                logger.info(f"Created record: {result}")
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")

# # ── Main function to trigger CSV processing ───────────────────────────────
# if __name__ == "__main__":
#     # Path to the CSV file
#     csv_file_path = "C:/Users/muthu/Downloads/lead-2025-06-24.csv"  # Fixed file path issue
#     process_csv_and_push_to_zoho(csv_file_path)
