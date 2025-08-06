import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet


load_dotenv()


INPUT_SHEET_ID    = os.getenv("INPUT_SHEET_ID")
INPUT_SHEET_NAME  = os.getenv("INPUT_SHEET_NAME")

SECRET_KEY                    = os.getenv("SECRET_KEY")



BATCH       = int(os.getenv("BATCH"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
BATCH_DELAY = float(os.getenv("BATCH_DELAY"))

SCRAPE_DELAY    = float(os.getenv("SCRAPE_DELAY"))
PAGE_LOAD_DELAY = float(os.getenv("PAGE_LOAD_DELAY"))
ACTION_DELAY    = float(os.getenv("ACTION_DELAY"))
CONNECTION_DELAY= float(os.getenv("CONNECTION_DELAY"))

SCRAPE_DELAY_MIN    = float(os.getenv("SCRAPE_DELAY_MIN", "2"))
SCRAPE_DELAY_MAX    = float(os.getenv("SCRAPE_DELAY_MAX", "6"))
PAGE_LOAD_DELAY_MIN = float(os.getenv("PAGE_LOAD_DELAY_MIN", "2"))
PAGE_LOAD_DELAY_MAX = float(os.getenv("PAGE_LOAD_DELAY_MAX", "7"))
ACTION_DELAY_MIN    = float(os.getenv("ACTION_DELAY_MIN", "3"))
ACTION_DELAY_MAX    = float(os.getenv("ACTION_DELAY_MAX", "8"))
CONNECTION_DELAY_MIN= float(os.getenv("CONNECTION_DELAY_MIN", "2"))
CONNECTION_DELAY_MAX= float(os.getenv("CONNECTION_DELAY_MAX", "8"))
TYPING_DELAY        =float(os.getenv("TYPING_DELAY"))

JITTER          = float(os.getenv("JITTER", "0.5"))
SCROLL_STEPS_MIN= int(os.getenv("SCROLL_STEPS_MIN", "2"))
SCROLL_STEPS_MAX= int(os.getenv("SCROLL_STEPS_MAX", "6"))
SCROLL_PAUSE    = float(os.getenv("SCROLL_PAUSE", "0.5"))

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_NAME     = os.getenv("DB_NAME", "automation")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DB_CFG = {
    "host":     DB_HOST,
    "port":     DB_PORT,
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "charset":  "utf8mb4"
}

FIELD_MAP = {
    "firstname":   "First Name",
    "lastname":    "Last Name",
    "linkedinurl": "LinkedIn Profile",
    "emaiid":      "Work Email",
    "designation": "Job Title",
    "companyurl":  "Company Domain",
    "location":    "Location"
}

OUTPUT_HEADERS = [
    "First name", "Last name", "US Coast", "Request sent date",
    "Connection date", "Status check date", "Connection Request",
    "Keyword", "Savd Srch Name", "Number",
    "Invite message", "LinkedIn URL", "Emai id", "Designation",
    "About", "No Of Followers", "No Of Connections",
    "Company name", "Company URL", "Country", "Location",
    "Request sent from"
]

COL_MAP = {
    "First name":            "first_name",
    "Last name":             "last_name",
    "US Coast":              "us_coast",
    "Request sent date":     "request_sent_date",
    "Connection date":       "connection_date",
    "Status check date":     "status_check_date",
    "Connection Request":    "connection_request",
    "profile_id":            "profile_id",
    "Keyword":               "keyword",
    "Savd Srch Name":        "saved_search_name",
    "Number":                "number",
    "Invite message":        "invite_message",
    "LinkedIn URL":          "linkedin_url",
    "Emai id":               "email_id",
    "Designation":           "designation",
    "About":                 "about",
    "No Of Followers":       "no_of_followers",
    "No Of Connections":     "no_of_connections",
    "Company name":          "company_name",
    "Company URL":           "company_url",
    "Country":               "country",
    "Location":              "location",
    "Request sent from":     "request_sent_from"
}