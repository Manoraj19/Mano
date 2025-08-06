import os
import time
import random
import pickle
import sys
import json
import openai
import logging
import datetime
import re
import requests
import math
from dotenv import load_dotenv
import os

from cryptography.fernet import Fernet
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from itertools import cycle
from multiprocessing.dummy import Pool as ThreadPool
# from app.zoho_api import create_record
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from oauth2client.service_account import ServiceAccountCredentials
from timezonefinder import TimezoneFinder
import multiprocessing
from geopy.geocoders import Nominatim
from zoneinfo import ZoneInfo
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))
from app import config


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TZ = datetime.timezone.utc
tf = TimezoneFinder()
geolocator = Nominatim(user_agent="linkedin_scraper")


SECRET_KEY = config.SECRET_KEY
fernet = Fernet(SECRET_KEY.encode())

BATCH_SIZE = config.BATCH_SIZE  
BATCH_DELAY = config.BATCH_DELAY
BATCH = config.BATCH
SCRAPE_DELAY = config.SCRAPE_DELAY
PAGE_LOAD_DELAY  = config.PAGE_LOAD_DELAY
JITTER = config.JITTER
COOKIES_PATH = "./" 

load_dotenv()

CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
CREATOR_APP_ID = os.getenv("ZOHO_CREATOR_APP_ID")
APP_LINK_NAME = os.getenv("APP_LINK_NAME")
BASE_URL = "https://creator.zoho.in"  # Correct base URL for Zoho India
ACCOUNT_OWNER_NAME = "gogenaisolutionspvtltd"  # Replace with actual account owner name
EDU_REPORT_NAME   = "education_Report"          # if you need those too
EXP_REPORT_NAME   = "experience_Report"
PARENT_REPORT     = "linkedindata_Report"  
# ── OAuth URL ───────────────────────────────────────────────────────────────
OAUTH_URL = "https://accounts.zoho.in/oauth/v2/token"  # Correct URL for token in Zoho India

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
        "scope":"ZohoCreator.form.CREATE,ZohoCreator.form.READ,ZohoCreator.report.CREATE,ZohoCreator.report.READ,ZohoCreator.report.UPDATE"}
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 0)  # Token expiry time in seconds
        expiry_time = time.time() + expires_in  # Calculate when token will expire
        logger.debug(f"Access token retrieved: {access_token}")
        return access_token, expiry_time
    else:
        logger.error(f"Error fetching access token: {response.status_code}, {response.text}")
        return None, None

_ZOHO_TOKEN: str    = None
_ZOHO_EXPIRY: float = 0.0

def get_access_token_with_refresh_token(refresh_token):
    """
    Exchange a long‐lived refresh token for a new access token.
    Returns (access_token, expiry_timestamp).
    """
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

def _headers() -> dict:
    """
    Returns headers for Zoho Creator API calls, automatically caching
    the access token until shortly before it expires.
    """
    global _ZOHO_TOKEN, _ZOHO_EXPIRY

    now = time.time()
    # If missing or about to expire in <60s, refresh it:
    if not _ZOHO_TOKEN or now + 60 > _ZOHO_EXPIRY:
        refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
        token, expiry = get_access_token_with_refresh_token(refresh_token)
        _ZOHO_TOKEN  = token
        _ZOHO_EXPIRY = expiry

    return {
        "Authorization": f"Zoho-oauthtoken {_ZOHO_TOKEN}",
        "Content-Type":  "application/json"
    }


# ── Create Record in Zoho Creator (Form) ────────────────────────────────────
def create_record(form_name, data):
    """Creates a new record in a specified Zoho Creator form."""
    url = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{CREATOR_APP_ID}/form/{form_name}"
    headers = _headers()
    response = requests.post(url, headers=headers, json={"data": data})

    if response.status_code == 200:
        return response.json()  # Return the response JSON containing the created record details
    else:
        log.error(f"Error creating record in form {form_name}: {response.text}")
        return {"error": response.text}

# ── Update Record in Zoho Creator (Report) ──────────────────────────────────
def find_records(report_name, criteria):
    crit = " and ".join(f'{k} == \"{v}\"' for k, v in criteria.items())
    url  = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{APP_LINK_NAME}/report/{report_name}"
    try:
        resp = requests.get(url, headers=_headers(), params={"criteria": f"({crit})"})
        resp.raise_for_status()
        return resp.json().get("data", [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # No matching rows yet
            return []
        raise

def update_record(report_name, record_id, data):
    url  = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{APP_LINK_NAME}/report/{PARENT_REPORT}/{record_id}"
    resp = requests.put(url, headers=_headers(), json={"data": data})
    resp.raise_for_status()
    return resp.json()

def upsert_linkedindata(data, lead_zoho_id=None):
    """
    Update the linkedindata record by Zoho record ID if available; else by profile_id.
    """
    if lead_zoho_id:
        try:
            url = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{APP_LINK_NAME}/report/{PARENT_REPORT}/{lead_zoho_id}"
            resp = requests.put(url, headers=_headers(), json={"data": data})
            resp.raise_for_status()
            log.info(f"Updated by Zoho ID: {lead_zoho_id}")
            return resp.json()
        except Exception as e:
            log.error(f"Error updating LinkedInData by Zoho ID {lead_zoho_id}: {e}")
            return {"error": f"Failed update by Zoho ID {lead_zoho_id}"}
    else:
        profile_id = data.get("profile_id")
        if not profile_id:
            log.error("Profile ID is missing from the record data. Skipping the record.")
            return {"error": "Missing profile_id"}
        existing = find_records(PARENT_REPORT, {"profile_id": profile_id})
        if existing:
            zoho_id = existing[0]["ID"]
            return update_record(PARENT_REPORT, zoho_id, data)
        else:
            url = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{CREATOR_APP_ID}/form/linkedindata"
            resp = requests.post(url, headers=_headers(), json={"data": data})
            if resp.status_code == 200:
                log.info(f"Created new LinkedInData record for profile_id: {profile_id}")
                return resp.json()
            else:
                log.error(f"Error creating record in Zoho: {resp.text}")
                return {"error": "Failed to create record"}


def get_input_urls_from_zoho():
    records = fetch_lead_data()
    result = []
    for r in records:
        linkedin_url = (r.get("LinkedIn_Profile") or "").strip()
        if not linkedin_url:
            continue
        result.append(r)  # just use the full dict
    return result

def fetch_lead_data():
    """
    Fetch records from Zoho Creator Lead_Data report.
    Returns a list of dicts, each representing a lead record.
    """
    url = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{APP_LINK_NAME}/report/Lead_Data_Report"
    try:
        resp = requests.get(url, headers=_headers())
        resp.raise_for_status()
        data = resp.json().get("data", [])
        # Filter only rows with LinkedIn Profile URL present
        records = [row for row in data if row.get("LinkedIn_Profile", "").strip()]
        return records
    except Exception as e:
        log.error(f"Error fetching Lead_Data from Zoho: {e}")
        return []

def dedupe_input_records(records):
    """Deduplicate on the normalized LinkedIn URL, but keep the full record dict."""
    seen = set()
    deduped = []
    for rec in records:
        url = rec.get("LinkedIn_Profile", "").strip()
        norm = extract_base_linkedin_url(url).lower().rstrip("/")
        if norm and norm not in seen:
            seen.add(norm)
            deduped.append(rec)
    return deduped

def init_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-notifications")
    # opts.add_argument("--headless")  
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    return webdriver.Chrome(options=opts)

def human_typing(element, text, min_delay=1.0, max_delay=2.5):
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(min_delay, max_delay))
        
        
def load_cookies(driver, account):
    cookie_file = account["cookie_file"]
    if os.path.exists(cookie_file):
        log.info(f"Loading cookies from {cookie_file}")
        with open(cookie_file, "rb") as f:
            cookies = pickle.load(f)
        
        # Load LinkedIn URL before adding cookies
        driver.get("https://www.linkedin.com")
        log.info(f"Successfully navigated to LinkedIn for {account['username']}")

        for cookie in cookies:
            # Ensure cookie domain matches LinkedIn's domain
            if cookie.get("domain") and "linkedin.com" not in cookie["domain"]:
                cookie["domain"] = "linkedin.com"
            if cookie.get("sameSite") == "None":
                cookie["sameSite"] = "Strict"
            driver.add_cookie(cookie)

        driver.get("https://www.linkedin.com/feed/")  # Go to the feed to verify if cookies work
        log.info(f"Successfully loaded cookies for {account['username']}")
    else:
        log.warning(f"No cookies found for {account['username']}, logging in.")
        login_and_save_cookies(driver, account, cookie_file)


def login_and_save_cookies(driver, account, cookie_file):
    """Login to LinkedIn and save cookies."""
    driver.get("https://www.linkedin.com/login")
    wait = WebDriverWait(driver, 15)
    username_el = wait.until(EC.presence_of_element_located((By.ID, "username")))

    username_el.clear()
    password_el = driver.find_element(By.ID, "password")
    password_el.clear()
    human_typing(username_el, account["username"])
    human_typing(password_el, account["password"])

    signin_button = driver.find_element(By.XPATH, "//button[@type='submit']")
    signin_button.click()

    wait.until(lambda d: "feed" in d.current_url or "checkpoint" in d.current_url)

    cookies = driver.get_cookies()
    with open(cookie_file, "wb") as f:
        pickle.dump(cookies, f)
    log.info(f"Login successful for {account['username']}, cookies saved to {cookie_file}")


def load_accounts():
    """
    Dynamically loads LinkedIn account credentials from environment variables.
    Looks for any keys that match the pattern LINKEDIN_USERNAME_ACCOUNT<n> and LINKEDIN_PASSWORD_ACCOUNT<n>.
    Returns a list of accounts with decrypted credentials.
    """
    accounts = []
    i = 1
    while True:
        uname_key = f"LINKEDIN_USERNAME_ACCOUNT{i}"
        pwd_key = f"LINKEDIN_PASSWORD_ACCOUNT{i}"
        if os.getenv(uname_key) and os.getenv(pwd_key):
            try:
                username = fernet.decrypt(os.getenv(uname_key).encode()).decode()
                password = fernet.decrypt(os.getenv(pwd_key).encode()).decode()
                cookie_file = os.path.join(COOKIES_PATH, f"cookies_account{i}.pkl")
                accounts.append({
                    "username": username,
                    "password": password,
                    "cookie_file": cookie_file
                })
                log.info(f"Loaded credentials for account {i}")
            except Exception as e:
                log.error(f"Error decrypting credentials for account {i}: {e}")
                break
            i += 1
        else:
            break
    return accounts

def scroll_like_human(driver):
    for _ in range(random.randint(config.SCROLL_STEPS_MIN, config.SCROLL_STEPS_MAX)):
        driver.execute_script(f"window.scrollBy(0,{random.randint(250,600)});")
        time.sleep(config.SCROLL_PAUSE + random.uniform(0, config.JITTER/2))
    driver.execute_script("window.scrollBy(0,-300);")

def nap(*args):
    if len(args) == 1:
        random_delay(args[0], config.JITTER)
    elif len(args) == 2:
        random_delay(random.uniform(args[0], args[1]), config.JITTER)

def random_delay(base, jitter=config.JITTER, min_delay=1, max_delay=None):
    d = base + random.uniform(0, jitter)
    if max_delay:
        d = min(d, max_delay)
    time.sleep(max(d, min_delay))

def safe_find(node, by, expr):
    try: return node.find_element(by, expr)
    except Exception: return None

def scrape_profile(driver, wait):
    info = {}
    nap(config.SCRAPE_DELAY_MIN, config.SCRAPE_DELAY_MAX)
    scroll_like_human(driver)

    head = safe_find(driver, By.CSS_SELECTOR, "div.mt2.relative div.text-body-medium.break-words")
    info["designation"] = head.text.strip() if head else ""
    comp = None
    if head:
        comp = safe_find(head, By.CSS_SELECTOR, "a[href*='/company/'],a[href*='/school/']")
    if not comp:
        comp = safe_find(driver, By.XPATH, "//section[contains(@id,'experience')]//a[contains(@href,'/company/')][1]")
    info["company_name"] = comp.text.strip() if comp else ""
    loc = safe_find(driver, By.CSS_SELECTOR, "span.text-body-small.inline.t-black--light.break-words")
    info["location"] = loc.text.strip() if loc else ""
    
    info["followers"] = "0"
    elems = driver.find_elements(By.XPATH, "//p[contains(translate(., 'FOLLOWERS','followers'), 'followers')]")
    for el in elems:
        text = el.text.strip()
        m = re.search(r"([\d,]+\+?)\s*followers", text, re.IGNORECASE)
        if m:
            info["followers"] = m.group(1)
            break
    info["connections"] = "0"
    elems = driver.find_elements(By.XPATH, "//li[contains(translate(., 'CONNECTIONS','connections'), 'connections')]")
    for el in elems:
        text = el.text.strip()
        m = re.search(r"([\d,]+\+?)\s*connections", text, re.IGNORECASE)
        if m:
            info["connections"] = m.group(1)
            break
        
    info["email"] = ""
    info["connected_on"] = ""
    link = safe_find(driver, By.ID, "top-card-text-details-contact-info")
    if link:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
        nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
        try:
            driver.execute_script("arguments[0].click();", link)
        except ElementClickInterceptedException:
            driver.execute_script("window.scrollBy(0,-100);")
            nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
            driver.execute_script("arguments[0].click();", link)
        nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
        modal = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.artdeco-modal__content")))
        for sec in modal.find_elements(By.CSS_SELECTOR, "section.pv-contact-info__contact-type"):
            title = (safe_find(sec, By.CSS_SELECTOR, "h3") or "").text.lower()
            if "email" in title:
                m = safe_find(sec, By.CSS_SELECTOR, "a[href^='mailto:']")
                if m:
                    info["email"] = m.text.strip()
            elif "connected" in title:
                s = safe_find(sec, By.CSS_SELECTOR, "span")
                if s:
                    info["connected_on"] = s.text.strip()
        btn = safe_find(driver, By.CSS_SELECTOR, "button.artdeco-modal__dismiss")
        if btn:
            btn.click()
            nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
    return info

def scrape_education(driver, wait):
    edu_list = []
    try:
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "section.artdeco-card li.pvs-list__paged-list-item")
        ))
        edu_items = driver.find_elements(By.CSS_SELECTOR, "section.artdeco-card li.pvs-list__paged-list-item")
        for item in edu_items:
            edu = {}
            try:
                school = item.find_element(By.CSS_SELECTOR, "span.t-bold[aria-hidden='true']").text.strip()
            except Exception:
                school = ""
            edu['school'] = school
            try:
                degree = item.find_element(By.CSS_SELECTOR, "span.t-14.t-normal span[aria-hidden='true']").text.strip()
            except Exception:
                degree = ""
            edu['degree'] = degree
            try:
                dates = item.find_element(By.CSS_SELECTOR, "span.t-14.t-normal.t-black--light span.pvs-entity__caption-wrapper[aria-hidden='true']").text.strip()
            except Exception:
                dates = ""
            edu['dates'] = dates
            desc = []
            try:
                desc_spans = item.find_elements(By.CSS_SELECTOR, "div.t-14.t-normal.t-black span[aria-hidden='true']")
                for d in desc_spans:
                    text = d.text.strip()
                    if text and text not in [school, degree, dates]:
                        desc.append(text)
            except Exception:
                pass
            if not desc:
                try:
                    desc_spans = item.find_elements(By.CSS_SELECTOR, "span[aria-hidden='true']")
                    for d in desc_spans:
                        text = d.text.strip()
                        if text and text not in [school, degree, dates]:
                            desc.append(text)
                except Exception:
                    pass
            edu['description'] = " | ".join(desc)
            edu_list.append(edu)
    except Exception as e:
        print(f"[WARN] Error scraping education: {e}")
    return edu_list

def scrape_experience(driver, wait):
    exp_points = []
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "section.artdeco-card")))
        exp_items = driver.find_elements(By.CSS_SELECTOR, "li.pvs-list__paged-list-item")
        for item in exp_items:
            try:
                title_el = item.find_element(By.CSS_SELECTOR, "div.display-flex.align-items-center.mr1.hoverable-link-text.t-bold span[aria-hidden='true']")
                title = title_el.text.strip() if title_el else ""
                spans = item.find_elements(By.CSS_SELECTOR, "span.t-14.t-normal span[aria-hidden='true']")
                company = spans[0].text.strip() if spans else ""
                employment_type = spans[1].text.strip() if len(spans) > 1 else ""
                date_el = item.find_element(By.CSS_SELECTOR, "span.t-14.t-normal.t-black--light .pvs-entity__caption-wrapper[aria-hidden='true']")
                date_range = date_el.text.strip() if date_el else ""
                try:
                    loc_el = item.find_elements(By.CSS_SELECTOR, "span.t-14.t-normal.t-black--light span[aria-hidden='true']")
                    location = ""
                    if len(loc_el) > 1:
                        location = loc_el[-1].text.strip()
                except Exception:
                    location = ""
                bullets = []
                try:
                    desc_els = item.find_elements(By.CSS_SELECTOR, "div.display-flex.align-items-center.t-14.t-normal.t-black span[aria-hidden='true']")
                    for desc in desc_els:
                        text = desc.text.strip()
                        if text and (text.startswith("•") or text.startswith("-")):
                            bullets.extend([b.strip() for b in text.split("\n") if b.strip()])
                except Exception:
                    pass
                if not bullets:
                    all_spans = item.find_elements(By.CSS_SELECTOR, "span[aria-hidden='true']")
                    for sp in all_spans:
                        text = sp.text.strip()
                        if text.startswith("•") or text.startswith("-"):
                            bullets.extend([b.strip() for b in text.split("\n") if b.strip()])
                header = f"{title}{', ' + company if company else ''}"
                if employment_type:
                    header += f" · {employment_type}"
                if date_range:
                    header += f" ({date_range})"
                if location:
                    header += f" | {location}"
                exp_lines = [header] + bullets if bullets else [header]
                exp_points.append('\n'.join([l for l in exp_lines if l.strip()]))
            except Exception:
                continue
    except Exception as e:
        log.warning(f"Error scraping experience: {e}")
    return exp_points


def scrape_about(driver):
    try:
        about_anchor = driver.find_element(By.ID, "about")
        driver.execute_script("arguments[0].scrollIntoView(true);", about_anchor)
        time.sleep(1)
        about_card = about_anchor.find_element(By.XPATH, "../../..")
        about_text_el = about_card.find_element(By.CSS_SELECTOR, "div.display-flex.ph5.pv3 span[aria-hidden='true']")
        about_text = about_text_el.get_attribute("innerText").strip()
        if not about_text:
            all_spans = about_card.find_elements(By.CSS_SELECTOR, "span[aria-hidden='true']")
            about_text = "\n".join(
                s.get_attribute("innerText").strip() for s in all_spans if s.text.strip()
            )

        return about_text
    except Exception as e:
        print("no about found",e)
        return ""

def extract_linkedin_id(url):
    """Extracts LinkedIn profile ID from the given URL."""
    url = url.strip().split("?")[0] 
    log.info(f"Processing LinkedIn URL: {url}")  # Log the URL being processed

    # Updated regex to extract the profile ID after `/in/`
    m = re.match(r"https://www\.linkedin\.com/in/([^/?#]+)/?", url)
    
    if m:
        return m.group(1)  # Return the profile ID (after /in/)
    else:
        log.error(f"Invalid LinkedIn URL format: {url}")  # Log if URL is not in expected format
        return None  # Return None if the URL doesn't match the expected format


def extract_base_linkedin_url(url: str) -> str:
    url = url.strip()
    m = re.match(r"(https://www\.linkedin\.com/in/[^/?#]+)", url)
    return (m.group(1) + "/") if m else url


# def get_sheet_col_indices(sheet):
#     header_row = sheet.row_values(1)
#     col_map = {c.lower(): i+1 for i, c in enumerate(header_row)}
#     def get_or_add(col_name, default_label):
#         idx = col_map.get(col_name)
#         if not idx:
#             sheet.update_cell(1, len(header_row)+1, default_label)
#             return len(header_row)+1
#         return idx
#     return {
#         "status_col": get_or_add("status", "Status"),
#         "connection_col": get_or_add("connection", "Connection"),
#         "date_col": get_or_add("date", "Date")
#     }
    



def extract_country(location_str):
    if not location_str or not isinstance(location_str, str):
        return ""
    parts = [x.strip() for x in location_str.split(",") if x.strip()]
    return parts[-1] if parts else location_str.strip()


def is_linkedin_404(driver):
    try:
        title = driver.title.lower()
        if any(txt in title for txt in ["page not found", "doesn’t exist", "not found", "profile not found"]): return True
        error_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'This page doesn’t exist') or contains(text(), 'Page not found') or contains(text(), 'Profile Not Found')]")
        if error_elements: return True
        if not driver.find_elements(By.CSS_SELECTOR, "div.mt2.relative h1"): return True
        return False
    except Exception: return True
    

def handle_connection(driver, wait, note, record):
    try:
        sec = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//section[contains(@class,'artdeco-card') and @data-member-id]")))

        scroll_like_human(driver)
        prev_connection_val = (record.get('connection_request', '').strip().lower())
        if prev_connection_val in ["already connected", "decline invitation"]:
            log.info(f"Skipping {record['profile_id']} because the connection request is already handled (status: {prev_connection_val})")
            return prev_connection_val.capitalize(), False  
        try:
            connect = safe_find(sec, By.XPATH, ".//button[.//span[text()='Connect']]")
            if connect:
                if prev_connection_val in ["pending", "request sent with note", "request sent without note"]:
                    # Instead of calling upsert_connection_data, just log the action
                    log.info(f"Declining invitation for profile_id {record['profile_id']}")
                    return "Decline Invitation", False

                driver.execute_script("arguments[0].scrollIntoView(true);", connect)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", connect)
                nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)

                try:
                    note_btn = wait.until(EC.element_to_be_clickable(
                        (By.XPATH, "//button[contains(@aria-label,'Add a note')]")))
                    driver.execute_script("arguments[0].click();", note_btn)
                    nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                    msg_area = wait.until(EC.presence_of_element_located(
                        (By.XPATH, "//textarea[@name='message']")))
                    msg_area.send_keys(note)
                    nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                    send_btn = wait.until(EC.element_to_be_clickable(
                        (By.XPATH, "//button[contains(@aria-label,'Send')]")))

                    driver.execute_script("arguments[0].click();", send_btn)
                    nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)

                    # Log the action instead of upserting to Zoho
                    log.info(f"Request Sent with Note for profile_id {record['profile_id']}")
                    return "Request Sent with Note", True
                except (TimeoutException, NoSuchElementException):
                    try:
                        upsell = safe_find(driver, By.CSS_SELECTOR, "div.artdeco-modal.modal-upsell")
                        if upsell:
                            dismiss_btn = safe_find(upsell, By.CSS_SELECTOR, "button.artdeco-modal__dismiss")
                            if dismiss_btn:
                                driver.execute_script("arguments[0].click();", dismiss_btn)
                                nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                                driver.execute_script("arguments[0].click();", connect)
                                nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                                skip_note = wait.until(EC.element_to_be_clickable(
                                    (By.XPATH, "//button[contains(@aria-label,'Send without a note')]")))
                                driver.execute_script("arguments[0].click();", skip_note)
                                nap(config.CONNECTION_DELAY_MIN, config.CONNECTION_DELAY_MAX)

                                # Log the action instead of upserting to Zoho
                                log.info(f"Request Sent without Note for profile_id {record['profile_id']}")
                                return "Request Sent without Note", True
                        # Log the blocked action instead of upserting to Zoho
                        log.info(f"Request Blocked for profile_id {record['profile_id']}")
                        return "Request Blocked", False
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            more_btn = safe_find(sec, By.XPATH, ".//button[@aria-label='More actions']")
            if more_btn:
                driver.execute_script("arguments[0].click();", more_btn)
                nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                dropdown_items = driver.find_elements(By.CSS_SELECTOR, "div.artdeco-dropdown__item[role='button']")
                connect = None
                for item in dropdown_items:
                    try:
                        span = safe_find(item, By.XPATH, ".//span")
                        if span and span.text.strip().lower() == "connect":
                            connect = item
                            break
                    except Exception:
                        pass
                if connect:
                    if prev_connection_val in ["pending", "request sent with note", "request sent without note"]:
                        # Log the action instead of upserting to Zoho
                        log.info(f"Declining invitation for profile_id {record['profile_id']}")
                        return "Decline Invitation", False
                    driver.execute_script("arguments[0].scrollIntoView(true);", connect)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", connect)
                    nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                    try:
                        note_btn = wait.until(EC.element_to_be_clickable(
                            (By.XPATH, "//button[contains(@aria-label,'Add a note')]")))
                        driver.execute_script("arguments[0].click();", note_btn)
                        nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                        msg_area = wait.until(EC.presence_of_element_located(
                            (By.XPATH, "//textarea[@name='message']")))
                        msg_area.send_keys(note)
                        nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                        send_btn = wait.until(EC.element_to_be_clickable(
                            (By.XPATH, "//button[contains(@aria-label,'Send')]")))
                        driver.execute_script("arguments[0].click();", send_btn)
                        nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)

                        # Log the action instead of upserting to Zoho
                        log.info(f"Request Sent with Note for profile_id {record['profile_id']}")
                        return "Request Sent with Note", True
                    except (TimeoutException, NoSuchElementException):
                        try:
                            upsell = safe_find(driver, By.CSS_SELECTOR, "div.artdeco-modal.modal-upsell")
                            if upsell:
                                dismiss_btn = safe_find(upsell, By.CSS_SELECTOR, "button.artdeco-modal__dismiss")
                                if dismiss_btn:
                                    driver.execute_script("arguments[0].click();", dismiss_btn)
                                    nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                                    driver.execute_script("arguments[0].click();", connect)
                                    nap(config.ACTION_DELAY_MIN, config.ACTION_DELAY_MAX)
                                    skip_note = wait.until(EC.element_to_be_clickable(
                                        (By.XPATH, "//button[contains(@aria-label,'Send without a note')]")))
                                    driver.execute_script("arguments[0].click();", skip_note)
                                    nap(config.CONNECTION_DELAY_MIN, config.CONNECTION_DELAY_MAX)

                                    # Log the action instead of upserting to Zoho
                                    log.info(f"Request Sent without Note for profile_id {record['profile_id']}")
                                    return "Request Sent without Note", True
                        except Exception:
                            pass
        except Exception:
            pass

        # Handle the "Pending" and "Already Connected" statuses
        try:
            if safe_find(sec, By.XPATH, ".//span[text()='Pending']"):
                # Log the action instead of upserting to Zoho
                log.info(f"Pending status for profile_id {record['profile_id']}")
                return "Pending", False
        except Exception:
            pass
        try:
            if safe_find(sec, By.XPATH, ".//span[text()='Message']"):
                # Log the action instead of upserting to Zoho
                log.info(f"Already Connected for profile_id {record['profile_id']}")
                return "Already Connected", False
        except Exception:
            pass

        # Log invalid action instead of upserting to Zoho
        log.info(f"No Connect Option for profile_id {record['profile_id']}")
        return "No Connect Option", False

    except TimeoutException:
        # Log the timeout action instead of upserting to Zoho
        log.info(f"Timeout for profile_id {record['profile_id']}")
        return "Connection Element Not Found", False
    except Exception as e:
        # Log unexpected errors instead of upserting to Zoho
        log.error(f"Unexpected error in handle_connection for profile_id {record['profile_id']}: {e}")
        return "Error", False


with open("scripts/messages.json", "r", encoding="utf-8") as f:
    message_data = json.load(f)
keywords = message_data.get("keywords", {})
HIGH_KEYWORDS = keywords.get("high", [])
MID_KEYWORDS = keywords.get("mid", [])
LOW_KEYWORDS = keywords.get("low", [])

def generate_message(fn, ln, des, comp=""):
    lvl = (
        "high" if any(k in des.lower() for k in HIGH_KEYWORDS)
        else "mid" if any(k in des.lower() for k in MID_KEYWORDS)
        else "low"
    )
    tone = "visionary and strategic" if lvl == "high" else "collaborative and practical"
    name = f"{fn} {ln}".strip() or "Professional"
    prompt = (
        f"Write a concise, 1-line LinkedIn connection message for {name}, "
        f"a '{des}'{f' at {comp}' if comp else ''}. "
        f"Make the tone {tone}, professional and respectful."
    )
    msgs = [{"role": "system", "content": "You are a LinkedIn assistant."}, {"role": "user", "content": prompt}]
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo", messages=msgs, temperature=0.7, max_tokens=80
        )
        return resp.choices[0].message.content.strip()
    except:
        fb = message_data.get(lvl, [])
        return random.choice(fb) if fb else "Hello, let's connect!"


def get_timezone_from_location(loc):
    if not loc:
        return TZ
    try:
        p = geolocator.geocode(loc)
        tz = tf.timezone_at(lng=p.longitude, lat=p.latitude) if p else None
        return ZoneInfo(tz) if tz else TZ
    except:
        return TZ

def compute_connection_dates(
    sent_status, data, profile_tz=TZ, previous_connection_date=""
):
    """
    Returns: (connection_date, connected_on)
    """
    now_dt = datetime.datetime.now(profile_tz)
    today_full = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    today_human = now_dt.strftime("%B %d, %Y")

    sent_status = (sent_status or "").lower()
    if sent_status == "pending":
        connection_date = previous_connection_date or ""
        connected_on = data.get("connected_on") or today_human
    elif sent_status in ["request sent with note", "request sent without note"]:
        connection_date = today_full
        connected_on = today_human + " (Today)"
    else:
        connection_date = previous_connection_date or ""
        connected_on = data.get("connected_on") or ""
    return connection_date, connected_on

def flatten_value(val):
    if isinstance(val, (list, tuple)):
        return ', '.join(str(v) for v in val)
    if isinstance(val, dict):
        return json.dumps(val)
    return val

def process_batch(args):
    batch_index, batch_urls, account = args
    log.info(f" [Batch {batch_index}] Using account: {account['username']}")
    
    driver = init_driver()
    wait = WebDriverWait(driver, 15)

    # Load cookies and login once before starting the batch processing
    load_cookies(driver, account)
    main_handle = driver.current_window_handle

    # Process each record in the batch
    for record in batch_urls:
        try:
            linkedin_url = record.get("LinkedIn_Profile", "").strip()
            if not linkedin_url or ' ' in linkedin_url:
                log.info(f"[Record {record.get('ID')}] Invalid LinkedIn URL")
                continue

            profile_id = extract_linkedin_id(linkedin_url)
            log.info(f"[Record {record.get('ID')}] Extracted profile_id: {profile_id} for URL: {linkedin_url}")
            if not profile_id:
                log.error(f"[Record {record.get('ID')}] Unable to extract profile_id from URL: {linkedin_url}")
                continue
            record['profile_id'] = profile_id

            # Fetch necessary details for the record
            first_name = record.get("First_Name", "").strip()
            last_name = record.get("Last_Name", "").strip()
            job_title = record.get("Job_Title", "").strip()
            company_domain = record.get("Company_Domain", "").strip()
            email = record.get("Email", "").strip()
            location = record.get("Location", "").strip()
            note = generate_message(first_name, last_name, job_title, company_domain)

            driver.get(linkedin_url)
            time.sleep(SCRAPE_DELAY)

            if is_linkedin_404(driver):
                log.info(f"[Record {record.get('ID')}] Profile not found for {linkedin_url}")
                continue

            status, sent = handle_connection(driver, wait, note, record)

            today_str = datetime.date.today().strftime('%Y-%m-%d')
            profile_tz = get_timezone_from_location(record.get('Location', ''))
            now_local = datetime.datetime.now(profile_tz)
            today_human = now_local.strftime('%Y-%m-%d')
            lead_zoho_id = record.get('ID')  # Always present if fetched from Zoho

            # --- Handle Pending Status ---
            if status.lower() == "pending":
                date_field = record.get('date_field', today_human).split()[0]
                try:
                    parsed = datetime.datetime.strptime(date_field, '%Y-%m-%d').date()
                except:
                    parsed = now_local.date()
                diff = (now_local.date() - parsed).days
                suffix = '(Today)' if diff == 0 else '(1 day ago)' if diff == 1 else f'({diff} days ago)'
                human_dt = f'{parsed.isoformat()} {suffix}'

                payload = {
                    'profile_id': profile_id,
                    'connection_request': 'Pending',
                    'connection_date': human_dt,
                    'status_check_date': today_human
                }
                resp = upsert_linkedindata(payload, lead_zoho_id=lead_zoho_id)
                log.info(f"[Record {lead_zoho_id}] Pending status, connection date: {human_dt} | Response: {resp}")
                random_delay(JITTER)
                continue

            # --- Handle Already Connected Status ---
            elif status.lower() == 'already connected':
                prof = scrape_profile(driver, wait)
                raw = prof.get('connected_on', '')
                payload = {
                    'profile_id': profile_id,
                    'connection_request': 'Already Connected',
                    'connection_date': raw
                }
                resp = upsert_linkedindata(payload, lead_zoho_id=lead_zoho_id)
                log.info(f"[Record {lead_zoho_id}] Already Connected, connection date: {raw} | Response: {resp}")
                random_delay(JITTER)
                continue

            # --- Handle Other Statuses (Request Sent with/without Note) ---
            else:
                conn_dt, _ = compute_connection_dates(status, record, profile_tz, '')
                base = conn_dt.split()[0] if conn_dt else today_human
                try:
                    parsed = datetime.datetime.strptime(base, '%Y-%m-%d').date()
                except:
                    parsed = now_local.date()
                diff = (now_local.date() - parsed).days
                suffix = '(Today)' if diff == 0 else '(1 day ago)' if diff == 1 else f'({diff} days ago)'
                human_conn_date = f'{parsed.isoformat()} {suffix}'

                prof = scrape_profile(driver, wait)
                about = scrape_about(driver)
                normalized_status = status.strip().title()

                profile_payload = {
                    'first_name': record.get('First_Name', '') or prof.get('designation', ''),
                    'last_name': record.get('Last_Name', ''),
                    'request_sent_date': today_human,
                    'connection_date': human_conn_date,
                    'status_check_date': today_human,
                    'connection_request': normalized_status,
                    'profile_id': profile_id,
                    'invite_message': note,
                    'request_sent_from': account['username'],
                    'linkedin_url': linkedin_url,
                    'email_id': record.get('Email', '') or prof.get('email', ''),
                    'designation': record.get('Job_Title', '') or prof.get('designation', ''),
                    'about': about,
                    'no_of_followers': prof.get('followers', ''),
                    'no_of_connections': prof.get('connections', ''),
                    'company_name': record.get('Company_Domain', '') or prof.get('company_name', ''),
                    'country': extract_country(record.get('Location', '') or prof.get('location', '')),
                    'location': record.get('Location', '') or prof.get('location', '')
                }

                # --- Find or create parent ---
                existing_records = find_records(PARENT_REPORT, {"profile_id": profile_id})
                if existing_records:
                    zoho_record_id = existing_records[0].get("ID")
                    log.info(f"LinkedInData already exists for profile_id {profile_id} with ID {zoho_record_id}")
                else:
                    parent_resp = create_record("linkedindata", profile_payload)
                    data_field = parent_resp.get("data")
                    if isinstance(data_field, dict):
                        zoho_record_id = data_field.get("ID")
                    elif isinstance(data_field, list) and data_field:
                        zoho_record_id = data_field[0].get("ID")
                    else:
                        raise RuntimeError(f"Couldn’t get Zoho record ID back: {parent_resp}")
                    log.info(f"Created LinkedInData for profile_id {profile_id} with ID {zoho_record_id}")

                # --- Create experience/education child records, link to parent by Zoho ID ---
                base_url = extract_base_linkedin_url(linkedin_url)
                driver.get(f"{base_url.rstrip('/')}/details/experience/")
                random_delay(SCRAPE_DELAY)
                exp_list = scrape_experience(driver, wait)
                if exp_list:
                    for idx, exp_text in enumerate(exp_list, start=1):
                        exp_payload = {
                            "linkedindata":     [zoho_record_id],      # Link to parent via lookup (Zoho ID)
                            "linkedin_url":     linkedin_url,
                            "sections":         f"{profile_id}_exp_{idx}",
                            "experience_entry": exp_text,
                        }
                        exp_resp = create_record("experience", exp_payload)
                        log.info(f"[Record {lead_zoho_id}] Created experience {idx}/{len(exp_list)} → {exp_resp}")
                else:
                    log.info(f"[Record {lead_zoho_id}] No experience entries found; skipping.")

                random_delay(PAGE_LOAD_DELAY)
                driver.get(f"{base_url.rstrip('/')}/details/education/")
                random_delay(SCRAPE_DELAY)
                edu_list = scrape_education(driver, wait)
                if edu_list:
                    for idx, edu in enumerate(edu_list, start=1):
                        entry_text = " | ".join(filter(None, [
                            edu.get("school", "").strip(),
                            edu.get("degree", "").strip(),
                            edu.get("dates", "").strip(),
                            edu.get("description", "").strip(),
                        ]))
                        edu_payload = {
                            "linkedindata":    [zoho_record_id],      # Link to parent via lookup (Zoho ID)
                            "linkedin_url":    linkedin_url,
                            "sections":        f"{profile_id}_edu_{idx}",
                            "education_entry": entry_text,
                        }
                        edu_resp = create_record("education", edu_payload)
                        log.info(f"[Record {lead_zoho_id}] Created education {idx}/{len(edu_list)} → {edu_resp}")
                else:
                    log.info(f"[Record {lead_zoho_id}] No education entries found; skipping.")

                random_delay(PAGE_LOAD_DELAY)

            # Cleanup
            driver.close()
            driver.switch_to.window(main_handle)
            random_delay(JITTER)

        except Exception as e:
            log.error(f"[Record {record.get('ID')}] Error: {e}")
            continue


def process_urls_for_account(args):
    acc_idx, account, assigned_urls = args
    log.info(f"[{account['username']}] Processing {len(assigned_urls)} LinkedIn URLs.")
    process_batch((1, assigned_urls, account))  # Process all URLs for this account
    log.info(f"[{account['username']}] All URLs processed.")


def chunk_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    time.sleep(8)
    accounts = load_accounts()
    if not accounts:
        log.error("No LinkedIn accounts configured.")
        return

    records = get_input_urls_from_zoho()
    # 2) dedupe them
    records = dedupe_input_records(records)
    if not records:
        log.warning("No LinkedIn URLs found in Zoho.")
        return

    # 3) round-robin assign to your N accounts
    num = len(accounts)
    assigned = [[] for _ in range(num)]
    for i, rec in enumerate(records):
        assigned[i % num].append(rec)

    group_args = [(i, accounts[i], assigned[i]) for i in range(num)]
    log.info("Assigning %d URLs to %d accounts", len(records), num)

    # 4) process in parallel
    with multiprocessing.Pool(processes=num) as pool:
        pool.map(process_urls_for_account, group_args)

    log.info("All done.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Process interrupted by user.")
        sys.exit(0)