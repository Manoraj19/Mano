import os
import time
import random
import pickle
import sys
import gspread
import json
import openai
import logging
import datetime
import re
import requests
import math
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from selenium import webdriver
from multiprocessing.dummy import Pool as ThreadPool
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from itertools import cycle
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from oauth2client.service_account import ServiceAccountCredentials
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from zoneinfo import ZoneInfo
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))
from app import config


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TZ = datetime.timezone.utc
tf = TimezoneFinder()
geolocator = Nominatim(user_agent="linkedin_scraper")

load_dotenv()
SECRET_KEY = config.SECRET_KEY
fernet = Fernet(SECRET_KEY.encode())

BATCH_SIZE = config.BATCH_SIZE  
BATCH_DELAY = config.BATCH_DELAY
BATCH = config.BATCH
SCRAPE_DELAY = config.SCRAPE_DELAY
PAGE_LOAD_DELAY  = config.PAGE_LOAD_DELAY
JITTER = config.JITTER
INPUT_SHEET_ID = config.INPUT_SHEET_ID
INPUT_SHEET_NAME = config.INPUT_SHEET_NAME
COOKIES_PATH = "./" 

SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(config.GOOGLE_APPLICATION_CREDENTIALS, SCOPES)
gclient = gspread.authorize(creds)

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

def upsert_linkedindata(data):
    """
    If a linkedindata record exists, update it; otherwise create a new one.
    """
    existing = find_records(PARENT_REPORT, {"profile_id": data["profile_id"]})
    if existing:
        zoho_id = existing[0]["ID"]
        return update_record(PARENT_REPORT, zoho_id, data)
    else:
        # No existing record → create a fresh one
        url  = f"{BASE_URL}/api/v2/{ACCOUNT_OWNER_NAME}/{CREATOR_APP_ID}/form/linkedindata"
        resp = requests.post(url, headers=_headers(), json={"data": data})
        resp.raise_for_status()
        return resp.json()
    


def dedupe_input_urls(url_tuples):
    seen = set()
    deduped = []
    for row_num, url in url_tuples:
        norm = extract_base_linkedin_url(url).lower().rstrip("/")
        if norm not in seen:
            seen.add(norm)
            deduped.append((row_num, url))
    return deduped


def get_input_urls():
    sheet = gclient.open_by_key(INPUT_SHEET_ID).worksheet(INPUT_SHEET_NAME)
    data = sheet.get_all_records()
    header_row = sheet.row_values(1)
    try:
        conn_idx = next(i for i, col in enumerate(header_row) if col.strip().lower() == 'connection')
    except StopIteration:
        conn_idx = None

    urls = []
    seen_urls = set()
    for i, row in enumerate(data):
        linkedin_url = (row.get("LinkedIn Profile") or "").strip()
        if not linkedin_url:
            continue

        base_url = extract_base_linkedin_url(linkedin_url).lower()
        if base_url in seen_urls:
        
            if conn_idx is not None:
                sheet.update_cell(i + 2, conn_idx + 1, "duplicate")
            continue
        seen_urls.add(base_url)

        status = ""
        if conn_idx is not None:
            status = (row.get(header_row[conn_idx], "") or "").strip().lower()
        if status in ["already connected", "decline invitation", "duplicate"]:
            continue
        urls.append((i + 2, linkedin_url))
    return urls

def init_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-notifications")
    # opts.add_argument("--headless")  
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    return webdriver.Chrome(options=opts)

def human_typing(element, text, min_delay=0.5, max_delay=0.9):
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
    url = url.strip().split("?")[0]
    m = re.match(r"https://www\.linkedin\.com/in/([^/?#]+)/?", url)
    return m.group(1) if m else ""

def extract_base_linkedin_url(url: str) -> str:
    url = url.strip()
    m = re.match(r"(https://www\.linkedin\.com/in/[^/?#]+)", url)
    return (m.group(1) + "/") if m else url


def get_sheet_col_indices(sheet):
    header_row = sheet.row_values(1)
    col_map = {c.lower(): i+1 for i, c in enumerate(header_row)}
    def get_or_add(col_name, default_label):
        idx = col_map.get(col_name)
        if not idx:
            sheet.update_cell(1, len(header_row)+1, default_label)
            return len(header_row)+1
        return idx
    return {
        "status_col": get_or_add("status", "Status"),
        "connection_col": get_or_add("connection", "Connection"),
        "date_col": get_or_add("date", "Date")
    }
    

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
    
def get_sheet_status(connection_request):
    invalid_statuses = [
        "404", "invalid url", "decline invitation", "request blocked",
        "no connect option", "timeout", "error", "duplicate"
    ]
    if (connection_request or "").strip().lower() in invalid_statuses:
        return "invalid"
    return "valid"

def handle_connection(driver, wait, note, sheet, row_num, status_col, connection_col, date_col):
    def update_sheet(status_text, conn_text):
        try:
            sheet.update_cell(row_num, status_col, get_sheet_status(conn_text))
            sheet.update_cell(row_num, connection_col, conn_text)
            date_value = sheet.cell(row_num, date_col).value
            if not date_value or str(date_value).strip() == "":
                sheet.update_cell(row_num, date_col, time.strftime("%Y-%m-%d"))
        except Exception as e:
            print(f"Could not update sheet for row {row_num}: {e}")

    try:
        sec = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//section[contains(@class,'artdeco-card') and @data-member-id]")))

        scroll_like_human(driver)
        prev_connection_val = (sheet.cell(row_num, connection_col).value or "").strip().lower()

        try:
            connect = safe_find(sec, By.XPATH, ".//button[.//span[text()='Connect']]")
            if connect:
                if prev_connection_val in ["pending", "request sent with note", "request sent without note"]:
                    update_sheet("valid", "Decline Invitation")
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
                    update_sheet("Valid", "Request Sent with Note")
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
                                update_sheet("Valid", "Request Sent without Note")
                                return "Request Sent without Note", True
                        update_sheet("Blocked", "Request Blocked")
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
                        update_sheet("valid", "Decline Invitation")
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
                        nap(config.CONNECTION_DELAY_MIN, config.CONNECTION_DELAY_MAX)
                        update_sheet("Valid", "Request Sent with Note")
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
                                    skip_note = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label,'Send without a note')]")))
                                    driver.execute_script("arguments[0].click();", skip_note)
                                    nap(config.CONNECTION_DELAY_MIN, config.CONNECTION_DELAY_MAX)
                                    update_sheet("Valid", "Request Sent without Note")
                                    return "Request Sent without Note", True
                            update_sheet("Blocked", "Request Blocked")
                            return "Request Blocked", False
                        except Exception:
                            pass
        except Exception:
            pass
        try:
            if safe_find(sec, By.XPATH, ".//span[text()='Pending']"):
                update_sheet("valid", "Pending")
                return "Pending", False
        except Exception:
            pass
        try:
            if safe_find(sec, By.XPATH, ".//span[text()='Message']"):
                update_sheet("valid", "Already Connected")
                return "Already Connected", False
        except Exception:
            pass
        update_sheet("Invalid", "No Connect Option")
        return "No Connect Option", False

    except TimeoutException:
        update_sheet("Invalid", "Timeout")
        print("[DEBUG] Timeout: Connection Element Not Found.")
        return "Connection Element Not Found", False
    except Exception as e:
        update_sheet("Invalid", "Error")
        print(f"[DEBUG] Unexpected error in handle_connection: {e}")
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

def update_pending_connection_status(cursor, profile_id, connection_date, status_check_date, connection_request):
    """
    Update pending connection status and date info for a LinkedIn profile in MySQL.
    """
    sql = """
        UPDATE linkedindata
        SET connection_date = %s,status_check_date = %s,connection_request = %sWHERE profile_id = %s
    """
    cursor.execute(sql, (connection_date, status_check_date, connection_request, profile_id))

def update_pending_statuses():
    """
    For every row in your Google Sheet whose Connection column == "Pending":
      • Re-check LinkedIn to see if the request is still pending
      • Insert the new status & dates into Zoho Creator
      • If no longer pending, clear the sheet’s “Pending” marker
    """
    sheet      = gclient.open_by_key(INPUT_SHEET_ID).worksheet(INPUT_SHEET_NAME)
    header     = sheet.row_values(1)
    status_col = header.index("Status") + 1
    conn_col   = header.index("Connection") + 1
    date_col   = header.index("Date") + 1

    driver = init_driver()
    wait   = WebDriverWait(driver, 15)
    today  = datetime.datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")

    all_rows = sheet.get_all_records()
    for idx, row in enumerate(all_rows, start=2):
        if (row.get("Connection") or "").strip().lower() != "pending":
            continue

        profile_url = row.get("LinkedIn Profile", "")
        profile_id  = extract_linkedin_id(profile_url)

        # Visit profile and check if it's still pending
        driver.get(profile_url)
        time.sleep(SCRAPE_DELAY + random.uniform(0, JITTER))

        still_pending = False
        try:
            card = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//section[contains(@class,'artdeco-card') and @data-member-id]"),
                    timeout=8
                )
            )
            if safe_find(card, By.XPATH, ".//span[text()='Pending']"):
                still_pending = True
        except TimeoutException:
            still_pending = False

        if still_pending:
            # Build a fresh payload for Zoho Creator (still pending status)
            payload = {
                "profile_id":         profile_id,
                "connection_request": "Pending",
                "connection_date":    row.get("Date", today),
                "status_check_date":  today
            }
            # Insert into Zoho Creator (linkedindata form)
            response = create_record("linkedindata", payload)
            log.info(f"[Row {idx}] Zoho Creator create (still pending) → {response}")
        else:
            # Clear the Pending marker in the sheet
            sheet.update_cell(idx, conn_col, "")
            sheet.update_cell(idx, status_col, "valid")
            log.info(f"[Row {idx}] No longer pending → sheet cleared")

    driver.quit()
    log.info("Pending-status updater complete.")



def get_existing_connection_date(cursor, profile_id):
    """Fetch existing connection_date for a profile from MySQL."""
    sql = "SELECT connection_date FROM linkedindata WHERE profile_id = %s"
    cursor.execute(sql, (profile_id,))
    row = cursor.fetchone()
    return row[0] if row and row[0] else ""

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
    load_cookies(driver, account)
    main_handle = driver.current_window_handle

    sheet = gclient.open_by_key(INPUT_SHEET_ID).worksheet(INPUT_SHEET_NAME)
    cols = get_sheet_col_indices(sheet)
    status_col = cols['status_col']
    conn_col = cols['connection_col']
    date_col = cols['date_col']
    header_row = sheet.row_values(1)
    header_len = len(header_row)

    for row_num, url in batch_urls:
        try:
            # 1) Basic sheet checks
            if not url or ' ' in url:
                sheet.update_cell(row_num, status_col, 'invalid')
                continue

            # 2) Pull row data
            row_vals = sheet.row_values(row_num)
            if len(row_vals) < header_len:
                row_vals += [''] * (header_len - len(row_vals))
            data_map = {header_row[i]: row_vals[i] for i in range(header_len)}

            # Skip already handled
            if data_map.get('Connection','').strip().lower() in ['already connected','decline invitation','duplicate']:
                continue

            profile_id = extract_linkedin_id(url)
            note = generate_message(
                data_map.get('First Name',''), data_map.get('Last Name',''),
                data_map.get('Job Title',''), data_map.get('Company Domain','')
            )

            # 3) Open profile
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])
            driver.get(url)
            random_delay(SCRAPE_DELAY)

            # 4) Invalid URL?
            if is_linkedin_404(driver):
                sheet.update_cell(row_num, status_col, 'invalid')
                driver.close(); driver.switch_to.window(main_handle)
                continue

            # 5) Attempt connection
            status, sent = handle_connection(driver, wait, note, sheet, row_num,
                                            status_col, conn_col, date_col)
            sheet_status = get_sheet_status(status)

            # 6) Update sheet
            today_str = datetime.date.today().strftime('%Y-%m-%d')
            sheet.update_cell(row_num, status_col, sheet_status)
            sheet.update_cell(row_num, conn_col, status)
            if not sheet.cell(row_num, date_col).value:
                sheet.update_cell(row_num, date_col, today_str)

            # timezone & now
            profile_tz = get_timezone_from_location(data_map.get('Location',''))
            now_local = datetime.datetime.now(profile_tz)
            today_human = now_local.strftime('%Y-%m-%d')

            # --- PENDING ---
            if sheet_status=='valid' and status.lower()=='pending':
                base = (data_map.get('Date') or today_human).split()[0]
                try: parsed = datetime.datetime.strptime(base,'%Y-%m-%d').date()
                except: parsed = now_local.date()
                diff = (now_local.date()-parsed).days
                suffix = '(Today)' if diff==0 else '(1 day ago)' if diff==1 else f'({diff} days ago)'
                human_dt = f'{parsed.isoformat()} {suffix}'
                payload = {
                    'profile_id': profile_id,
                    'connection_request': 'Pending',
                    'connection_date': human_dt,
                    'status_check_date': today_human
                }
                resp = upsert_linkedindata(payload)
                log.info(f"[Row {row_num}] Upsert pending → {resp}")
                driver.close(); driver.switch_to.window(main_handle); random_delay(JITTER)
                continue

            # --- ALREADY CONNECTED ---
            if status.lower()=='already connected':
                prof = scrape_profile(driver, wait)
                raw = prof.get('connected_on','')
                payload = {
                    'profile_id': profile_id,
                    'connection_request': 'Already Connected',
                    'connection_date': raw
                }
                resp = upsert_linkedindata(payload)
                log.info(f"[Row {row_num}] Upsert connected → {resp}")
                driver.close(); driver.switch_to.window(main_handle); random_delay(JITTER)
                continue

            # --- FULL SCRAPE ---
            if sheet_status=='valid':
                # human_conn_date
                conn_dt, _ = compute_connection_dates(status, data_map, profile_tz, '')
                base = conn_dt.split()[0] if conn_dt else today_human
                try: parsed = datetime.datetime.strptime(base,'%Y-%m-%d').date()
                except: parsed = now_local.date()
                diff = (now_local.date()-parsed).days
                suffix = '(Today)' if diff==0 else '(1 day ago)' if diff==1 else f'({diff} days ago)'
                human_conn_date = f'{parsed.isoformat()} {suffix}'

                prof = scrape_profile(driver, wait)
                about = scrape_about(driver)
                normalized_status = status.strip().title()

                profile_payload = {
                    'first_name': data_map.get('First Name','') or prof.get('designation',''),
                    'last_name': data_map.get('Last Name',''),
                    'request_sent_date': today_human,
                    'connection_date': human_conn_date,
                    'status_check_date': today_human,
                    'connection_request': normalized_status,
                    'profile_id': profile_id,
                    'invite_message': note,
                    'request_sent_from': account['username'],
                    'linkedin_url': url,
                    'email_id': data_map.get('Work Email','') or prof.get('email',''),
                    'designation': data_map.get('Job Title','') or prof.get('designation',''),
                    'about': about,
                    'no_of_followers': prof.get('followers',''),
                    'no_of_connections': prof.get('connections',''),
                    'company_name': data_map.get('Company Domain','') or prof.get('company_name',''),
                    'country': extract_country(data_map.get('Location','') or prof.get('location','')),
                    'location': data_map.get('Location','') or prof.get('location','')
                }
                
                parent_resp = upsert_linkedindata(profile_payload)
                log.info(f"[Row {row_num}] Upsert parent → {parent_resp}")
                
                # Extract the Zoho record ID
                data_field = parent_resp.get("data")
                if isinstance(data_field, dict):
                    zoho_record_id = data_field.get("ID")
                elif isinstance(data_field, list) and data_field:
                    zoho_record_id = data_field[0].get("ID")
                else:
                    raise RuntimeError(f"Couldn’t get Zoho record ID back: {parent_resp}")
                base_url = extract_base_linkedin_url(url)
                driver.get(f"{base_url.rstrip('/')}/details/experience/")
                random_delay(SCRAPE_DELAY)
                exp_list = scrape_experience(driver, wait)
                if exp_list:
                    for idx, exp_text in enumerate(exp_list, start=1):
                        exp_payload = {
                            "linkedindata":     [zoho_record_id],           
                            "linkedin_url":     url,                        
                            "sections":         f"{profile_id}_exp_{idx}",  
                            "experience_entry": exp_text,                   
                        }
                        resp = create_record("experience", exp_payload)
                        log.info(f"[Row {row_num}] Created experience {idx}/{len(exp_list)} → {resp}")
                else:
                    log.info(f"[Row {row_num}] No experience entries found; skipping.")
                    
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
                            "linkedindata":    [zoho_record_id],           
                            "linkedin_url":    url,                         
                            "sections":        f"{profile_id}_edu_{idx}",   
                            "education_entry": entry_text,                  
                        }
                        resp = create_record("education", edu_payload)
                        log.info(f"[Row {row_num}] Created education {idx}/{len(edu_list)} → {resp}")
                else:
                    log.info(f"[Row {row_num}] No education entries found; skipping.")
                random_delay(PAGE_LOAD_DELAY)
            # cleanup
            driver.close()
            driver.switch_to.window(main_handle)
            random_delay(JITTER)

        except Exception as e:
            log.error(f"[Row {row_num}] Error: {e}")
            try:
                driver.close(); driver.switch_to.window(main_handle)
            except:
                pass

    driver.quit()
    log.info(f" [Batch {batch_index}] Complete.")



def process_urls_for_account(args):
    acc_idx, account, assigned_urls = args
    log.info(f"[{account['username']}] Processing {len(assigned_urls)} LinkedIn URLs.")
    process_batch((acc_idx, assigned_urls, account))
    log.info(f"[{account['username']}] All URLs processed.")

def chunk_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main():
    # Load all available LinkedIn accounts (from .env or config)
    accounts = load_accounts()
    if not accounts:
        logging.error("No LinkedIn accounts configured in .env.")
        return

    # Load input LinkedIn URLs (from Google Sheets, etc)
    url_tuples = get_input_urls()   # ← list of (row_num, linkedin_url)
    url_tuples = dedupe_input_urls(url_tuples)
    if not url_tuples:
        logging.warning("📭 No LinkedIn URLs found in input.")
        return

    num_accounts = len(accounts)
    assigned = [[] for _ in range(num_accounts)]
    # Distribute URLs in round-robin fashion to each account
    for idx, tup in enumerate(url_tuples):
        assigned[idx % num_accounts].append(tup)   # tup is (row_num, linkedin_url)

    # Prepare arguments for each worker
    group_args = [(i, accounts[i], assigned[i]) for i in range(num_accounts)]
    logging.info(f"Assigning {len(url_tuples)} URLs to {num_accounts} accounts (round-robin)")

    with ThreadPool(processes=num_accounts) as pool:
        pool.map(process_urls_for_account, group_args)

    logging.info("All accounts and URLs completed.")


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1].lower() == "pending":
            update_pending_statuses()    
        else:
            main()   
    except KeyboardInterrupt:
        print("\nInterrupted by user, exiting gracefully.")
        sys.exit(0)
