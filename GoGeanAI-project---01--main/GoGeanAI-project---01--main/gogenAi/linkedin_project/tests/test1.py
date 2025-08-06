import os
import time
import random
import pickle
import sys
import gspread
import logging
import re
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from oauth2client.service_account import ServiceAccountCredentials
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))
from app import config

# ---------- Setup ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
load_dotenv()
SECRET_KEY = config.SECRET_KEY
fernet = Fernet(SECRET_KEY.encode())

COOKIES_PATH = "./"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
INPUT_SHEET_ID = os.getenv("INPUT_SHEET_ID")
INPUT_SHEET_NAME = os.getenv("INPUT_SHEET_NAME")
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY", 4))
JITTER = float(os.getenv("JITTER", 1))
EDU_OUTPUT_SHEET = os.getenv("EDU_OUTPUT_SHEET", "educationdata")
EXP_OUTPUT_SHEET = os.getenv("EXP_OUTPUT_SHEET", "experiencedata")

SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, SCOPES)
gclient = gspread.authorize(creds)

# --------- Humanization functions ---------
def human_typing(element, text, min_delay=0.08, max_delay=0.19):
    """Type text character by character, like a human."""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(min_delay, max_delay))

def human_sleep(min_s, max_s):
    """Sleep for a random duration between min and max seconds."""
    t = random.uniform(min_s, max_s)
    time.sleep(t)
    return t

# --------- Selenium & Cookie Functions ---------
def init_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--headless")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    return webdriver.Chrome(options=opts)
def load_cookies(driver, account):
    cookie_file = account["cookie_file"]
    if os.path.exists(cookie_file):
        log.info(f"Loading cookies from {cookie_file}")
        with open(cookie_file, "rb") as f:
            cookies = pickle.load(f)
        driver.get("https://www.linkedin.com")
        time.sleep(random.uniform(2, 3))

        # Get the current domain for comparison
        current_domain = ".linkedin.com"

        for cookie in cookies:
            # Fix domain for all cookies
            cookie_domain = cookie.get("domain", "")
            # Always set to .linkedin.com for consistency
            cookie["domain"] = ".linkedin.com"

            # Remove 'sameSite' if its value is None, or fix as needed
            if cookie.get("sameSite") == "None":
                cookie["sameSite"] = "Strict"
            # Selenium expects 'expiry' not 'expirationDate'
            if "expirationDate" in cookie:
                cookie["expiry"] = int(cookie.pop("expirationDate"))
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                log.warning(f"Failed to add cookie {cookie.get('name')}: {e}")

        driver.get("https://www.linkedin.com/feed/")
        time.sleep(random.uniform(2, 3))
        if "feed" in driver.current_url:
            log.info(f"Session restored from cookies for {account['username']}")
            return True
        else:
            log.warning(f"Cookies expired for {account['username']}")
    return False

def login_and_save_cookies(driver, account, cookie_file):
    driver.get("https://www.linkedin.com/login")
    wait = WebDriverWait(driver, 15)
    username_el = wait.until(EC.presence_of_element_located((By.ID, "username")))
    username_el.clear()
    password_el = driver.find_element(By.ID, "password")
    password_el.clear()
    human_typing(username_el, account["username"])
    human_sleep(0.8, 1.6)
    human_typing(password_el, account["password"])
    human_sleep(0.8, 1.2)
    signin_button = driver.find_element(By.XPATH, "//button[@type='submit']")
    signin_button.click()
    wait.until(lambda d: "feed" in d.current_url or "checkpoint" in d.current_url)
    human_sleep(2, 4)
    cookies = driver.get_cookies()
    with open(cookie_file, "wb") as f:
        pickle.dump(cookies, f)
    log.info(f"Login successful for {account['username']}, cookies saved to {cookie_file}")

def load_cookies_or_login(driver, account):
    cookie_file = account["cookie_file"]
    if not load_cookies(driver, account):
        login_and_save_cookies(driver, account, cookie_file)

# --------- Account Loader ---------
def load_accounts():
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
    if not accounts:
        log.error("No LinkedIn accounts found in environment variables!")
        sys.exit(1)
    return accounts

# --------- Util ---------
def extract_profile_id(url):
    url = url.strip().split("?")[0]
    m = re.match(r"https://www\.linkedin\.com/in/([^/?#]+)/?", url)
    return m.group(1) if m else ""

def extract_base_linkedin_url(url: str) -> str:
    url = url.strip()
    m = re.match(r"(https://www\.linkedin\.com/in/[^/?#]+)", url)
    return (m.group(1) + "/") if m else url

def is_linkedin_404(driver):
    try:
        return "This page doesn’t exist" in driver.page_source or "Profile Not Found" in driver.page_source
    except Exception:
        return False

# --------- Scraper Functions ---------
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
        log.warning(f"[WARN] Error scraping education: {e}")
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

# --------- Main Script ---------
def main():
    input_sheet = gclient.open_by_key(INPUT_SHEET_ID).worksheet(INPUT_SHEET_NAME)
    edu_sheet = gclient.open_by_key(INPUT_SHEET_ID).worksheet(EDU_OUTPUT_SHEET)
    exp_sheet = gclient.open_by_key(INPUT_SHEET_ID).worksheet(EXP_OUTPUT_SHEET)
    data = input_sheet.get_all_records()
    accounts = load_accounts()
    num_accounts = len(accounts)

    processed_profile_ids = set()
    try:
        edu_rows = edu_sheet.get_all_records()
        exp_rows = exp_sheet.get_all_records()
        for row in edu_rows:
            processed_profile_ids.add(row.get("profile_id", "").strip())
        for row in exp_rows:
            processed_profile_ids.add(row.get("profile_id", "").strip())
    except Exception as e:
        log.warning(f"Could not load already processed profiles: {e}")

    for i, row in enumerate(data):
        linkedin_url = (row.get("Linkedin link") or "").strip()
        connection_status = row.get("Connection", "").strip()
        if "Link is wrong" in connection_status:
            log.info(f"[Row {i+2}] Skipped: Link marked as wrong in sheet ({linkedin_url})")
            continue
        if not linkedin_url:
            continue
        profile_id = extract_profile_id(linkedin_url)
        if not profile_id or profile_id in processed_profile_ids:
            log.info(f"Skipping already processed: {linkedin_url}")
            continue
        account = accounts[i % num_accounts]
        driver = init_driver()
        load_cookies_or_login(driver, account)
        wait = WebDriverWait(driver, 15)
        log.info(f"[{i+2}] Using: {account['username']} | {linkedin_url}")
        base_url = extract_base_linkedin_url(linkedin_url)

        # Add some human wait before visiting the profile
        human_sleep(2, 6)
        driver.get(base_url)
        human_sleep(2, 4)
        if is_linkedin_404(driver):
            log.info(f"[Row {i+2}] Profile not found (404): {linkedin_url}")
            driver.quit()
            continue

        # Scrape experience
        driver.get(f"{base_url.rstrip('/')}/details/experience/")
        human_sleep(2.5, 5.5)
        exp_list = scrape_experience(driver, wait)
        for idx, exp_entry in enumerate(exp_list, 1):
            exp_sheet.append_row([
                profile_id,
                linkedin_url,
                "Experience",
                idx,
                exp_entry
            ])
            human_sleep(0.2, 0.7)

        # Scrape education
        driver.get(f"{base_url.rstrip('/')}/details/education/")
        human_sleep(2.5, 5.5)
        edu_list = scrape_education(driver, wait)
        for idx, edu in enumerate(edu_list, 1):
            parts = [edu.get('school', ''), edu.get('degree', ''), edu.get('dates', ''), edu.get('description', '')]
            edu_entry = " | ".join(filter(None, parts))
            edu_sheet.append_row([
                profile_id,
                linkedin_url,
                "Education",
                idx,
                edu_entry
            ])
            human_sleep(0.2, 0.7)

        log.info(f"✔ Done: {linkedin_url}")
        driver.quit()
        # Extra delay between profiles (acts most human!)
        human_sleep(5, 14)

    log.info("All profiles processed.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user, exiting gracefully.")
