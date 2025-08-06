import os
import time
import random
import logging
import glob
import csv
import sys
import re
import pyautogui
from cryptography.fernet import Fernet
from datetime import datetime,timedelta
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import gspread
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))
from app import zoho_api
from app import setup
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "tests")))
from tests.test3 import main as test3_main

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)

# Load delay parameters from the .env file
GOOGLE_SHEET_ID = os.getenv("INPUT_SHEET_ID")
SECRET_KEY_JSON = "secret_key.json"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

CLAYRUN_EMAIL = os.getenv("CLAYRUN_EMAIL")
CLAYRUN_PASSWORD = os.getenv("CLAYRUN_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY")
# Decrypt
if SECRET_KEY and CLAYRUN_EMAIL and CLAYRUN_PASSWORD:
    fernet = Fernet(SECRET_KEY.encode())
    email_decrypted = fernet.decrypt(CLAYRUN_EMAIL.encode()).decode()
    password_decrypted = fernet.decrypt(CLAYRUN_PASSWORD.encode()).decode()
else:
    raise Exception("SECRET_KEY or Clay.run credentials missing in .env")

PAGE_LOAD_DELAY = float(os.getenv("PAGE_LOAD_DELAY", 4))
ACTION_DELAY = float(os.getenv("ACTION_DELAY", 3))
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY", 4))
CONNECTION_DELAY = float(os.getenv("CONNECTION_DELAY", 4))
JITTER = float(os.getenv("JITTER", 1.5))
SCROLL_STEPS_MIN = int(os.getenv("SCROLL_STEPS_MIN", 2))
SCROLL_STEPS_MAX = int(os.getenv("SCROLL_STEPS_MAX", 6))
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", 0.4))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
BATCH_DELAY = int(os.getenv("BATCH_DELAY", 40))


def setup_driver():
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    # -- Important: set download directory to script's directory --
    prefs = {
        "download.default_directory": SCRIPT_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    return driver

def decrypt_credential(encrypted_text, key):
    cipher_suite = Fernet(key)
    return cipher_suite.decrypt(encrypted_text.encode()).decode()


def wait_for_csv_download(download_dir, timeout=120):
    logging.info("Waiting for the CSV file to appear in the directory...")
    end_time = time.time() + timeout
    while time.time() < end_time:
        csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
        if csv_files:
            csv_file = max(csv_files, key=os.path.getctime)  # Latest file
            logging.info(f"Found downloaded CSV file: {csv_file}")
            return csv_file
        time.sleep(2)
    raise FileNotFoundError("CSV file was not downloaded in the expected time.")


def append_csv_to_gsheet(csv_file_path, sheet_id, worksheet_index=0):
    sheet_columns = [
        "First Name",
        "Last Name",
        "Full Name",
        "Job Title",
        "Location",
        "Company Domain",
        "LinkedIn Profile",
        "Work Email",
        "Validation",
    ]
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SECRET_KEY_JSON, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.get_worksheet(worksheet_index)
    with open(csv_file_path, "r", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sheet_row = [
                row.get("First Name", ""),
                row.get("Last Name", ""),
                row.get("Full Name", ""),
                row.get("Job Title", ""),
                row.get("Location", ""),
                row.get("Company Domain", ""),
                row.get("LinkedIn Profile", ""),
                row.get("Email", ""),  # Map CSV 'Email' to 'Work Email'
                "",  # Validation blank
            ]
            worksheet.append_row(sheet_row, value_input_option="USER_ENTERED")
    logging.info(f"Appended CSV data to Google Sheet (ID: {sheet_id})")
    try:
        os.remove(csv_file_path)
        logging.info(f"Deleted CSV file: {csv_file_path}")
    except Exception as e:
        logging.warning(f"Could not delete CSV file: {e}")


def human_type(element, text, min_delay=0.1, max_delay=0.3):
    for char in text:
        element.send_keys(char)
        human_delay(min_delay, max_delay)


def human_delay(min_delay, max_delay):
    delay = random.uniform(min_delay, max_delay)
    logging.debug(f"Sleeping for {delay:.2f} seconds (human delay).")
    time.sleep(delay)


# Google Sheets credentials setup
logging.basicConfig(level=logging.INFO)


# Google Sheets credentials setup
def get_google_sheet_data():
    scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("scripts/secret_key.json", scope)
    client = gspread.authorize(creds)

    # Open the sheet and get the data
    sheet = client.open_by_key(os.getenv("CLAY_SHEET_ID"))
    worksheet = sheet.worksheet(os.getenv("CLAY_SHEET_NAME"))

    # Get all rows of data
    data = worksheet.get_all_values()

    # Get today's date in the format "19 Jun 25" (DD MMM YY)
    current_date = datetime.now().strftime("%-d %B %Y")
    logging.info(f"Current date: {current_date}")
    header_row = data[1]  # Row 2 (0-based index)
    
    try:
        # Find the column index that matches today's date
        current_date_column_index = header_row.index(
            current_date
        )  # Find the column index of today's date
        logging.info(f"Found today's date in column: {current_date_column_index}")
    except ValueError:
        logging.error(f"Current date {current_date} not found in the sheet header.")
        return {}

    # Extract the corresponding data from that column
    search_criteria = {}

    for row in data[2:]:
        label = row[
            1
        ].strip()  # The first column contains the label (e.g., "Industries to include")
        value = row[
            current_date_column_index
        ].strip()  # Get the value in the column corresponding to today's date
        search_criteria[label] = value

    logging.info(f"Search criteria for today ({current_date}): {search_criteria}")
    return search_criteria


def login_to_clay(driver):
    from dotenv import load_dotenv
    import os
    load_dotenv()  # Make sure .env is loaded

    SECRET_KEY = os.getenv("SECRET_KEY")
    CLAYRUN_EMAIL = os.getenv("CLAYRUN_EMAIL")
    CLAYRUN_PASSWORD = os.getenv("CLAYRUN_PASSWORD")

    # Decrypt credentials
    if not (SECRET_KEY and CLAYRUN_EMAIL and CLAYRUN_PASSWORD):
        logging.error("Missing Clay.run credentials or SECRET_KEY in .env!")
        return

    try:
        email_decrypted = decrypt_credential(CLAYRUN_EMAIL, SECRET_KEY)
        password_decrypted = decrypt_credential(CLAYRUN_PASSWORD, SECRET_KEY)
    except Exception as e:
        logging.error(f"Error decrypting Clay.run credentials: {e}")
        return

    logging.info("Opening Clay.run login page...")
    driver.get("https://app.clay.com/login")
    time.sleep(PAGE_LOAD_DELAY)

    try:
        email_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "headlessui-control-:r0:"))
        )
        human_type(email_input, email_decrypted)
        logging.info("Typed email.")
        human_delay(ACTION_DELAY, ACTION_DELAY)

        continue_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Continue')]")
            )
        )
        continue_button.click()
        logging.info("Clicked 'Continue' for email.")
        time.sleep(PAGE_LOAD_DELAY)

        password_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "headlessui-control-:r2:"))
        )
        human_type(password_input, password_decrypted)
        logging.info("Typed password.")
        human_delay(ACTION_DELAY, ACTION_DELAY)

        submit_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Continue')]")
            )
        )
        submit_button.click()
        logging.info("Clicked 'Continue' for password.")
        logging.info("Waiting for dashboard to load...")
        time.sleep(PAGE_LOAD_DELAY)

    except TimeoutError as e:
        logging.error(f"Login failed: {e}")


def safe_click(driver, element):
    """Scroll the element into view and click it using JavaScript."""
    try:
        # Scroll the element into view
        driver.execute_script("arguments[0].scrollIntoView(true);", element)

        # Wait until the element is clickable
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(element))

        # Use JavaScript to click on the element directly
        driver.execute_script("arguments[0].click();", element)

        logging.info(f"Successfully clicked on the element: {element}")
    except Exception as e:
        logging.error(f"Error while clicking element: {e}")
        raise


def safe_focus_and_type(driver, element, text):
    try:
        WebDriverWait(driver, 20).until(EC.visibility_of(element))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable(element))
        element.click()  # <-- click first
        driver.execute_script("arguments[0].focus();", element)
        human_delay(ACTION_DELAY, ACTION_DELAY)
        human_type(element, text)
        element.send_keys(Keys.ENTER)
        driver.execute_script("arguments[0].blur();", element)
        element.send_keys(Keys.TAB)
        human_delay(ACTION_DELAY, ACTION_DELAY)
    except Exception as e:
        logging.error(f"Error typing into element: {e}")


# def handle_enrichment_and_save(driver):
#     """Handle the 'Add Enrichment' and 'Save and run' processes."""
#     try:
#         add_enrichment_button = WebDriverWait(driver, 700).until(
#             EC.presence_of_element_located(
#                 (By.XPATH, "//button[@data-testid='title-bar-enrich-data']")
#             )
#         )
#         add_enrichment_button.click()
#         logging.info("Clicked on Add Enrichment button.")
#         human_delay(ACTION_DELAY, ACTION_DELAY)

#         # View all enrichments
#         view_all_enrichments_button = WebDriverWait(driver, 10).until(
#             EC.element_to_be_clickable(
#                 (By.XPATH, "//button[normalize-space(text())='View all enrichments']")
#             )
#         )
#         actions = ActionChains(driver)
#         actions.move_to_element(view_all_enrichments_button).perform()
#         view_all_enrichments_button.click()
#         logging.info("Clicked on 'View all enrichments' button.")
#         human_delay(ACTION_DELAY, ACTION_DELAY)

#         # Search for ZeroBounce
#         search_input = WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located(
#                 (By.XPATH, "//input[@aria-label='Search with AI']")
#             )
#         )
#         search_input.clear()  # Clear any pre-filled value
#         search_input.send_keys("Validate Email , ZeroBounce")
#         search_input.send_keys(Keys.RETURN)
#         logging.info("Entered 'ZeroBounce' in the search input.")
#         zero_bounce_xpath = "//div[contains(@class, 'group') and contains(@class, 'flex')]//p[normalize-space(text())='ZeroBounce']/ancestor::div[contains(@class, 'flex')]"
#         find_zero_bounce_button = WebDriverWait(driver, 10).until(
#             EC.element_to_be_clickable((By.XPATH, zero_bounce_xpath))
#         )

#         # Click the 'ZeroBounce' element
#         find_zero_bounce_button.click()
#         logging.info("Clicked on 'ZeroBounce'.")
#         human_delay(ACTION_DELAY, ACTION_DELAY)

#         # Continue to add fields
#         continue_button = WebDriverWait(driver, 10).until(
#             EC.element_to_be_clickable(
#                 (By.XPATH, "//button[normalize-space(text())='Continue to add fields']")
#             )
#         )
#         continue_button.click()

#          # Wait for the button to be clickable
#         logging.info("Clicked on 'Continue to add fields'.")

#         # Save changes
#         save_button = WebDriverWait(driver, 10).until(
#             EC.element_to_be_clickable(
#                 (By.XPATH, "//button[normalize-space(text())='Save']")
#             )
#         )
#         save_button.click()
#         logging.info("Clicked on 'Save' button.")
#         human_delay(ACTION_DELAY, ACTION_DELAY)

#         # Save and run 10 rows
#         save_10_button = WebDriverWait(driver, 10).until(
#             EC.element_to_be_clickable(
#                 (By.XPATH, "(//button[contains(text(), 'Save and run 10 rows')])[1]")
#             )
#         )
#         save_10_button.click()

#         logging.info("Clicked on 'Save and run 10 rows' button.")
#         human_delay(PAGE_LOAD_DELAY, PAGE_LOAD_DELAY)

#         # Wait for the rows to finish processing
#         WebDriverWait(driver, 30).until(
#             EC.presence_of_element_located(
#                 (By.XPATH, "//span[contains(text(), 'Rows processed successfully')]")
#             )
#         )
#         logging.info("Rows processed successfully.")

#     except Exception as e:
#         logging.error(f"Error while handling enrichment and save: {e}")
#         time.sleep(PAGE_LOAD_DELAY)  # Allow some time for recovery if needed


def fill_form_fields(driver, data):
    try:
        # Click on the 'Find and enrich people' button
        human_delay(ACTION_DELAY, ACTION_DELAY)
        enrich_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button//h5[contains(text(), 'Find people')]")
            )
        )
        safe_click(driver, enrich_button)

        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Click on the 'Company attributes' button
        company_attributes_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[.//p[text()='Company attributes']]")
            )
        )
        safe_click(driver, company_attributes_button)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # **Industries to include**
        industries_include_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Industries to include']")
            )
        )
        industries_include_data = data.get("Industries to include", "").strip()
        if industries_include_data:
            industries_include_list = industries_include_data.split(",")
            for industry in industries_include_list:
                industries_include_input.send_keys(
                    industry.strip()
                )  # Append the industry
                industries_include_input.send_keys(
                    Keys.ENTER
                )  # Add the industry to the list
                time.sleep(ACTION_DELAY)
            industries_include_input.send_keys(Keys.TAB)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No industries to include, skipping this field.")

        # **Industries to exclude**
        industries_exclude_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Industries to exclude']")
            )
        )
        industries_exclude_data = data.get("Industries to exclude", "").strip()
        if industries_exclude_data:
            industries_exclude_list = industries_exclude_data.split(",")
            for industry in industries_exclude_list:
                industries_exclude_input.send_keys(
                    industry.strip()
                )  # Append the industry
                industries_exclude_input.send_keys(
                    Keys.ENTER
                )  # Add the industry to the list
                time.sleep(ACTION_DELAY)
            industries_exclude_input.send_keys(Keys.TAB)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No industries to exclude, skipping this field.")

        # **Company sizes**
        company_sizes_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Company sizes']")
            )
        )
        company_sizes_data = data.get("Company sizes", "").strip()

        # Log the company sizes data to ensure it's being fetched correctly
        logging.info(f"Company sizes data: {company_sizes_data}")

        if company_sizes_data:
            company_sizes_list = company_sizes_data.split(",")  # Split by comma
            logging.info(
                f"Company sizes list: {company_sizes_list}"
            )  # Log the list after splitting
            for size in company_sizes_list:
                company_sizes_input.send_keys(size.strip())  # Append each company size
                company_sizes_input.send_keys(
                    Keys.ENTER
                )  # Add the company size to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            company_sizes_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No company sizes to include, skipping this field.")

        # **Description keywords to include**
        description_include_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Description keywords to include']")
            )
        )
        description_include_data = data.get(
            "Description keywords to include", ""
        ).strip()
        if description_include_data:
            safe_focus_and_type(
                driver, description_include_input, description_include_data
            )
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No description keywords to include, skipping this field.")

        # **Description keywords to exclude**
        description_exclude_input = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Description keywords to exclude']")
            )
        )
        description_exclude_data = data.get(
            "Description keywords to exclude", ""
        ).strip()
        if description_exclude_data:
            safe_focus_and_type(
                driver, description_exclude_input, description_exclude_data
            )
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No description keywords to exclude, skipping this field.")

        job_title_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[contains(@aria-expanded, 'false')]//p[contains(text(), 'Job title')]",
                )
            )
        )
        safe_click(driver, job_title_button)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        seniority_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Seniority']")
            )
        )
        seniority_data = data.get("Seniority", "").strip()
        if seniority_data:
            seniority_list = seniority_data.split(",")  # Split by comma
            for seniority in seniority_list:
                seniority_input.send_keys(seniority.strip())  # Append the seniority
                seniority_input.send_keys(Keys.ENTER)  # Add the seniority to the list
                time.sleep(ACTION_DELAY)
            seniority_input.send_keys(Keys.TAB)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No seniority data, skipping this field.")

        # **Job functions**
        job_functions_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Job functions']")
            )
        )
        job_functions_data = data.get("Job functions", "").strip()
        if job_functions_data:
            job_functions_list = job_functions_data.split(",")  # Split by comma
            for job_function in job_functions_list:
                job_functions_input.send_keys(
                    job_function.strip()
                )  # Append the job function
                job_functions_input.send_keys(
                    Keys.ENTER
                )  # Add the job function to the list
                time.sleep(ACTION_DELAY)
            job_functions_input.send_keys(Keys.TAB)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No job functions data, skipping this field.")

        # **Job titles to include**
        job_titles_include_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Job titles to include']")
            )
        )
        job_titles_include_data = data.get("Job titles to include", "").strip()
        if job_titles_include_data:
            job_titles_include_list = job_titles_include_data.split(
                ","
            )  # Split by comma
            for job_title_include in job_titles_include_list:
                job_titles_include_input.send_keys(
                    job_title_include.strip()
                )  # Append the job title
                job_titles_include_input.send_keys(
                    Keys.ENTER
                )  # Add the job title to the list
                time.sleep(ACTION_DELAY)
            job_titles_include_input.send_keys(Keys.TAB)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No job titles to include, skipping this field.")

        # **Job titles to exclude**
        job_titles_exclude_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Job titles to exclude']")
            )
        )
        job_titles_exclude_data = data.get("Job titles to exclude", "").strip()
        if job_titles_exclude_data:
            job_titles_exclude_list = job_titles_exclude_data.split(
                ","
            )  # Split by comma
            for job_title_exclude in job_titles_exclude_list:
                job_titles_exclude_input.send_keys(
                    job_title_exclude.strip()
                )  # Append the job title
                job_titles_exclude_input.send_keys(
                    Keys.ENTER
                )  # Add the job title to the list
                time.sleep(ACTION_DELAY)
            job_titles_exclude_input.send_keys(Keys.TAB)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No job titles to exclude, skipping this field.")

        # Click on 'Experience' section button
        experience_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[contains(@aria-expanded, 'false')]//p[contains(text(), 'Experience')]",
                )
            )
        )
        safe_click(driver, experience_button)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        months_in_current_role_min_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Months in current role')]]//input[@placeholder='Min']",
                )
            )
        )
        months_in_current_role_min_data = data.get("Months in current role(Min)", "")
        months_in_current_role_min_input.clear()  # Clear any existing value
        months_in_current_role_min_input.send_keys(
            months_in_current_role_min_data
        )  # Fill the input field
        months_in_current_role_min_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        # Fill 'Months in current role (Max)' field
        months_in_current_role_max_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Months in current role')]]//input[@placeholder='Max']",
                )
            )
        )
        months_in_current_role_max_data = data.get("Months in current role(Max)", "")
        months_in_current_role_max_input.clear()  # Clear any existing value
        months_in_current_role_max_input.send_keys(
            months_in_current_role_max_data
        )  # Fill the input field
        months_in_current_role_max_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        # Fill 'No of experiences (Min)' field
        no_of_experiences_min_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Number of experiences')]]//input[@placeholder='Min']",
                )
            )
        )
        no_of_experiences_min_data = data.get("No of experiences(Min)", "")
        no_of_experiences_min_input.clear()  # Clear any existing value
        no_of_experiences_min_input.send_keys(
            no_of_experiences_min_data
        )  # Fill the input field
        no_of_experiences_min_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        # Fill 'No of experiences (Max)' field
        no_of_experiences_max_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Number of experiences')]]//input[@placeholder='Max']",
                )
            )
        )
        no_of_experiences_max_data = data.get("No of experiences(Max)", "")
        no_of_experiences_max_input.clear()  # Clear any existing value
        no_of_experiences_max_input.send_keys(
            no_of_experiences_max_data
        )  # Fill the input field
        no_of_experiences_max_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        experience_keywords_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Experience description keywords']")
            )
        )
        experience_keywords_data = data.get("Experience description keywords", "")
        safe_focus_and_type(driver, experience_keywords_input, experience_keywords_data)
        human_delay(ACTION_DELAY, ACTION_DELAY)

        
        location_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//button[@data-slot='disclosure-trigger' and .//p[text()='Location']]",
                )  # Update this XPath as per your actual HTML
            )
        )
        safe_click(driver, location_input)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)
        
        
        # # **Countries to include**
        # countries_include_input = WebDriverWait(driver, 10).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH, "//input[@aria-label='Countries to include']")
        #     )
        # )
        # countries_include_data = data.get("Countries to include", "").strip()
        # if countries_include_data:
        #     countries_include_list = countries_include_data.split(",")  # Split by comma
        #     for country in countries_include_list:
        #         countries_include_input.send_keys(country.strip())  # Append the country
        #         countries_include_input.send_keys(
        #             Keys.ENTER
        #         )  # Add the country to the list
        #         time.sleep(ACTION_DELAY)  # Human delay between entries
        #     countries_include_input.send_keys(Keys.TAB)  # Move to the next field
        #     human_delay(ACTION_DELAY, ACTION_DELAY)
        # else:
        #     logging.info("No countries to exclude, skipping this field.")
        try:
            countries_include_input = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//input[@aria-label='Countries to include']")
                )
            )
            print("[DEBUG] Got countries_include_input element")

            countries_include_data = data.get("Countries to include", "").strip()
            print(f"[DEBUG] Raw countries_include_data: '{countries_include_data}'")

            if countries_include_data:
                countries_include_list = countries_include_data.split(",")
                print(f"[DEBUG] Split countries list: {countries_include_list}")

                for country in countries_include_list:
                    country = country.strip()
                    print(f"[DEBUG] Handling country: '{country}'")
                    countries_include_input.clear()
                    print(f"[DEBUG] Cleared input for country: '{country}'")

                    # Type character by character
                    for char in country:
                        countries_include_input.send_keys(char)
                        print(f"[DEBUG] Typed char: '{char}'")
                        time.sleep(0.1)

                    if country.lower() == "united states":
                        print(f"[DEBUG] Special handling for '{country}' (try precise JS click for 2nd option)")
                        try:
                            time.sleep(0.7)  # Let the dropdown appear
                            js_script = """
                                        let container = document.querySelector('ul[id^="headlessui-combobox-options-"], div[id^="headlessui-combobox-options-"]');
                                        if (!container) return 'No options container found';
                                        let options = Array.from(container.querySelectorAll('[role="option"]'))
                                            .filter(el =>
                                                el.offsetParent !== null &&
                                                el.textContent.trim().toLowerCase().includes('united states')
                                            );
                                        if (options.length >= 2) {
                                            let rect = options[1].getBoundingClientRect();
                                            let x = rect.left + rect.width/2;
                                            let y = rect.top + rect.height/2;
                                            options[1].dispatchEvent(new MouseEvent('mouseover', {view: window, bubbles: true, cancelable: true}));
                                            options[1].dispatchEvent(new MouseEvent('mousedown', {view: window, bubbles: true, cancelable: true, clientX: x, clientY: y}));
                                            options[1].dispatchEvent(new MouseEvent('mouseup', {view: window, bubbles: true, cancelable: true, clientX: x, clientY: y}));
                                            options[1].dispatchEvent(new MouseEvent('click', {view: window, bubbles: true, cancelable: true, clientX: x, clientY: y}));
                                            return 'Clicked option: ' + options[1].textContent.trim();
                                        }
                                        return 'Found count: ' + options.length;
"""

                            result = driver.execute_script(js_script)
                            print(f"[DEBUG] JS result: {result}")
                            if not result.startswith('Clicked'):
                                print("[DEBUG] JS could not find 2nd option, pressing ENTER as fallback")
                                countries_include_input.send_keys(Keys.ENTER)
                                time.sleep(ACTION_DELAY)
                            else:
                                time.sleep(ACTION_DELAY)
                        except Exception as e:
                            print(f"[ERROR] JS click failed for '{country}': {e}")
                            driver.save_screenshot(f'debug_countries_include_{country}_js.png')
                            countries_include_input.send_keys(Keys.ENTER)
                            time.sleep(ACTION_DELAY)
        except Exception as e:
            print(f"[ERROR] JS click failed for '{country}': {e}")
            driver.save_screenshot(f'debug_countries_include_{country}_js.png')
            countries_include_input.send_keys(Keys.ENTER)
            time.sleep(ACTION_DELAY)
                    
                

        # **Countries to exclude**
        countries_exclude_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Countries to exclude']")
            )
        )
        countries_exclude_data = data.get("Countries to exclude", "").strip()
        if countries_exclude_data:
            countries_exclude_list = countries_exclude_data.split(",")  # Split by comma
            for country in countries_exclude_list:
                countries_exclude_input.send_keys(country.strip())  # Append the country
                countries_exclude_input.send_keys(
                    Keys.ENTER
                )  # Add the country to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            countries_exclude_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No countries to exclude, skipping this field.")

        # **Regions to include**
        regions_include_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Regions to include']")
            )
        )
        regions_include_data = data.get("Regions to include", "").strip()
        if regions_include_data:
            regions_include_list = regions_include_data.split(",")  # Split by comma
            for region in regions_include_list:
                regions_include_input.send_keys(region.strip())  # Append the region
                regions_include_input.send_keys(
                    Keys.ENTER
                )  # Add the region to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            regions_include_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No regions to include, skipping this field.")

        # **Regions to exclude**
        regions_exclude_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Regions to exclude']")
            )
        )
        regions_exclude_data = data.get("Regions to exclude", "").strip()
        if regions_exclude_data:
            regions_exclude_list = regions_exclude_data.split(",")  # Split by comma
            for region in regions_exclude_list:
                regions_exclude_input.send_keys(region.strip())  # Append the region
                regions_exclude_input.send_keys(
                    Keys.ENTER
                )  # Add the region to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            regions_exclude_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No regions to exclude, skipping this field.")

        # **Cities to include**
        cities_include_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Cities to include']")
            )
        )
        cities_include_data = data.get("Cities to include", "").strip()
        if cities_include_data:
            cities_include_list = cities_include_data.split(",")  # Split by comma
            for city in cities_include_list:
                cities_include_input.send_keys(city.strip())  # Append the city
                cities_include_input.send_keys(Keys.ENTER)  # Add the city to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            cities_include_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No cities to include, skipping this field.")

        # **Cities to exclude**
        cities_exclude_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Cities to exclude']")
            )
        )
        cities_exclude_data = data.get("Cities to exclude", "").strip()
        if cities_exclude_data:
            cities_exclude_list = cities_exclude_data.split(",")  # Split by comma
            for city in cities_exclude_list:
                cities_exclude_input.send_keys(city.strip())  # Append the city
                cities_exclude_input.send_keys(Keys.ENTER)  # Add the city to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            cities_exclude_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No cities to exclude, skipping this field.")

        # **State, provinces or municipalities to include**
        state_include_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@aria-label='States, provinces, or municipalities to include']",
                )
            )
        )
        state_include_data = data.get(
            "State, provinces or municipalities to include", ""
        ).strip()
        if state_include_data:
            state_include_list = state_include_data.split(",")  # Split by comma
            for state in state_include_list:
                state_include_input.send_keys(state.strip())  # Append the state
                state_include_input.send_keys(Keys.ENTER)  # Add the state to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            state_include_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No states to include, skipping this field.")

        # **State, provinces or municipalities to exclude**
        state_exclude_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@aria-label='States, provinces, or municipalities to exclude']",
                )
            )
        )
        state_exclude_data = data.get(
            "State, provinces or municipalities to exclude", ""
        ).strip()
        if state_exclude_data:
            state_exclude_list = state_exclude_data.split(",")  # Split by comma
            for state in state_exclude_list:
                state_exclude_input.send_keys(state.strip())  # Append the state
                state_exclude_input.send_keys(Keys.ENTER)  # Add the state to the list
                time.sleep(ACTION_DELAY)  # Human delay between entries
            state_exclude_input.send_keys(Keys.TAB)  # Move to the next field
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No states to exclude, skipping this field.")

        profile_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//button[@data-slot='disclosure-trigger' and contains(., 'Profile')]",
                )  # Update this XPath as per your actual HTML
            )
        )
        safe_click(driver, profile_input)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Profile: Names
        names_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@aria-label='Names']",
                )  # Update the XPath as per your actual HTML
            )
        )
        names_data = data.get("Names", "").strip()
        if names_data:
            safe_focus_and_type(driver, names_input, names_data)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No 'Names' data found, skipping this field.")
            
        profile_keywords = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Profile keywords']")
            )
        )
        profile_keywords_data = data.get("Profile keywords", "").strip()
        if profile_keywords_data:
            safe_focus_and_type(driver, profile_keywords, profile_keywords_data)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No 'Profile keywords' data found, skipping this field.")
            
        
        headline_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Headline keywords']")
            )
        )
        headline_data = data.get(
            "Headline keywords", ""
        ).strip()  # Get data from the dictionary
        if headline_data:
            safe_focus_and_type(
                driver, headline_input, headline_data
            )  # Using the safe_focus_and_type function
            time.sleep(ACTION_DELAY)  # Enter the input into the field
            logging.info(f"Entered 'Headline keywords': {headline_data}")
        else:
            logging.info("No 'Headline keywords' data found, skipping this field.")

        # *About Section Keywords*
        about_keywords_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='About section keywords']")
            )
        )
        about_keywords_data = data.get(
            "About section keywords", ""
        ).strip()  # Get data from the dictionary
        if about_keywords_data:
            safe_focus_and_type(
                driver, about_keywords_input, about_keywords_data
            )  # Using the safe_focus_and_type function
            time.sleep(ACTION_DELAY)  # Enter the input into the field
            logging.info(f"Entered 'About section keywords': {about_keywords_data}")
        else:
            logging.info("No 'About section keywords' data found, skipping this field.")

        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Profile: No of connections (Min)
        connections_min_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Number of connections')]]//input[@placeholder='Min']",
                )  # Update the XPath as per your actual HTML
            )
        )
        connections_min_data = data.get("No of connections(Min)", "")
        connections_min_input.clear()  # Clear any existing value
        connections_min_input.send_keys(connections_min_data)  # Fill the input field
        connections_min_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        # Profile: No of connections (Max)
        connections_max_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Number of connections')]]//input[@placeholder='Max']",
                )  # Update the XPath as per your actual HTML
            )
        )
        connections_max_data = data.get("No of connections(Max)", "")
        connections_max_input.clear()  # Clear any existing value
        connections_max_input.send_keys(connections_max_data)  # Fill the input field
        connections_max_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        # Profile: Number of followers (Min)
        followers_min_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Number of followers')]]//input[@placeholder='Min']",
                )  # Update the XPath as per your actual HTML
            )
        )
        followers_min_data = data.get("Number of followers(Min)", "")
        followers_min_input.clear()  # Clear any existing value
        followers_min_input.send_keys(followers_min_data)  # Fill the input field
        followers_min_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        # Profile: Number of followers (Max)
        followers_max_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[label[contains(text(), 'Number of followers')]]//input[@placeholder='Max']",
                )  # Update the XPath as per your actual HTML
            )
        )
        followers_max_data = data.get("Number of followers(Max)", "")
        followers_max_input.clear()  # Clear any existing value
        followers_max_input.send_keys(followers_max_data)  # Fill the input field
        followers_max_input.send_keys(Keys.TAB)  # Move to the next field
        time.sleep(ACTION_DELAY)  # Human delay

        certifications = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//button[@data-slot='disclosure-trigger' and .//p[text()='Certifications']]",
                )  # Update this XPath as per your actual HTML
            )
        )
        safe_click(driver, certifications)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Certifications: Certification keywords
        certifications_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@aria-label='Certification keywords']",
                )  # Update the XPath as per your actual HTML
            )
        )
        certifications_data = data.get("Certification keywords", "").strip()
        if certifications_data:
            safe_focus_and_type(driver, certifications_input, certifications_data)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No 'Certification keywords' data found, skipping this field.")

        languages = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//button[@data-slot='disclosure-trigger' and .//p[text()='Languages']]",
                )  # Update this XPath as per your actual HTML
            )
        )
        safe_click(driver, languages)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Languages: Languages
        languages_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@aria-label='Languages']",
                )  # Update the XPath as per your actual HTML
            )
        )
        languages_data = data.get("Languages", "").strip()
        if languages_data:
            safe_focus_and_type(driver, languages_input, languages_data)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No 'Languages' data found, skipping this field.")
        # Education section
        education_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//button[@data-slot='disclosure-trigger' and .//p[text()='Education']]",
                )  # Update this XPath as per your actual HTML
            )
        )
        safe_click(driver, education_button)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Education: School names
        education_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@aria-label='School names']",
                )  # Update the XPath as per your actual HTML
            )
        )
        education_data = data.get("School names", "").strip()
        if education_data:
            safe_focus_and_type(driver, education_input, education_data)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No 'School names' data found, skipping this field.")

        # Companies section
        companies_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//button[@data-slot='disclosure-trigger' and .//p[text()='Companies']]",
                )  # Update this XPath as per your actual HTML
            )
        )
        safe_click(driver, companies_button)  # Using JavaScript to click
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Companies: Companies
        companies_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@aria-label='Companies']",
                )  # Update the XPath as per your actual HTML
            )
        )
        companies_data = data.get("Companies", "").strip()
        if companies_data:
            safe_focus_and_type(driver, companies_input, companies_data)
            human_delay(ACTION_DELAY, ACTION_DELAY)
        else:
            logging.info("No 'Companies' data found, skipping this field.")

        # Limit field
        limit_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//label[contains(text(),'Limit')]//following-sibling::div//input",
                )
            )
        )
        limit_value = data.get("Limit")
        limit_input.clear()  # Clear any pre-filled value
        limit_input.send_keys(limit_value)
        logging.info(f"Entered '{limit_value}' in the 'Limit' input.")

        # Limit per company field
        limit_per_company_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//label[contains(text(),'Limit per company')]//following-sibling::div//input",
                )
            )
        )
        limit_per_company_value = data.get("Limit per company")
        limit_per_company_input.clear()  # Clear any pre-filled value
        limit_per_company_input.send_keys(limit_per_company_value)
        logging.info(
            f"Entered '{limit_per_company_value}' in the 'Limit per company' input."
        )

        # Human delay (if needed)
        human_delay(ACTION_DELAY, ACTION_DELAY)

        next_buttons = driver.find_elements(By.XPATH, "//button[@type='button'][contains(text(),'Next')]")
        if next_buttons:
            safe_click(driver, next_buttons[0])
            logging.info("Clicked the 'Next' button.")
            time.sleep(10)
        else:
            logging.warning('No results found. Try simplifying your search or using fewer filters.')
            driver.quit()
            sys.exit(0) 

        save_buttons = driver.find_elements(By.XPATH, "//button[contains(text(),'Save and run')]")
        clicked = False

        for btn in save_buttons:
            btn_text = btn.text.strip()
            if btn_text == "Save and run 10 rows":
                human_delay(ACTION_DELAY, ACTION_DELAY)
                btn.click()
                human_delay(PAGE_LOAD_DELAY, PAGE_LOAD_DELAY)
                logging.info("Clicked the 'Save and run 10 rows' button.")
                clicked = True
                break

        if not clicked:
            logging.error("Save button with text 'Save and run 10 rows' not found. Closing browser.")
            driver.quit()
            sys.exit(0) 

        # Step 2: Click on "Add Enrichment"
        # actions_button = WebDriverWait(driver, 10).until(
        #     EC.element_to_be_clickable(
        #         (By.XPATH, "//button[contains(.,'Actions') and @type='button']")
        #     )
        # )
        # actions_button.click()
        # logging.info("Clicked on 'Actions' button.")
        # human_delay(ACTION_DELAY, ACTION_DELAY)

        # Directly search & select "Find Work Email"
        search_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@aria-label='Search with AI']")
            )
        )
        search_input.clear()
        search_input.send_keys("Find Work Email , LeadMagic")
        search_input.send_keys(Keys.RETURN)
        logging.info("Searched for 'Find Work Email'")
        # Locate the "Find Work Email" button in the grid

        find_work_email_option = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((
            By.XPATH,
            "//button[.//p[normalize-space(text())='Find work email'] and .//p[contains(text(),'LeadMagic')]]"
        ))
    )

        safe_click(driver, find_work_email_option)
        human_delay(
            PAGE_LOAD_DELAY, PAGE_LOAD_DELAY
        )  # Click using the safe_click method
        logging.info("Selected 'Find Work Email' from the dropdown")

        continue_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[normalize-space(text())='Continue to add fields']")
            )
        )
        continue_button.click()
        human_delay(PAGE_LOAD_DELAY, PAGE_LOAD_DELAY)

        # Wait for the Save button to be clickable and then click it
        save_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[normalize-space(text())='Save']")
            )
        )
        save_button.click()
        human_delay(PAGE_LOAD_DELAY, PAGE_LOAD_DELAY)
        logging.info("Clicked on 'Save' button.")
        # Searching and clicking the "Save and run" button dynamically
        save_buttons = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@aria-label='Save and run all rows']")
            )
        )
        save_buttons.click()
        logging.info("Clicked on 'Save and run <number> rows' button.")

        human_delay(PAGE_LOAD_DELAY, PAGE_LOAD_DELAY)
        time.sleep(120)


        # handle_enrichment_and_save(driver)
        handle_export_and_download(driver)

        # Additional actions
        time.sleep(SCRAPE_DELAY)

    except Exception as e:
        logging.error(f"Error while filling form fields: {e}")
        pass  # Continue the flow, even if an error occurs

        time.sleep(PAGE_LOAD_DELAY)


def handle_export_and_download(driver):
    try:
        # Wait for the "Actions" button and click it
        actions_button = WebDriverWait(driver, 200).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[normalize-space(text())='Actions']")
            )
        )
        actions_button.click()
        logging.info("Clicked on 'Actions' button.")
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Wait for the "Export" option to be available and click it
        export_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//button[@role='menuitem' and contains(., 'Export')]"
            ))
        )
        safe_click(driver, export_button)  # Using JavaScript to click
        logging.info("Clicked on 'Export' button.")
        human_delay(ACTION_DELAY, ACTION_DELAY)
        time.sleep(PAGE_LOAD_DELAY)

        # Wait for the "Download CSV" button to be clickable and click it
        # download_csv_button = WebDriverWait(driver, 10).until(
        #     EC.element_to_be_clickable((By.XPATH, "//button[normalize-space(text())='Download CSV']"))
        # )
        # driver.execute_script("arguments[0].scrollIntoView(true);", download_csv_button)
        # actions = ActionChains(driver)
        # actions.move_to_element(download_csv_button).perform()
        # # Click the "Download CSV" button
        # download_csv_button.click()
        # #safe_click(driver, download_csv_button)
        # #download_csv_button.click()
        # logging.info("Clicked on 'Download CSV' button.")
        # human_delay(ACTION_DELAY, ACTION_DELAY)

        download_csv_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//div[@class='flex w-full flex-row items-center justify-between gap-2']//p[@data-slot='text' and normalize-space(.)='Download CSV']",
                )
            )
        )
        # download_csv_button.click()
        safe_click(driver, download_csv_button)
        logging.info("Clicked on 'Download CSV' button.")
        human_delay(ACTION_DELAY, ACTION_DELAY)

        # Wait for the download dialog box to appear
        logging.info("Waiting for download dialog box to appear...")
        time.sleep(3)  # Allow time for the download dialog to appear

        # Use PyAutoGUI to press 'Enter' (this simulates clicking the 'Save' button in the dialog)
        pyautogui.press("enter")
        logging.info("Pressed 'Enter' to confirm download.")

        # Wait for the download to complete (you can adjust the time based on your network speed)
        time.sleep(10)  # Adjust this based on the expected download time
        download_folder = os.path.join(os.getcwd(), "downloads")
        # delete_yesterdays_file(download_folder)
        download_file = get_latest_downloaded_file(download_folder)
        today = datetime.now().strftime("%Y-%m-%d")  # e.g. "2025-06-23"
        new_name = f"lead-{today}"  # "lead-2025-06-23"
        renamed_file = rename_downloaded_file(download_file, new_name, download_folder)
        logging.info(f"Downloaded file renamed to: {renamed_file}")

    except Exception as e:
        logging.error(f"Error while handling export and download: {e}")
        raise

# def delete_yesterdays_file(downloads_dir):
#     # Get yesterday's date in "YYYY-MM-DD" format
#     yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
#     # Build the path to the file that was created yesterday
#     file_to_delete = os.path.join(downloads_dir, f"lead-{yesterday_date}.csv")
    
#     # Check if the file exists and delete it
#     if os.path.exists(file_to_delete):
#         try:
#             os.remove(file_to_delete)
#             logging.info(f"Successfully deleted yesterday's file: {file_to_delete}")
#         except Exception as e:
#             logging.error(f"Error while deleting file {file_to_delete}: {e}")
#     else:
#         logging.info(f"No file found to delete for yesterday: {file_to_delete}")


def setup_driver():
    options = Options()
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    options.add_argument("--no-sandbox")

    # Set the download directory to your current project folder
    download_path = os.path.join(
        os.getcwd(), "downloads"
    )  # Set the path to the 'downloads' folder in your current project
    if not os.path.exists(download_path):  # Create the folder if it doesn't exist
        os.makedirs(download_path)

    # Configure Chrome options for downloading files
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_path,  # Set the download directory
            "download.prompt_for_download": False,  # Disable the prompt for download
            "download.directory_upgrade": True,  # Ensure the directory is upgraded
        },
    )

    driver = webdriver.Chrome(options=options)
    return driver


def get_latest_downloaded_file(download_folder):
    """
    Function to get the most recently downloaded file from the download directory.
    """
    # Get list of files in the download folder
    files = os.listdir(download_folder)

    # Filter out directories and keep only files
    files = [f for f in files if os.path.isfile(os.path.join(download_folder, f))]

    if not files:
        return None  # No files found

    # Get the full path of the most recently modified file
    latest_file = max(
        files, key=lambda f: os.path.getmtime(os.path.join(download_folder, f))
    )
    return os.path.join(download_folder, latest_file)


def rename_downloaded_file(latest_file, new_name, download_folder):
    """
    Function to rename the downloaded file.
    """
    # Get the latest downloaded file
    # latest_file = get_latest_downloaded_file(download_folder)

    if latest_file:
        # Generate the new file path
        file_extension = os.path.splitext(latest_file)[
            1
        ]  # Get the file extension (e.g., .csv)
        new_file_path = os.path.join(download_folder, f"{new_name}{file_extension}")

        # Rename the file
        os.rename(latest_file, new_file_path)
        print(f"File renamed to: {new_file_path}")
        return new_file_path
    else:
        print("No file found to rename.")
        return None


def main():
    driver = setup_driver()
    login_to_clay(driver)

    # Get data from Google Sheet (search criteria based on today's date)
    data = get_google_sheet_data()

    if not data:
        logging.error("No data found, exiting.")
        return
    
    # Fill out the Clay form
    logging.info("Filling form with data...")
    fill_form_fields(driver, data)
    
    logging.info("Clay form filling completed.")

    # Step to download the CSV after form submission
    logging.info("Downloading CSV from Clay...")
    
    current_dir = os.getcwd()
    downloads_dir = os.path.join(current_dir, "downloads")
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)  
    current_date = datetime.now().strftime("%Y-%m-%d")

    csv_file_path = os.path.join(downloads_dir, f"lead-{current_date}.csv")
    logging.info(f"Processing CSV file: {csv_file_path}")
    
    logging.info("Waiting for some time before pushing data to Zoho...")
    time.sleep(10) 
    
    zoho_api.process_csv_and_push_to_zoho(csv_file_path)  
    
    logging.info("CSV processing completed.")
    
    time.sleep(60) 

    #logging.info("start linkedin-connection")
    #setup.main()  # mutltiple login with parallel processing
    logging.info("Starting test3 main function...")
    
    test3_main()

    time.sleep(60) 

    driver.quit()   

    logging.info("Browser closed, script finished.") 


if __name__ == "__main__":
    main()
