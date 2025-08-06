# LinkedIn Automation Suite

A robust Python automation tool for LinkedIn profile connection, scraping, and enrichment, featuring:
- Multi-account support with encrypted credentials and persistent sessions (cookies)
- Dynamic batch processing
- Integration with Google Sheets (input and status tracking)
- MySQL storage of all enriched/scraped data
- GPT-powered message generation for connection requests
- Full profile extraction: name, title, company, email, followers, connections, about, experience, education
- Human-like Selenium automation with anti-bot delays and error handling

---

## Features

- **Multi-Account Management:** Securely handle multiple LinkedIn logins using encrypted credentials (`Fernet`).
- **Batch Processing:** Assigns profile URLs to accounts in round-robin batches for parallel processing.
- **Smart Connection Requests:** Sends AI-generated, context-aware messages for each connection.
- **Data Deduplication:** Avoids sending duplicate requests or scraping the same profile twice.
- **Resilient Scraping:** Handles connection states (pending, declined, already connected) and page errors robustly.
- **Google Sheets Integration:** Reads input profiles and writes back statuses in real-time.
- **MySQL Storage:** Persists all enriched and scraped data, including education and experience history.
- **Status Checking:** Rechecks pending requests and updates their status in both DB and Sheet.
- **Human-like Automation:** Random delays, scrolling, typing, and action sequences to mimic human behavior.

---

## Prerequisites

- Python 3.8+
- Chrome browser (or update driver logic for Edge/Firefox)
- [Google Cloud Service Account](https://developers.google.com/identity/protocols/oauth2/service-account) (JSON key for Sheets API)
- MySQL database (schema as per `linkedindata`, `education_table`, `experience_table`)
- OpenAI API key (for GPT connection messages)

---

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/linkedin-automation-suite.git
    cd linkedin-automation-suite
    ```

2. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3. **Google Service Account Setup:**
    - Obtain your service account key file from Google Cloud Console.
    - Rename it to `secret_key.json` and place it in the root directory.

4. **MySQL Setup:**
    - Create your database and required tables (see schema in `/db/schema.sql` or comments).

5. **Environment Variables (.env):**
    - Create a `.env` file based on `.env.example`.
    - Use Fernet to encrypt your LinkedIn credentials (see `encrypt_credentials.py`).
    - Example:
      ```
      SECRET_KEY=your_fernet_key
      LINKEDIN_USERNAME_ACCOUNT1=... (encrypted)
      LINKEDIN_PASSWORD_ACCOUNT1=... (encrypted)
      BATCH_SIZE=5
      INPUT_SHEET_ID=your_google_sheet_id
      INPUT_SHEET_NAME=Sheet1
      SCRAPE_DELAY=6
      JITTER=3
      OPENAI_API_KEY=sk-...
      ```
    - You can add as many accounts as needed with `LINKEDIN_USERNAME_ACCOUNT2`, etc.

6. **Message Templates:**
    - Edit `scripts/messages.json` to set fallback messages and keywords for job titles.

---

## Usage

### Initial Run

- **First-time only:**  
  Run the script to log in manually and save cookies for each account.
  ```bash
  python main.py
