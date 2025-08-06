
import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

load_dotenv()

cfg = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "#Bharathi@16"),
    "database": os.getenv("DB_NAME", "automation"),
    "charset":  "utf8mb4"
}

DDL = """
CREATE DATABASE IF NOT EXISTS automation DEFAULT CHARACTER SET utf8mb4;
USE automation;
CREATE TABLE linkedindata (
profile_id          VARCHAR(50)   PRIMARY KEY,
first_name          VARCHAR(100),
last_name           VARCHAR(100),
us_coast            VARCHAR(100),
request_sent_date   DATETIME,
connection_date     VARCHAR(50),
status_check_date   DATETIME,
connection_request  TEXT,
keyword             VARCHAR(100),
saved_search_name   VARCHAR(100),
number              VARCHAR(50),
invite_message      TEXT,
request_sent_from   VARCHAR(100),
linkedin_url        VARCHAR(255),
email_id            VARCHAR(100),
designation         VARCHAR(100),
about               TEXT,
no_of_followers     VARCHAR(50),
no_of_connections   VARCHAR(50),
company_name        VARCHAR(100),
company_url         VARCHAR(255),
country             VARCHAR(100),
location            VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS education_table (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    profile_id        VARCHAR(50) NOT NULL,
    linkedin_url      VARCHAR(255),
    sections          TEXT,
    education_entry   TEXT,
    FOREIGN KEY (profile_id)
    REFERENCES linkedindata(profile_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS experience_table (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    profile_id        VARCHAR(50) NOT NULL,
    linkedin_url      VARCHAR(255),
    sections          TEXT,
    experience_entry  TEXT,
    FOREIGN KEY (profile_id)
    REFERENCES linkedindata(profile_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;
"""

def main():
    try:
        conn = mysql.connector.connect(**cfg)
        cursor = conn.cursor()
        for stmt in DDL.split(";"):
            if stmt.strip():
                cursor.execute(stmt + ";")
        print("Database & tables ready.")
    except mysql.connector.Error as e:
        print(" Error:", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
