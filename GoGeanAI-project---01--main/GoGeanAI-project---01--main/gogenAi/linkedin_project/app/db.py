import mysql.connector
from config import DB_CFG

def init_db():
    return mysql.connector.connect(**DB_CFG)

def insert_linkedindata(cursor, data: dict):
    sql = """
    INSERT INTO linkedindata
    (profile_id, first_name, last_name, us_coast,
     request_sent_date, connection_date, status_check_date,
     connection_request, keyword, saved_search_name, number,
     invite_message, request_sent_from, linkedin_url,
     email_id, designation, about,
     no_of_followers, no_of_connections,
     company_name, company_url, country, location)
    VALUES
    (%(profile_id)s, %(first_name)s, %(last_name)s, %(us_coast)s,
     %(request_sent_date)s, %(connection_date)s, %(status_check_date)s,
     %(connection_request)s, %(keyword)s, %(saved_search_name)s, %(number)s,
     %(invite_message)s, %(request_sent_from)s, %(linkedin_url)s,
     %(email_id)s, %(designation)s, %(about)s,
     %(no_of_followers)s, %(no_of_connections)s,
     %(company_name)s, %(company_url)s, %(country)s, %(location)s)
    ON DUPLICATE KEY UPDATE
      about = VALUES(about),
      no_of_followers = VALUES(no_of_followers),
      no_of_connections = VALUES(no_of_connections)
    """
    cursor.execute(sql, data)

def insert_education_records(cursor, profile_id, linkedin_url, edu_list):
    sql = """
    INSERT INTO education_table 
    (profile_id, linkedin_url, sections, education_entry)
    VALUES (%s, %s, %s, %s)
    """
    for i, edu in enumerate(edu_list, start=1):
        section_label = f"{profile_id}_{i}"
        # serialize the dict to a string
        entry_text = " | ".join(filter(None, [
            edu.get('school', '').strip(),
            edu.get('degree', '').strip(),
            edu.get('dates', '').strip(),
            edu.get('description', '').strip(),
        ]))
        cursor.execute(sql, (profile_id, linkedin_url, section_label, entry_text))

def insert_experience_records(cursor, profile_id, linkedin_url, exp_list):
    sql = """
    INSERT INTO experience_table
    (profile_id, linkedin_url, sections, experience_entry)
    VALUES (%s, %s, %s, %s)
    """
    for i, exp in enumerate(exp_list, start=1):
        section_label = f"{profile_id}_{i}"
        # if exp is already a string, use it; if it's a dict, serialize it:
        if isinstance(exp, dict):
            entry_text = exp.get('header', '') + "\n" + "\n".join(exp.get('bullets', []))
        else:
            entry_text = exp
        cursor.execute(sql, (profile_id, linkedin_url, section_label, entry_text))

