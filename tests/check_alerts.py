# check_alerts.py

import os
from datetime import date
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import smtplib
from email.mime.text import MIMEText

# Load environment variables from a local .env file (if it exists)
load_dotenv()

# Build the database URL for SQLAlchemy
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Example: postgresql+psycopg2://user:password@host:port/dbname
# --- Construct database URL ---
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Create engine and query ---
engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"})


def get_alert_rows():
    """
    Connect via SQLAlchemy and fetch any rows from ext.api_daily_alerts
    whose alert_date = today. Returns a list of (alert_id, issues_list) tuples.
    """
    query = text("""
        SELECT alert_id, issues
          FROM ext.api_daily_alerts
         WHERE alert_date = :today
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"today": date.today()})
        # result.fetchall() returns a list of Row objects; Row[1] (issues) becomes a Python list
        rows = result.fetchall()
    return rows  # each row is a SQLAlchemy Row: (alert_id, issues_list)

def success():
    subject = f"[SUCCESS] API successfully run for {date.today().isoformat()}"
    body = "No issues detected"

    return body, subject


def alert(all_issues):
    
    subject = f"[ALERT] API Daily Issues for {date.today().isoformat()}"
    body_lines = [
        f"The following issues were logged on {date.today().isoformat()}:",
        ""
    ]
    for idx, issue in enumerate(all_issues, start=1):
        body_lines.append(f"{idx}. {issue}")
    body = "\n".join(body_lines)

    return body, subject





def send_email(body, subject):
    """
    all_issues: a flat list of strings (e.g. ["2: never processed", "5: parser-error (...)"])
    Sends a single email that enumerates each issue.
    """
    smtp_host     = os.getenv("SMTP_HOST")
    smtp_port     = int(os.getenv("SMTP_PORT", "587"))
    smtp_user     = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from    = os.getenv("EMAIL_FROM")
    email_to      = os.getenv("EMAIL_TO").split(",")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = email_from
    msg["To"]      = ", ".join(email_to)

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, email_to, msg.as_string())
        server.quit()
        print("✅ Email sent successfully!")
    except Exception as e:
        print("❌ Failed to send email:", e)



def main():
    rows = get_alert_rows()
    if not rows:
        print(f"[{date.today().isoformat()}] No alerts found.")
        body, subject = success()
        send_email(body, subject)
        return

    # Flatten all issues arrays from multiple alert rows
    all_issues = []
    for _alert_id, issue_list in rows:
        # issue_list is a Python list (Postgres text[] → SQLAlchemy → list[str])
        all_issues.extend(issue_list or [])

    if all_issues:
        body, subject = alert(all_issues)
        send_email(body, subject)
        print(f"Sent email with {len(all_issues)} issue(s).")
    else:
        print("Found alert rows, but `issues[]` was empty—no email sent.")


if __name__ == "__main__":
    main()
