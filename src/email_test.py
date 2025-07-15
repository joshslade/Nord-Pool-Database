# test_gmail_smtp.py
import os
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText

load_dotenv()

smtp_host     = os.getenv("SMTP_HOST")
smtp_port     = int(os.getenv("SMTP_PORT", "587"))
smtp_user     = os.getenv("SMTP_USER")
smtp_password = os.getenv("SMTP_PASSWORD")
email_from    = os.getenv("EMAIL_FROM")
email_to      = os.getenv("EMAIL_TO").split(",")

# Create a simple test message
subject = "SMTP Test from Python"
body    = "Hello! This is a test email from our alert-checker script."
msg = MIMEText(body)
msg["Subject"] = subject
msg["From"]    = email_from
msg["To"]      = ", ".join(email_to)

try:
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()                  # upgrade to TLS
    server.login(smtp_user, smtp_password)
    server.sendmail(email_from, email_to, msg.as_string())
    server.quit()
    print("✅ Test email sent successfully!")
except Exception as e:
    print("❌ Failed to send test email:", e)