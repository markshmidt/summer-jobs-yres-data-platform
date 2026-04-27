"""
Test SMTP email alerting.

Usage:
    source .venv/bin/activate
    python -m pytest tests/test_email.py -v
"""

import os
import smtplib
import pytest
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", SMTP_USER)


@pytest.fixture(scope="session")
def smtp_connection():
    """Connect to Gmail SMTP and authenticate."""
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASSWORD)
    yield server
    server.quit()


class TestEmail:
    def test_smtp_credentials_set(self):
        assert SMTP_USER, "SMTP_USER not set in .env"
        assert SMTP_PASSWORD, "SMTP_PASSWORD not set in .env"

    def test_smtp_login(self, smtp_connection):
        # If we got here, login succeeded in the fixture
        assert smtp_connection.noop()[0] == 250

    def test_send_alert(self, smtp_connection):
        msg = MIMEText("Test alert from CSJ Pipeline. Email alerting works!")
        msg["Subject"] = "CSJ Pipeline - Test Alert"
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL

        errors = smtp_connection.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())
        assert errors == {}, f"Send failed: {errors}"
