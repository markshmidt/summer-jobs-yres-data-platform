import os
import json
import urllib.request
import pytest
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_CREDS_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/google-credentials.json"
)


@pytest.fixture
def gsheets_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
    return gspread.authorize(creds)


@pytest.fixture
def databricks_env():
    return {
        "host": os.environ["DATABRICKS_HOST"],
        "token": os.environ["DATABRICKS_TOKEN"],
        "warehouse_id": os.environ["DATABRICKS_SQL_WAREHOUSE_ID"],
    }


class TestDatabricksConnection:
    def test_clusters_endpoint_reachable(self, databricks_env):
        req = urllib.request.Request(
            f"{databricks_env['host']}/api/2.0/clusters/list",
            headers={"Authorization": f"Bearer {databricks_env['token']}"},
        )
        resp = urllib.request.urlopen(req)
        assert resp.status == 200

    def test_sql_warehouse_exists(self, databricks_env):
        req = urllib.request.Request(
            f"{databricks_env['host']}/api/2.0/sql/warehouses/{databricks_env['warehouse_id']}",
            headers={"Authorization": f"Bearer {databricks_env['token']}"},
        )
        data = json.loads(urllib.request.urlopen(req).read())
        assert "id" in data
        assert data["id"] == databricks_env["warehouse_id"]

    def test_token_is_valid(self, databricks_env):
        req = urllib.request.Request(
            f"{databricks_env['host']}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {databricks_env['token']}"},
        )
        data = json.loads(urllib.request.urlopen(req).read())
        assert "userName" in data


class TestGoogleSheetsConnection:
    def test_can_open_sheet(self, gsheets_client):
        sheet = gsheets_client.open_by_key(os.environ["GOOGLE_SHEET_ID"])
        assert sheet.title is not None

    def test_sheet_has_expected_columns(self, gsheets_client):
        sheet = gsheets_client.open_by_key(os.environ["GOOGLE_SHEET_ID"])
        headers = sheet.sheet1.row_values(1)
        assert len(headers) > 0
        assert "Amount Paid" in headers[4] or "Montant" in headers[4]

    def test_sheet_has_data_rows(self, gsheets_client):
        sheet = gsheets_client.open_by_key(os.environ["GOOGLE_SHEET_ID"])
        all_values = sheet.sheet1.get_all_values()
        assert len(all_values) > 1, "Sheet has no data rows beyond the header"
