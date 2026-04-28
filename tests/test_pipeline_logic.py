"""
Test the data transformation logic locally using pandas.
No Spark or Databricks needed — validates the same logic
that the notebooks will run on real data.

Usage:
    # From project root, using the test venv:
    /tmp/gsheet-test/bin/python -m pytest tests/test_pipeline_logic.py -v

    # Or with any Python env that has pandas, gspread, google-auth, pyarrow:
    pip install pandas pyarrow gspread google-auth pytest
    python -m pytest tests/test_pipeline_logic.py -v
"""

import os
import json
import pytest
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "google-credentials.json")
PARQUET_CACHE = "/tmp/raw_funding.parquet"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def raw_df():
    """Load data from cached parquet or fetch fresh from Google Sheets."""
    if os.path.exists(PARQUET_CACHE):
        return pd.read_parquet(PARQUET_CACHE)

    with open(CREDENTIALS_PATH) as f:
        creds_dict = json.load(f)
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ])
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key("1NfZzvZ0YhizKE1cAID_mnkZwz5bO0icdw1Ax5tS6DWE")
    data = sheet.worksheet("AB, BC, ON").get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    df.to_parquet(PARQUET_CACHE, index=False)
    return df


@pytest.fixture(scope="session")
def silver_df(raw_df):
    """Simulate silver layer: type casting, dedup, computed columns."""
    df = raw_df.copy()
    df.columns = [
        "program_year", "region", "riding",
        "organization_name", "amount_paid", "jobs_created",
    ]
    df["amount_paid"] = pd.to_numeric(df["amount_paid"], errors="coerce")
    df["jobs_created"] = pd.to_numeric(df["jobs_created"], errors="coerce")
    df["program_year"] = pd.to_numeric(df["program_year"], errors="coerce")
    df["row_id"] = range(len(df))
    df["avg_salary"] = df.apply(
        lambda r: r["amount_paid"] / r["jobs_created"] if r["jobs_created"] > 0 else None,
        axis=1,
    )
    return df


@pytest.fixture(scope="session")
def gold_df(silver_df):
    """Simulate gold layer: per-org per-riding per-year aggregation."""
    valid = silver_df[silver_df["jobs_created"] > 0].copy()
    gold = valid.groupby(["region", "riding", "program_year", "organization_name"]).agg(
        total_funding=("amount_paid", "sum"),
        total_jobs=("jobs_created", "sum"),
        grant_count=("amount_paid", "count"),
    ).reset_index()
    gold["avg_salary"] = gold["total_funding"] / gold["total_jobs"]
    return gold


# ---------------------------------------------------------------------------
# Bronze layer tests
# ---------------------------------------------------------------------------

class TestBronze:
    def test_row_count(self, raw_df):
        assert len(raw_df) > 100_000, f"Expected 100k+ rows, got {len(raw_df)}"

    def test_column_count(self, raw_df):
        assert len(raw_df.columns) == 6

    def test_no_empty_headers(self, raw_df):
        for col in raw_df.columns:
            assert col.strip() != "", f"Empty column header found"

    def test_all_columns_are_strings(self, raw_df):
        for col in raw_df.columns:
            assert raw_df[col].dtype == "object" or "str" in str(raw_df[col].dtype).lower()


# ---------------------------------------------------------------------------
# Silver layer tests
# ---------------------------------------------------------------------------

class TestSilver:
    def test_no_nulls_in_key_columns(self, silver_df):
        for col in ["program_year", "region", "riding"]:
            nulls = silver_df[col].isna().sum()
            assert nulls == 0, f"{col} has {nulls} nulls"

    def test_no_negative_funding(self, silver_df):
        negatives = silver_df[silver_df["amount_paid"] < 0]
        assert len(negatives) == 0, f"{len(negatives)} rows with negative funding"

    def test_amount_paid_is_numeric(self, silver_df):
        assert silver_df["amount_paid"].dtype in ("float64", "int64")

    def test_jobs_created_is_numeric(self, silver_df):
        assert silver_df["jobs_created"].dtype in ("float64", "int64")

    def test_year_range(self, silver_df):
        years = silver_df["program_year"].dropna().unique()
        assert min(years) >= 2017
        assert max(years) <= 2025

    def test_expected_provinces(self, silver_df):
        regions = set(silver_df["region"].unique())
        assert "Alberta" in regions
        assert "Ontario" in regions
        assert any("British Columbia" in r for r in regions)

    def test_zero_funding_with_jobs_flagged(self, silver_df):
        """These rows exist in the data — silver should be aware of them."""
        bad = silver_df[(silver_df["amount_paid"] == 0) & (silver_df["jobs_created"] > 0)]
        print(f"\n  INFO: {len(bad)} rows have $0 funding but jobs > 0")
        assert len(bad) < 500, f"Unexpectedly many zero-funding rows: {len(bad)}"

    def test_avg_salary_reasonable(self, silver_df):
        valid = silver_df[silver_df["avg_salary"].notna()]
        median = valid["avg_salary"].median()
        assert 1000 < median < 20000, f"Median avg salary is ${median:,.0f} — suspicious"

    def test_high_salary_outliers_are_few(self, silver_df):
        outliers = silver_df[silver_df["avg_salary"] > 50_000]
        assert len(outliers) < 10, f"{len(outliers)} rows with avg salary > $50k"


# ---------------------------------------------------------------------------
# Gold layer tests
# ---------------------------------------------------------------------------

class TestSilverRowId:
    def test_row_id_unique(self, silver_df):
        assert silver_df["row_id"].is_unique, "row_id should be unique per row"

    def test_all_rows_preserved(self, raw_df, silver_df):
        assert len(silver_df) == len(raw_df), "Silver should keep all rows (no dedup)"


class TestGold:
    def test_has_org_level_rows(self, gold_df):
        assert len(gold_df) > 100_000, f"Expected 100k+ org-level rows, got {len(gold_df)}"

    def test_org_column_present(self, gold_df):
        assert "organization_name" in gold_df.columns

    def test_multi_grant_orgs_combined(self, gold_df):
        multi = gold_df[gold_df["grant_count"] > 1]
        assert len(multi) > 0, "Expected some orgs with multiple grants combined"

    def test_no_negative_aggregations(self, gold_df):
        assert (gold_df["total_funding"] >= 0).all()
        assert (gold_df["total_jobs"] > 0).all()

    def test_avg_salary_matches(self, gold_df):
        recalc = gold_df["total_funding"] / gold_df["total_jobs"]
        diff = (gold_df["avg_salary"] - recalc).abs().max()
        assert diff < 0.01, f"avg_salary doesn't match total_funding/total_jobs (diff={diff})"

    def test_three_provinces(self, gold_df):
        assert gold_df["region"].nunique() == 3

    def test_nine_years(self, gold_df):
        assert gold_df["program_year"].nunique() == 9
