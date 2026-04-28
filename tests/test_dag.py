"""
Test that the DAG file parses correctly and has the expected structure.
Does not require Airflow running — imports the module directly.

Usage:
    source .venv/bin/activate
    python -m pytest tests/test_dag.py -v
"""

import importlib
import os
import sys
import types
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="session")
def dag():
    """Load the DAG module with Airflow imports mocked out."""
    # Stub out airflow imports so tests work without Airflow installed
    airflow_stub = types.ModuleType("airflow")
    airflow_models = types.ModuleType("airflow.models")
    airflow_operators = types.ModuleType("airflow.operators")
    airflow_operators_python = types.ModuleType("airflow.operators.python")

    class FakeDAG:
        def __init__(self, **kwargs):
            self.dag_id = kwargs.get("dag_id")
            self.default_args = kwargs.get("default_args", {})
            self.tasks = []
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class FakePythonOperator:
        def __init__(self, **kwargs):
            self.task_id = kwargs.get("task_id")
            self.python_callable = kwargs.get("python_callable")
        def __rshift__(self, other): return other

    airflow_stub.DAG = FakeDAG
    airflow_operators_python.PythonOperator = FakePythonOperator

    sys.modules["airflow"] = airflow_stub
    sys.modules["airflow.models"] = airflow_models
    sys.modules["airflow.operators"] = airflow_operators
    sys.modules["airflow.operators.python"] = airflow_operators_python

    spec = importlib.util.spec_from_file_location(
        "csj_pipeline_dag", "dags/csj_pipeline_dag.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module

    # Cleanup stubs
    for key in ["airflow", "airflow.models", "airflow.operators", "airflow.operators.python"]:
        sys.modules.pop(key, None)


class TestDAGStructure:
    def test_dag_id(self, dag):
        assert dag.dag.dag_id == "csj_pipeline"

    def test_has_extract_function(self, dag):
        assert callable(dag.extract_and_upload)

    def test_has_run_bronze_function(self, dag):
        assert callable(dag.run_bronze)

    def test_has_run_silver_function(self, dag):
        assert callable(dag.run_silver)

    def test_has_run_gold_function(self, dag):
        assert callable(dag.run_gold)

    def test_notebook_base_path(self, dag):
        assert "/notebooks" in dag.NOTEBOOK_BASE

    def test_dbfs_upload_path(self, dag):
        assert dag.DBFS_UPLOAD_PATH.endswith(".parquet")

    def test_email_alerting_enabled(self, dag):
        assert dag.default_args["email_on_failure"] is True
        assert len(dag.default_args["email"]) > 0
