import os
import sys

import pytest


# Add project root to path so config/src modules are importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.paths import DataPaths


@pytest.fixture(scope="session")
def paths():
    return DataPaths("dev")


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    spark_session_cls = pyspark.sql.SparkSession

    # 1) Prefer an existing active session (Databricks notebook context)
    session = spark_session_cls.getActiveSession()
    created_by_fixture = False

    # 2) Fallback: check notebook user namespace for pre-created `spark`
    if session is None:
        try:
            from IPython import get_ipython

            ipy = get_ipython()
            if ipy is not None:
                candidate = ipy.user_ns.get("spark")
                if candidate is not None:
                    session = candidate
        except Exception:
            session = None

    # 3) Final fallback: create a session (works in local/dev contexts)
    if session is None:
        try:
            session = spark_session_cls.builder.appName("test-quality-integration").getOrCreate()
            created_by_fixture = True
        except Exception as exc:
            pytest.skip(
                "Integration tests require an active Databricks Spark session. "
                "The current test runner does not provide a valid Spark context. "
                f"Original error: {exc}"
            )

    yield session

    # Stop only sessions that were created by this fixture.
    if created_by_fixture:
        session.stop()


def read_table_or_fail(spark, table_name: str):
    try:
        return spark.table(table_name)
    except Exception as exc:
        pytest.fail(
            f"Required table not found: {table_name}. "
            f"Run prerequisite pipeline notebooks/jobs first. Original error: {exc}"
        )
