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
    session = pyspark.sql.SparkSession.builder.appName("test-quality-integration").getOrCreate()
    yield session
    session.stop()


def read_table_or_fail(spark, table_name: str):
    try:
        return spark.table(table_name)
    except Exception as exc:
        pytest.fail(
            f"Required table not found: {table_name}. "
            f"Run prerequisite pipeline notebooks/jobs first. Original error: {exc}"
        )
