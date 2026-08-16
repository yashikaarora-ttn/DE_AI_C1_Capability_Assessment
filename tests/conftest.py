"""Shared pytest fixtures for Spark-based tests."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("medallion-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session
