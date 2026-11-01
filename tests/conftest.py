"""
Shared fixtures for the EpiScope test suite (ENG-4).

A single Spark session is built once per test run: starting one costs several
seconds, and the suite would otherwise spend more time on JVM startup than on
assertions.

Tests that need a lake are skipped when none is present rather than failing,
so `make test` is useful on a fresh clone before `make all` has run. Tests
that need no lake, which is most of the quality and privacy logic, always
run.
"""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

DATA = os.environ.get("EPISCOPE_DATA_ROOT", os.path.join(ROOT, "episcope_data"))
if not os.path.isdir(DATA):
    alt = "/content/episcope_data"
    if os.path.isdir(alt):
        DATA = alt


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.appName("EpiScope-tests")
         .master("local[2]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "Asia/Dhaka")
         .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def data_root():
    return DATA


@pytest.fixture(scope="session")
def gold(spark, data_root):
    p = os.path.join(data_root, "lake", "gold", "weekly_incidence")
    if not os.path.isdir(p):
        pytest.skip("no gold layer; run `make all` first")
    return spark.read.parquet(p)


@pytest.fixture(scope="session")
def districts():
    import csv
    for base in [os.environ.get("EPISCOPE_PUBLIC_ROOT", ""),
                 os.path.join(ROOT, "episcope_public"),
                 "/content/drive/MyDrive/episcope-public"]:
        if not base:
            continue
        f = os.path.join(base, "S1_district_reference_enriched.csv")
        if os.path.exists(f):
            return list(csv.DictReader(open(f)))
    pytest.skip("district reference not found; run `make public` first")
