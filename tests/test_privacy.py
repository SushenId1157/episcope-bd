"""
Privacy tests (PRV-1, PRV-2).

PRV-1 is tested structurally: the de-identified frame must not carry the
identifier at all, rather than carrying it unused. PRV-2 is tested on what
would actually ship, because suppression correct in intent can still fail in
serialisation.
"""
import datetime
import json
import os

import pytest
from pyspark.sql import Row

from pipeline.silver.build import deidentify
import serving.export as ex


SALT = "0" * 64


def test_patient_id_is_removed_not_merely_unused(spark):
    """
    PRV-1 states direct identifiers never go beyond bronze. A column that is
    present but ignored would still travel into silver, the quarantine table
    and any extract taken from them.
    """
    df = spark.createDataFrame([Row(patient_id="BD12345678", age_years=34)])
    out = deidentify(df, SALT)
    assert "patient_id" not in out.columns
    assert "patient_hash" in out.columns


def test_hash_is_salted_and_full_length(spark):
    df = spark.createDataFrame([Row(patient_id="BD12345678", age_years=34)])
    h1 = deidentify(df, SALT).first()["patient_hash"]
    h2 = deidentify(df, "f" * 64).first()["patient_hash"]
    assert len(h1) == 64
    assert h1 != h2, "a different salt must give a different hash"


def test_same_patient_hashes_consistently(spark):
    """Cross-facility de-duplication (DQ-4) depends on this."""
    df = spark.createDataFrame([Row(patient_id="BD11111111", age_years=20),
                                Row(patient_id="BD11111111", age_years=20)])
    hs = [r["patient_hash"] for r in deidentify(df, SALT).collect()]
    assert hs[0] == hs[1]


def test_age_is_reduced_to_a_band(spark):
    df = spark.createDataFrame([Row(patient_id="BD1", age_years=7),
                                Row(patient_id="BD2", age_years=70)])
    bands = [r["age_band"] for r in deidentify(df, SALT).collect()]
    assert bands == ["5-14", "65+"]


def test_small_counts_are_suppressed():
    """PRV-2: no count from 1 to 4 may appear."""
    vals, total, audit = ex.suppress_series([10, 3, 25, 1], total=39)
    assert vals[1] is None and vals[3] is None
    assert vals[0] == 10 and vals[2] == 25


def test_zero_is_not_suppressed():
    """Zero discloses nothing about a patient and must stay visible."""
    vals, _, _ = ex.suppress_series([0, 10, 20], total=30)
    assert vals[0] == 0


def test_a_lone_suppressed_cell_is_not_recoverable_by_subtraction():
    """
    PRV-2's second clause. With one cell hidden and the total published,
    the hidden value is exactly total minus the rest, so a second cell must
    be withheld as well.
    """
    vals, total, audit = ex.suppress_series([100, 3, 50], total=153)
    hidden = sum(1 for v in vals if v is None)
    assert hidden >= 2, "one hidden cell plus a total is exactly recoverable"
    assert audit["complementary"], "no complementary suppression was applied"


def test_derived_rates_do_not_leak_a_suppressed_count():
    """
    Incidence multiplied by population recovers the count as surely as
    subtraction does, so a rate derived from a hidden value must be hidden
    with it.
    """
    records = [{"cases": None, "incidence_per_100k": 1.4, "population": 214000}]
    leaks = ex.audit_derivable_counts(records)
    assert leaks, "a published rate beside a suppressed count was not flagged"

    records_ok = [{"cases": None, "incidence_per_100k": None}]
    assert not ex.audit_derivable_counts(records_ok)


def test_the_audit_does_not_confuse_an_index_for_a_count():
    """
    An earlier audit flagged every integer in 1..4 anywhere in the payload
    and returned 1,966 findings, almost all week numbers. A check that
    cannot tell a count from an index provides no assurance.
    """
    payload = {"epi_week": 3, "horizon": 2, "alert_weeks": 1,
               "cases": 500}
    assert ex.audit_no_small_counts(payload) == []

    payload_bad = {"epi_week": 3, "cases": 2}
    assert ex.audit_no_small_counts(payload_bad)


@pytest.mark.parametrize("name", [
    "map_districts.json", "seasonal_heatmap.json", "early_warning.json",
    "timeseries.json",
])
def test_the_shipped_dashboard_files_are_clean(name):
    """
    The decisive test: what is actually on disk for the browser to fetch.
    Skipped if the export has not been run.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p = os.path.join(root, "dashboard", "data", name)
    if not os.path.exists(p):
        pytest.skip(f"{name} not exported; run `make serve` first")
    bad = ex.audit_no_small_counts(json.load(open(p)), name)
    assert not bad, f"PRV-2 violations in {name}: {bad[:5]}"


def test_payload_is_within_the_browser_budget():
    """VIS-4 caps the total the browser loads at 10 MB."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    d = os.path.join(root, "dashboard", "data")
    if not os.path.isdir(d):
        pytest.skip("no exports; run `make serve` first")
    total = sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d))
    assert total / 1024 / 1024 <= 10.0, f"payload is {total/1024/1024:.2f} MB"
