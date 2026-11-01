"""
Quality rule tests (DQ-1).

Each rule is exercised on a row built to break exactly that rule, so a test
failure names the rule that regressed rather than reporting that something,
somewhere, changed.
"""
import datetime

import pytest
from pyspark.sql import Row, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DateType, BooleanType)

from pipeline.quality.rules import encounter_rules, apply_rules


DISTRICTS = ["D01", "D02"]


def _row(**over):
    base = dict(
        encounter_id="E-D01-2023-30-dengue-000001",
        patient_hash="a" * 64,
        facility_id="F0001",
        district_id="D01",
        onset_date=datetime.date(2023, 7, 20),
        report_date=datetime.date(2023, 7, 24),
        age_years=34,
        sex="M",
        icd10_code="A90",
        disease_group="dengue",
        outcome="recovered",
        _facility_exists=True,
        _dup_rank=1,
        _batch_id="B1",
    )
    base.update(over)
    return Row(**base)


SCHEMA = StructType([
    StructField("encounter_id", StringType()),
    StructField("patient_hash", StringType()),
    StructField("facility_id", StringType()),
    StructField("district_id", StringType()),
    StructField("onset_date", DateType()),
    StructField("report_date", DateType()),
    StructField("age_years", IntegerType()),
    StructField("sex", StringType()),
    StructField("icd10_code", StringType()),
    StructField("disease_group", StringType()),
    StructField("outcome", StringType()),
    StructField("_facility_exists", BooleanType()),
    StructField("_dup_rank", IntegerType()),
    StructField("_batch_id", StringType()),
])


def _codes(spark, row):
    # An explicit schema, because a row carrying a null in the field under
    # test gives Spark nothing to infer that column's type from.
    df = spark.createDataFrame([row], schema=SCHEMA)
    rules = encounter_rules(DISTRICTS, "2021-01-01", "2025-12-31")
    _, quarantined, _ = apply_rules(df, rules)
    rows = quarantined.collect()
    return set(rows[0]["_quarantine_reasons"].split(",")) if rows else set()


def test_valid_row_passes_every_rule(spark):
    assert _codes(spark, _row()) == set()


@pytest.mark.parametrize("field,value,code", [
    ("age_years", 212, "DQ03_AGE_RANGE"),
    ("age_years", -1, "DQ03_AGE_RANGE"),
    ("sex", "?", "DQ04_SEX_DOMAIN"),
    ("district_id", "D99", "DQ05_DISTRICT_FK"),
    ("_facility_exists", False, "DQ06_FACILITY_FK"),
    ("icd10_code", "Z99", "DQ12_ICD10_VALID"),
    ("outcome", "maybe", "DQ13_OUTCOME_DOMAIN"),
    ("_dup_rank", 2, "DQ14_DUPLICATE_ENCOUNTER"),
])
def test_each_rule_catches_its_own_defect(spark, field, value, code):
    assert code in _codes(spark, _row(**{field: value}))


def test_report_before_onset_is_caught(spark):
    codes = _codes(spark, _row(onset_date=datetime.date(2023, 7, 24),
                               report_date=datetime.date(2023, 7, 20)))
    assert "DQ09_DATE_ORDER" in codes


def test_reporting_delay_beyond_21_days_is_caught(spark):
    codes = _codes(spark, _row(onset_date=datetime.date(2023, 7, 1),
                               report_date=datetime.date(2023, 8, 1)))
    assert "DQ10_REPORTING_DELAY" in codes


def test_null_is_a_failure_not_a_silent_pass(spark):
    """
    Spark evaluates NULL > 5 as NULL rather than false, so a naive filter
    lets null-bearing rows through the very check meant to catch them. The
    rules use eqNullSafe for this reason, and this test is what keeps that
    from regressing.
    """
    assert "DQ03_AGE_RANGE" in _codes(spark, _row(age_years=None))
    assert "DQ04_SEX_DOMAIN" in _codes(spark, _row(sex=None))
    assert "DQ07_ONSET_NOT_NULL" in _codes(spark, _row(onset_date=None))


def test_rule_count_meets_requirement():
    """DQ-1 requires at least 12 automated rules."""
    rules = encounter_rules(DISTRICTS, "2021-01-01", "2025-12-31")
    assert len(rules) >= 12


def test_a_row_can_fail_several_rules_at_once(spark):
    codes = _codes(spark, _row(age_years=212, sex="?", _facility_exists=False))
    assert {"DQ03_AGE_RANGE", "DQ04_SEX_DOMAIN",
            "DQ06_FACILITY_FK"} <= codes
