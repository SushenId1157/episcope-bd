"""
EpiScope schema contract.

The instructor's hidden 14-day incremental drop arrives in the same schema as
S4-S6 (Section 5.2). Every reader and writer in the pipeline imports its schema
from this module, so the contract lives in exactly one place and the ingestion
code contains no hard-coded column lists, dates, file names or row counts.
"""
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DateType, BooleanType,
                               DoubleType, TimestampType, LongType)

# ------------------------------------------------------- S4 encounter line list
# patient_id is a direct identifier: it exists in bronze only and is replaced by
# a salted hash before the silver layer (PRV-1).
ENCOUNTER_SCHEMA = StructType([
    StructField("encounter_id",   StringType(),  False),
    StructField("patient_id",     StringType(),  True),
    StructField("facility_id",    StringType(),  True),
    StructField("district_id",    StringType(),  True),
    StructField("onset_date",     DateType(),    True),
    StructField("report_date",    DateType(),    True),
    StructField("age_years",      IntegerType(), True),
    StructField("sex",            StringType(),  True),   # M / F / O
    StructField("icd10_code",     StringType(),  True),
    StructField("disease_group",  StringType(),  True),
    StructField("lab_confirmed",  BooleanType(), True),
    StructField("admitted",       BooleanType(), True),
    StructField("outcome",        StringType(),  True),   # recovered/died/unknown
])

# ------------------------------------------------------------ S5 facility registry
FACILITY_SCHEMA = StructType([
    StructField("facility_id",     StringType(),  False),
    StructField("facility_name",   StringType(),  True),
    StructField("facility_type",   StringType(),  True),  # district_hospital /
                                                          # upazila_hc / clinic
    StructField("district_id",     StringType(),  True),
    StructField("district_name",   StringType(),  True),
    StructField("division",        StringType(),  True),
    StructField("latitude",        DoubleType(),  True),
    StructField("longitude",       DoubleType(),  True),
    StructField("total_beds",      IntegerType(), True),
    StructField("icu_beds",        IntegerType(), True),
    StructField("opened_year",     IntegerType(), True),
])

# ------------------------------------------- S6 daily stock and bed occupancy
STOCK_SCHEMA = StructType([
    StructField("snapshot_date",      DateType(),    False),
    StructField("facility_id",        StringType(),  False),
    StructField("iv_saline_units",    IntegerType(), True),
    StructField("ns1_kits",           IntegerType(), True),
    StructField("ors_sachets",        IntegerType(), True),
    StructField("antimalarial_doses", IntegerType(), True),
    StructField("occupied_beds",      IntegerType(), True),
    StructField("occupied_icu_beds",  IntegerType(), True),
])

# ------------------------------------------- ground truth (never read by AN-1)
# Section 5.2: "Record them in a separate ground_truth_outbreaks file that the
# detection code must never read." It is used only by the evaluation step AN-2.
GROUND_TRUTH_SCHEMA = StructType([
    StructField("outbreak_id",      StringType(),  False),
    StructField("district_id",      StringType(),  True),
    StructField("district_name",    StringType(),  True),
    StructField("disease_group",    StringType(),  True),
    StructField("start_date",       DateType(),    True),
    StructField("end_date",         DateType(),    True),
    StructField("peak_date",        DateType(),    True),
    StructField("peak_multiplier",  DoubleType(),  True),
    StructField("outbreak_type",    StringType(),  True),  # point/slow/national
    StructField("is_national",      BooleanType(), True),
    StructField("excess_cases",     LongType(),    True),
])

# --------------------------------------------- bronze ingestion metadata (ING-1)
# Added to every bronze table, never removed downstream: this is what makes
# gold-to-bronze lineage possible (DQ-3).
BRONZE_METADATA_FIELDS = [
    StructField("_source_id",       StringType(),    False),
    StructField("_batch_id",        StringType(),    False),
    StructField("_source_file",     StringType(),    True),
    StructField("_file_checksum",   StringType(),    True),
    StructField("_load_timestamp",  TimestampType(), False),
    StructField("_ingest_mode",     StringType(),    True),  # full / incremental
]


def with_bronze_metadata(schema: StructType) -> StructType:
    """Return `schema` extended with the ING-1 ingestion metadata columns."""
    return StructType(list(schema.fields) + list(BRONZE_METADATA_FIELDS))


# ---------------------------------------------------------- controlled values
VALID_SEX = ["M", "F", "O"]
VALID_OUTCOME = ["recovered", "died", "unknown"]
VALID_FACILITY_TYPES = ["district_hospital", "upazila_hc", "clinic",
                        "medical_college", "private_clinic"]

# Allowed ICD-10 prefixes. Anything outside this set fails quality rule DQ-ICD10.
VALID_ICD10_PREFIXES = ["A00", "A01", "A02", "A09", "A90", "A91",
                        "B34", "B50", "B51", "B52", "B53", "B54",
                        "J06", "J10", "J11", "R50", "U07"]

AGE_MIN, AGE_MAX = 0, 120          # outside this range -> quarantine (DQ-AGE)
