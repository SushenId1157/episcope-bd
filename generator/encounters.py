"""
EpiScope - explode weekly case counts into individual encounter records (S4).

Design constraint: the measured environment has 2 CPU cores, so every per-row
operation must run inside the JVM. There are no Python UDFs here. Each
encounter's attributes are derived from a hash of its own encounter_id, which
makes the generator fully deterministic for a given seed while remaining
embarrassingly parallel: no RNG state is carried between rows or partitions.

Deliberate defects required by Section 5.2 (duplicates, orphan facility keys,
invalid demographics, reporting delay, weekend effects) are injected here, in
the raw feed, so that the quality rules in DQ-1 have genuine faults to catch.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


def _unit(col, salt: str, seed: int):
    """
    A reproducible pseudo-random number in [0,1) derived from `col`.

    xxhash64 is a Spark built-in, so this evaluates in the JVM. Mixing in a
    per-field salt means age, sex, delay and so on are independent draws for
    the same encounter rather than all keyed to one value.
    """
    h = F.xxhash64(F.concat_ws(":", col, F.lit(salt), F.lit(str(seed))))
    return (F.abs(h) % F.lit(1_000_000)) / F.lit(1_000_000.0)


def build_encounters(spark: SparkSession, weekly_df: DataFrame,
                     facilities_df: DataFrame, districts_df: DataFrame,
                     diseases: dict, defect_rates: dict, reporting: dict,
                     seed: int) -> DataFrame:
    """
    Expand one row per (district, disease, epi-week, case) into encounters.

    weekly_df must carry: district_id, disease_group, epi_year, epi_week,
    cases, outbreak_multiplier.
    """
    # ---------------------------------------------------------- 1. explode
    # sequence() builds an array of length `cases`; explode turns each element
    # into a row. Rows with zero cases drop out naturally.
    enc = (weekly_df
           .filter(F.col("cases") > 0)
           .withColumn("case_seq",
                       F.explode(F.sequence(F.lit(1), F.col("cases"))))
           .drop("cases"))

    # Deterministic, globally unique encounter id built from its own key.
    enc = enc.withColumn(
        "encounter_id",
        F.concat_ws("-", F.lit("E"), F.col("district_id"),
                    F.col("epi_year"), F.lpad(F.col("epi_week"), 2, "0"),
                    F.col("disease_group"), F.lpad(F.col("case_seq"), 6, "0")))

    # ------------------------------------------------- 2. dates and delays
    # Monday of the ISO week, then a day offset within the week. Fewer onsets
    # are recorded at the weekend (Friday and Saturday in Bangladesh).
    # Monday of the ISO week, computed arithmetically rather than parsed.
    # Mixing a calendar year with an ISO week number in one format string is
    # ambiguous and Spark rejects it. By definition, 4 January always falls in
    # ISO week 1 of its year, so the Monday of week 1 is found from that date
    # and every later week is a multiple of seven days from it. This is exact
    # across year boundaries, where late December can belong to week 1 of the
    # following epi-year.
    jan4 = F.to_date(F.concat_ws("-", F.col("epi_year"), F.lit("01"),
                                 F.lit("04")), "yyyy-MM-dd")
    # Spark's dayofweek is 1=Sunday..7=Saturday; convert to ISO 1=Monday..7=Sunday
    iso_dow = ((F.dayofweek(jan4) + F.lit(5)) % F.lit(7)) + F.lit(1)
    monday_week1 = F.date_sub(jan4, (iso_dow - F.lit(1)).cast("int"))
    # date_add requires an INT offset; epi_week arrives as BIGINT because
    # Spark infers LongType for Python ints, so the offset is cast explicitly.
    enc = enc.withColumn(
        "week_monday",
        F.date_add(monday_week1,
                   ((F.col("epi_week") - F.lit(1)) * F.lit(7)).cast("int")))

    day_u = _unit(F.col("encounter_id"), "dayofweek", seed)
    enc = enc.withColumn("day_offset", (day_u * F.lit(7)).cast("int"))
    enc = enc.withColumn("onset_date",
                         F.date_add(F.col("week_monday"), F.col("day_offset")))

    # Reporting delay: right-skewed over 0-21 days. Two uniforms multiplied
    # approximate a gamma-like shape cheaply and entirely in the JVM.
    d1 = _unit(F.col("encounter_id"), "delay1", seed)
    d2 = _unit(F.col("encounter_id"), "delay2", seed)
    max_delay = float(reporting["max_delay_days"])
    enc = enc.withColumn(
        "raw_delay",
        F.pow(d1 * d2, F.lit(0.65)) * F.lit(max_delay))

    # Reports due on a Friday or Saturday slip to the next working day.
    enc = enc.withColumn("delay_days",
                         F.least(F.col("raw_delay").cast("int"),
                                 F.lit(int(max_delay))))
    enc = enc.withColumn("report_date",
                         F.date_add(F.col("onset_date"), F.col("delay_days")))
    enc = enc.withColumn(
        "report_dow", F.dayofweek(F.col("report_date")))       # 1=Sun .. 7=Sat
    enc = enc.withColumn(
        "report_date",
        F.when(F.col("report_dow") == 6, F.date_add(F.col("report_date"), 2))
         .when(F.col("report_dow") == 7, F.date_add(F.col("report_date"), 1))
         .otherwise(F.col("report_date")))
    enc = enc.drop("raw_delay", "report_dow", "week_monday", "day_offset")

    # ------------------------------------------------- 3. demographics
    # Age bands differ by disease, following the age_profile in the config.
    age_u = _unit(F.col("encounter_id"), "age", seed)
    age_expr = F.lit(30)
    for name, dcfg in diseases.items():
        profile = dcfg.get("age_profile", "all_ages")
        if profile == "young_adult":
            band = (F.lit(14) + age_u * F.lit(36)).cast("int")
        elif profile == "children_and_elderly":
            band = F.when(age_u < 0.45, (age_u * F.lit(11)).cast("int")) \
                    .when(age_u > 0.80,
                          (F.lit(60) + (age_u - 0.80) * F.lit(140)).cast("int")) \
                    .otherwise((F.lit(12) + age_u * F.lit(45)).cast("int"))
        else:
            band = (age_u * F.lit(80)).cast("int")
        age_expr = F.when(F.col("disease_group") == name, band).otherwise(age_expr)
    enc = enc.withColumn("age_years", age_expr)

    sex_u = _unit(F.col("encounter_id"), "sex", seed)
    enc = enc.withColumn(
        "sex", F.when(sex_u < 0.49, F.lit("M"))
                .when(sex_u < 0.985, F.lit("F"))
                .otherwise(F.lit("O")))

    # ------------------------------------------------- 4. clinical fields
    icd_u = _unit(F.col("encounter_id"), "icd", seed)
    icd_expr = F.lit("R50")
    for name, dcfg in diseases.items():
        codes, weights = dcfg["icd10_codes"], dcfg["icd10_weights"]
        cum, branch = 0.0, None
        for code, w in zip(codes, weights):
            cum += w
            cond = icd_u < F.lit(cum)
            branch = F.when(cond, F.lit(code)) if branch is None \
                else branch.when(cond, F.lit(code))
        branch = branch.otherwise(F.lit(codes[-1]))
        icd_expr = F.when(F.col("disease_group") == name, branch) \
                    .otherwise(icd_expr)
    enc = enc.withColumn("icd10_code", icd_expr)

    # Severity cascade: admission probability rises during an outbreak, as the
    # brief requires (dengue admission rate rises during outbreaks).
    adm_u = _unit(F.col("encounter_id"), "admit", seed)
    adm_expr = F.lit(0.05)
    for name, dcfg in diseases.items():
        normal = float(dcfg["admission_rate"])
        during = float(dcfg["admission_rate_outbreak"])
        rate = F.when(F.col("outbreak_multiplier") > F.lit(1.3), F.lit(during)) \
                .otherwise(F.lit(normal))
        adm_expr = F.when(F.col("disease_group") == name, rate) \
                    .otherwise(adm_expr)
    enc = enc.withColumn("admitted", adm_u < adm_expr)

    lab_u = _unit(F.col("encounter_id"), "lab", seed)
    lab_expr = F.lit(0.1)
    for name, dcfg in diseases.items():
        lab_expr = F.when(F.col("disease_group") == name,
                          F.lit(float(dcfg["lab_confirm_rate"]))) \
                    .otherwise(lab_expr)
    enc = enc.withColumn("lab_confirmed", lab_u < lab_expr)

    out_u = _unit(F.col("encounter_id"), "outcome", seed)
    cfr_expr = F.lit(0.001)
    for name, dcfg in diseases.items():
        cfr_expr = F.when(F.col("disease_group") == name,
                          F.lit(float(dcfg["cfr"]))) .otherwise(cfr_expr)
    enc = enc.withColumn(
        "outcome",
        F.when(out_u < cfr_expr, F.lit("died"))
         .when(out_u < F.lit(0.93), F.lit("recovered"))
         .otherwise(F.lit("unknown")))

    # ------------------------------------------------- 5. patient identity
    # A synthetic national ID. A small share of patients present more than
    # once, which is what DQ-4 cross-facility de-duplication has to find.
    pid_u = _unit(F.col("encounter_id"), "patient", seed)
    enc = enc.withColumn(
        "patient_id",
        F.concat(F.lit("BD"),
                 F.lpad(((F.abs(F.xxhash64(F.concat_ws(":", F.col("encounter_id"),
                                                       F.lit("pid"),
                                                       F.lit(str(seed)))))
                          % F.lit(90_000_000)) + F.lit(10_000_000))
                        .cast("string"), 8, "0")))

    # ------------------------------------------------- 6. facility assignment
    # Each district's facilities are numbered, then an encounter picks one.
    fac = (facilities_df
           .select("facility_id", "district_id")
           .withColumn("fac_rank",
                       F.row_number().over(Window.partitionBy("district_id")
                                           .orderBy("facility_id")) - 1))
    fac_count = fac.groupBy("district_id").agg(
        F.count("*").alias("n_fac"))

    enc = enc.join(F.broadcast(fac_count), "district_id", "left")
    fac_u = _unit(F.col("encounter_id"), "facility", seed)
    enc = enc.withColumn("fac_rank",
                         (fac_u * F.col("n_fac")).cast("int"))
    enc = enc.join(F.broadcast(fac), ["district_id", "fac_rank"], "left") \
             .drop("fac_rank", "n_fac")

    # ------------------------------------------------- 7. deliberate defects
    # 7a. Orphan facility keys: a reference to a facility that is not in S5.
    # Orphan facility keys point at facilities absent from the S5 registry.
    # Real referential breaks come from a handful of recurring bad codes - a
    # decommissioned site, a typo in a reporting template - rather than from a
    # thousand distinct values, so the orphan ids are drawn from a small fixed
    # pool. This makes the quarantine report in DQ-2 actionable: an officer can
    # see which specific codes are failing and why.
    orphan_u = _unit(F.col("encounter_id"), "orphan", seed)
    enc = enc.withColumn(
        "facility_id",
        F.when(orphan_u < F.lit(float(defect_rates["orphan_facility_keys"])),
               F.concat(F.lit("F9"),
                        F.lpad((F.abs(F.xxhash64(F.col("encounter_id")))
                                % F.lit(12)).cast("string"), 3, "0")))
         .otherwise(F.col("facility_id")))

    # 7b. Invalid demographics: impossible ages and unknown sex codes.
    inv_u = _unit(F.col("encounter_id"), "invalid", seed)
    inv_rate = float(defect_rates["invalid_demographics"])
    enc = enc.withColumn(
        "age_years",
        F.when(inv_u < F.lit(inv_rate * 0.6),
               F.lit(150) + (inv_u * F.lit(10_000)).cast("int") % F.lit(120))
         .otherwise(F.col("age_years")))
    enc = enc.withColumn(
        "sex",
        F.when((inv_u >= F.lit(inv_rate * 0.6)) & (inv_u < F.lit(inv_rate)),
               F.when(inv_u * F.lit(1000) % F.lit(2) < F.lit(1), F.lit("?"))
                .otherwise(F.lit("U")))
         .otherwise(F.col("sex")))

    enc = enc.select(
        "encounter_id", "patient_id", "facility_id", "district_id",
        "onset_date", "report_date", "age_years", "sex", "icd10_code",
        "disease_group", "lab_confirmed", "admitted", "outcome",
        "epi_year", "epi_week", "outbreak_multiplier", "delay_days")

    # 7c. Duplicates: about 2% of encounters are resubmitted, keeping the same
    # encounter_id but arriving later. Idempotent ingestion (ING-4) and the
    # duplicate rule in DQ-1 both have to handle these.
    dup_u = _unit(F.col("encounter_id"), "dup", seed)
    dups = (enc.filter(dup_u < F.lit(float(defect_rates["duplicate_encounters"])))
               .withColumn("report_date",
                           F.date_add(F.col("report_date"),
                                      F.lit(1) + (_unit(F.col("encounter_id"),
                                                        "dupdelay", seed)
                                                  * F.lit(9)).cast("int"))))

    return enc.unionByName(dups)
