"""
Generator tests: determinism and the Section 5.2 specification.

These need no lake and no Spark, so they run on a clean clone and are the
fastest signal that the generator still behaves as specified.
"""
import pytest

import generator.config as cfg
import generator.facilities as gfac
import generator.outbreaks as gob


def test_same_seed_gives_identical_facilities(districts):
    """Section 5.2 requires the generator to be deterministic for a seed."""
    a = gfac.build_registry(districts, seed=cfg.DEFAULT_SEED)
    b = gfac.build_registry(districts, seed=cfg.DEFAULT_SEED)
    assert a == b


def test_different_seed_changes_output(districts):
    """A seed that changes nothing would make determinism meaningless."""
    a = gfac.build_registry(districts, seed=cfg.DEFAULT_SEED)
    c = gfac.build_registry(districts, seed=cfg.DEFAULT_SEED + 1)
    assert a != c


def test_facility_count_meets_requirement(districts):
    """S5 requires at least 600 facilities."""
    reg = gfac.build_registry(districts, seed=cfg.DEFAULT_SEED)
    assert len(reg) >= 600


def test_every_district_has_facilities(districts):
    reg = gfac.build_registry(districts, seed=cfg.DEFAULT_SEED)
    covered = {f["district_id"] for f in reg}
    assert len(covered) == len(districts)


def test_icu_beds_never_exceed_total_beds(districts):
    reg = gfac.build_registry(districts, seed=cfg.DEFAULT_SEED)
    assert all(f["icu_beds"] <= f["total_beds"] for f in reg)


def test_outbreak_plan_is_deterministic(districts):
    args = (districts, cfg.OUTBREAK_SPEC, cfg.DISEASES,
            cfg.SURVEILLANCE_DISEASES, cfg.DEFAULT_SEED,
            cfg.STUDY_START, cfg.STUDY_END)
    p1, t1 = gob.build_outbreak_plan(*args)
    p2, t2 = gob.build_outbreak_plan(*args)
    assert p1 == p2 and t1 == t2


def test_outbreak_count_meets_requirement(districts):
    """Section 5.2: at least 30 outbreaks, one slow-growing, one national."""
    _, truth = gob.build_outbreak_plan(
        districts, cfg.OUTBREAK_SPEC, cfg.DISEASES,
        cfg.SURVEILLANCE_DISEASES, cfg.DEFAULT_SEED,
        cfg.STUDY_START, cfg.STUDY_END)
    district_level = [t for t in truth if not t["is_national"]]
    types = {t["outbreak_type"] for t in truth}
    assert len(district_level) >= 30
    assert "slow_growing" in types
    assert "national_novel_respiratory" in types


def test_malaria_outbreaks_stay_in_hill_districts(districts):
    """
    Geography is a hard constraint, not a preference. A malaria epidemic in
    the northern plains would be epidemiologically false and would discredit
    the dataset. An earlier version let the spreading rule override it.
    """
    hills = {d["district_id"] for d in districts
             if str(d.get("is_hill")).lower() == "true"}
    _, truth = gob.build_outbreak_plan(
        districts, cfg.OUTBREAK_SPEC, cfg.DISEASES,
        cfg.SURVEILLANCE_DISEASES, cfg.DEFAULT_SEED,
        cfg.STUDY_START, cfg.STUDY_END)
    malaria = [t for t in truth if t["disease_group"] == "malaria"]
    assert malaria, "no malaria outbreaks were generated"
    assert all(t["district_id"] in hills for t in malaria)


def test_dengue_outbreaks_peak_in_season(districts):
    """The brief states dengue follows the monsoon."""
    _, truth = gob.build_outbreak_plan(
        districts, cfg.OUTBREAK_SPEC, cfg.DISEASES,
        cfg.SURVEILLANCE_DISEASES, cfg.DEFAULT_SEED,
        cfg.STUDY_START, cfg.STUDY_END)
    dengue = [t for t in truth if t["disease_group"] == "dengue"]
    assert dengue
    for t in dengue:
        wk = t["peak_date"].isocalendar()[1]
        assert 26 <= wk <= 45, f"{t['outbreak_id']} peaks in week {wk}"


def test_defect_rates_match_the_specification():
    """Section 5.2 names 2% duplicates, 0.5% orphans, 1% invalid."""
    d = cfg.DEFECT_RATES
    assert abs(d["duplicate_encounters"] - 0.02) < 1e-9
    assert abs(d["orphan_facility_keys"] - 0.005) < 1e-9
    assert abs(d["invalid_demographics"] - 0.01) < 1e-9
