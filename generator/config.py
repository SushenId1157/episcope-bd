"""
EpiScope synthetic generator - epidemiological configuration (Section 5.2).

Every constant here is a modelling assumption of this teaching project. None of
it is an official statistic. Seasonal shapes and climate lags follow the
qualitative patterns described in the assignment brief and in the surveillance
literature cited in the report; the numbers are chosen to produce realistic
synthetic data, not to reproduce any real epidemic.
"""
from __future__ import annotations

# ---------------------------------------------------------------- study window
STUDY_START = "2021-01-01"
STUDY_END   = "2025-12-31"
DEFAULT_SEED = 20260101

# ------------------------------------------------------------------- diseases
# icd10_codes: drawn at random per encounter, weighted by icd10_weights.
# peak_week  : epidemiological week of the annual seasonal maximum.
# season_width: how broad the annual peak is, in weeks (Gaussian sigma).
# baseline_rate: expected weekly cases per 100,000 population at the seasonal
#                trough, before climate, outbreak and overdispersion effects.
# nb_dispersion: negative-binomial size parameter k. Smaller k = more
#                overdispersion. Variance = mu + mu^2 / k.
DISEASES = {
    "dengue": {
        "display_name": "Dengue",
        "icd10_codes": ["A90", "A91"],
        "icd10_weights": [0.82, 0.18],          # A91 = severe/haemorrhagic
        "peak_week": 36,                         # early September
        "season_width": 7.0,
        "baseline_rate": 3.266,
        "nb_dispersion": 10.0,
        "urban_amplification": 2.6,              # concentrated in large cities
        "hill_amplification": 0.5,
        "climate": {
            "rain_lag_weeks": 6,                 # brief: roughly 4-8 weeks
            "rain_coefficient": 0.55,
            "temp_lag_weeks": 4,
            "temp_coefficient": 0.35,
            "temp_optimum_c": 29.0,
            "humidity_coefficient": 0.20,
        },
        "admission_rate": 0.17,
        "admission_rate_outbreak": 0.28,         # severity rises in outbreaks
        "lab_confirm_rate": 0.62,
        "cfr": 0.004,
        "age_profile": "young_adult",
    },
    "diarrhoeal": {
        "display_name": "Diarrhoeal disease / cholera",
        "icd10_codes": ["A00", "A09"],
        "icd10_weights": [0.08, 0.92],
        "peak_week": 16,                         # pre-monsoon, April
        "season_width": 9.0,
        "baseline_rate": 12.338,
        "nb_dispersion": 13.3,
        "urban_amplification": 1.3,
        "hill_amplification": 1.1,
        "climate": {
            "rain_lag_weeks": 1,                 # spikes soon after heavy rain
            "rain_coefficient": 0.70,
            "temp_lag_weeks": 2,
            "temp_coefficient": 0.25,
            "temp_optimum_c": 31.0,
            "humidity_coefficient": 0.10,
        },
        "admission_rate": 0.11,
        "admission_rate_outbreak": 0.19,
        "lab_confirm_rate": 0.22,
        "cfr": 0.002,
        "age_profile": "children_and_elderly",
    },
    "ili": {
        "display_name": "Influenza-like illness",
        "icd10_codes": ["J10", "J11"],
        "icd10_weights": [0.35, 0.65],
        "peak_week": 28,                         # rainy-season peak, July
        "season_width": 8.0,
        "baseline_rate": 18.507,
        "nb_dispersion": 18.0,
        "urban_amplification": 1.5,
        "hill_amplification": 0.9,
        "climate": {
            "rain_lag_weeks": 2,
            "rain_coefficient": 0.30,
            "temp_lag_weeks": 2,
            "temp_coefficient": -0.30,           # cooler weeks favour ILI
            "temp_optimum_c": 22.0,
            "humidity_coefficient": 0.15,
        },
        "secondary_peak_week": 2,                # smaller winter wave
        "secondary_peak_strength": 0.45,
        "admission_rate": 0.05,
        "admission_rate_outbreak": 0.10,
        "lab_confirm_rate": 0.18,
        "cfr": 0.001,
        "age_profile": "all_ages",
    },
    "malaria": {
        "display_name": "Malaria",
        "icd10_codes": ["B50", "B51", "B52", "B53", "B54"],
        "icd10_weights": [0.58, 0.12, 0.06, 0.04, 0.20],
        "peak_week": 26,                         # late June
        "season_width": 10.0,
        "baseline_rate": 0.907,
        "nb_dispersion": 9.5,
        "urban_amplification": 0.15,             # rare in cities
        "hill_amplification": 9.0,               # concentrated in hill tracts
        "climate": {
            "rain_lag_weeks": 5,
            "rain_coefficient": 0.45,
            "temp_lag_weeks": 3,
            "temp_coefficient": 0.30,
            "temp_optimum_c": 28.0,
            "humidity_coefficient": 0.25,
        },
        "admission_rate": 0.14,
        "admission_rate_outbreak": 0.22,
        "lab_confirm_rate": 0.88,                # RDT widely used
        "cfr": 0.003,
        "age_profile": "all_ages",
    },
    "other_febrile": {
        "display_name": "Other febrile illness (background)",
        "icd10_codes": ["R50", "A01", "A02", "B34", "J06"],
        "icd10_weights": [0.40, 0.08, 0.12, 0.20, 0.20],
        "peak_week": 20,
        "season_width": 16.0,
        "baseline_rate": 28.306,
        "nb_dispersion": 26.0,
        "urban_amplification": 1.2,
        "hill_amplification": 1.0,
        "climate": {
            "rain_lag_weeks": 3, "rain_coefficient": 0.12,
            "temp_lag_weeks": 2, "temp_coefficient": 0.08,
            "temp_optimum_c": 28.0, "humidity_coefficient": 0.05,
        },
        "admission_rate": 0.04,
        "admission_rate_outbreak": 0.06,
        "lab_confirm_rate": 0.09,
        "cfr": 0.0008,
        "age_profile": "all_ages",
    },
}

# Diseases the surveillance system monitors explicitly (AN-1, AN-4, VIS-2).
SURVEILLANCE_DISEASES = ["dengue", "diarrhoeal", "ili", "malaria"]
FORECAST_DISEASES = ["dengue", "diarrhoeal"]     # AN-4 requires at least two

# --------------------------------------------------- administrative geography
# The 8 divisions of Bangladesh and the districts belonging to each.
# This is real administrative structure, used to attach a division to each
# district returned by geoBoundaries (S1) and to partition gold tables (ING-3).
DIVISIONS = {
    "Barishal": ["Barguna", "Barishal", "Bhola", "Jhalokati", "Patuakhali",
                 "Pirojpur"],
    "Chattogram": ["Bandarban", "Brahmanbaria", "Chandpur", "Chattogram",
                   "Cumilla", "Cox's Bazar", "Feni", "Khagrachhari",
                   "Lakshmipur", "Noakhali", "Rangamati"],
    "Dhaka": ["Dhaka", "Faridpur", "Gazipur", "Gopalganj", "Kishoreganj",
              "Madaripur", "Manikganj", "Munshiganj", "Narayanganj",
              "Narsingdi", "Rajbari", "Shariatpur", "Tangail"],
    "Khulna": ["Bagerhat", "Chuadanga", "Jashore", "Jhenaidah", "Khulna",
               "Kushtia", "Magura", "Meherpur", "Narail", "Satkhira"],
    "Mymensingh": ["Jamalpur", "Mymensingh", "Netrokona", "Sherpur"],
    "Rajshahi": ["Bogura", "Chapai Nawabganj", "Joypurhat", "Naogaon",
                 "Natore", "Pabna", "Rajshahi", "Sirajganj"],
    "Rangpur": ["Dinajpur", "Gaibandha", "Kurigram", "Lalmonirhat",
                "Nilphamari", "Panchagarh", "Rangpur", "Thakurgaon"],
    "Sylhet": ["Habiganj", "Moulvibazar", "Sunamganj", "Sylhet"],
}

# Districts treated as highly urban: dengue amplification applies here (the
# brief states dengue is concentrated in large cities).
URBAN_DISTRICTS = ["Dhaka", "Chattogram", "Gazipur", "Narayanganj", "Khulna",
                   "Rajshahi", "Sylhet", "Barishal", "Cumilla", "Mymensingh",
                   "Bogura", "Rangpur"]

# Chittagong Hill Tracts and adjacent forested districts: malaria focus.
HILL_DISTRICTS = ["Bandarban", "Rangamati", "Khagrachhari", "Cox's Bazar",
                  "Sylhet", "Moulvibazar", "Sunamganj", "Habiganj"]

# --------------------------------------------------------- district name aliases
# geoBoundaries (S1) carries the pre-2018 English spellings of several district
# names, while DIVISIONS above uses the current official romanisations. Mapping
# them here keeps one canonical name across the lake and documents the conflict
# instead of silently renaming the source. Keys are matched after
# normalisation (lower-case, alphanumeric only).
DISTRICT_NAME_ALIASES = {
    "barisal":        "Barishal",
    "bogra":          "Bogura",
    "brahamanbaria":  "Brahmanbaria",
    "chittagong":     "Chattogram",
    "comilla":        "Cumilla",
    "jessore":        "Jashore",
    "maulvibazar":    "Moulvibazar",
    "nawabganj":      "Chapai Nawabganj",
    "netrakona":      "Netrokona",
    # further historical spellings that may appear in other boundary releases
    "coxsbazar":      "Cox's Bazar",
    "chapainawabganj": "Chapai Nawabganj",
    "khagrachari":    "Khagrachhari",
    "jhalakati":      "Jhalokati",
    "munshigonj":     "Munshiganj",
}

# ------------------------------------------------------- deliberate defects
# Section 5.2 requires these to be present so the quality rules (DQ-1) have
# something real to catch.
DEFECT_RATES = {
    "duplicate_encounters": 0.020,   # ~2% resubmitted records
    "orphan_facility_keys": 0.005,   # ~0.5% facility_id not in the registry
    "invalid_demographics": 0.010,   # ~1% impossible age or unknown sex
}

REPORTING = {
    "max_delay_days": 21,            # right-skewed 0-21 day reporting delay
    "delay_shape": 1.6,              # gamma shape - most reports arrive early
    "delay_scale": 2.4,
    "weekend_report_factor": 0.45,   # fewer reports on Fri/Sat (BD weekend)
    "holiday_report_factor": 0.30,
}

# ------------------------------------------------------------------ outbreaks
# Section 5.2 requires at least 30 injected outbreaks of varying size, duration
# and location, including one slow-growing outbreak and a nationwide novel
# respiratory surge. The exact list is built deterministically from the seed in
# generator/outbreaks.py; these are the shape parameters it draws from.
OUTBREAK_SPEC = {
    "n_district_outbreaks": 32,      # exceeds the required minimum of 30
    "duration_weeks": (4, 14),
    "peak_multiplier": (2.5, 9.0),   # peak cases relative to expected baseline
    "slow_growing": {
        "count": 2,
        "duration_weeks": (18, 26),
        "peak_multiplier": (1.6, 2.2),   # deliberately hard to detect early
    },
    "novel_respiratory": {
        "start_week_index": 148,     # within 2023, mid-study
        "duration_weeks": 16,
        "peak_multiplier": 7.5,
        "national": True,
        "disease": "ili",
        "icd10_code": "U07.1",       # novel respiratory pathogen
    },
}

# At least one reporting artefact: a facility that goes silent, then back-fills.
# A naive detector will flag the back-fill week as an outbreak (AN-3).
REPORTING_ARTEFACTS = [
    {"artefact_id": "ART-01", "type": "silent_then_backfill",
     "silent_weeks": 3, "start_week_index": 95,
     "note": "Large facility stops reporting, then submits all held records "
             "in a single week."},
    {"artefact_id": "ART-02", "type": "silent_then_backfill",
     "silent_weeks": 2, "start_week_index": 187,
     "note": "Second, smaller back-fill event in a different division."},
]

# ------------------------------------------------------------------ scaling
# At SF=1 the generator must produce at least 25 million encounters (Section 5.2).
# TARGET_ENCOUNTERS_SF1 is the design target; the achieved count is measured,
# never assumed.
TARGET_ENCOUNTERS_SF1 = 25_000_000
VALID_SCALE_FACTORS = [0.01, 0.05, 0.25, 0.5, 1.0]
