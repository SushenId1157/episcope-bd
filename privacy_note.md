# EpiScope Privacy Note

## 1. Introduction

EpiScope uses privacy protection techniques to ensure that sensitive patient information is not exposed in published dashboard outputs. The project focuses on two main privacy protections: patient de-identification (PRV-1) and small-cell suppression (PRV-2).

## 2. Patient De-identification (PRV-1)

Patient de-identification is performed in `pipeline/silver/build.py`.

- The original `patient_id` is combined with a secret salt.
- A SHA-256 hash is generated from the salted identifier and stored as `patient_hash`.
- The original `patient_id` column is removed after hashing.
- The salt is stored as an environment variable and should not be committed to GitHub.
- Age values are grouped into broader age bands to reduce the exposure of personal information.

This process replaces direct patient identifiers with pseudonymous identifiers before further processing.

## 3. Data Quality and Privacy Checks

The file `pipeline/quality/rules.py` contains several data-quality rules that help maintain the reliability of the processed data.

One important rule is `DQ02_PATIENT_HASH`, which checks that the patient hash exists and contains exactly 64 characters, as expected from a SHA-256 hash.

Other rules validate important fields such as age, sex, district, facility, dates, disease codes, and duplicate encounters.

Records that fail the quality checks are separated into quarantine with their corresponding rule codes.

## 4. Small-Cell Suppression (PRV-2)

The file `serving/export.py` implements small-cell suppression to protect patient confidentiality in published dashboard outputs.

- Patient counts between 1 and 4 are hidden.
- A count of zero remains visible because it does not indicate the presence of a patient.
- Suppressed values are represented as `null` in the exported JSON.
- The dashboard displays hidden values as "withheld" instead of showing zero.
- Suppression is applied to relevant patient-count fields, such as cases, admissions, deaths, and distinct patients.

This prevents viewers from identifying individuals through very small patient counts.

## 5. Complementary Suppression

EpiScope also uses complementary suppression to provide additional privacy protection.

Sometimes, hiding only one small value is not enough. If the total and all other values are visible, someone could calculate the hidden value using subtraction.

To prevent this:

- Another value may also be hidden when only one value in a group is suppressed.
- The smallest remaining positive count can be selected for additional suppression.
- If no suitable value is available, the total may also be withheld.

This makes it more difficult to reconstruct hidden patient counts from published information.

## 6. Privacy Auditing

The export process includes privacy auditing to check whether sensitive information is accidentally exposed.

The auditing functions include:

- `audit_no_small_counts`: Checks whether small patient counts appear in exported data.
- `audit_derivable_counts`: Checks whether suppressed counts can be calculated from other published values.
- Export validation: Checks the generated dashboard files before publication.

The project also includes privacy tests in `tests/test_privacy.py` to verify the de-identification, suppression, complementary suppression, and auditing mechanisms.

## 7. Conclusion

EpiScope combines patient de-identification, small-cell suppression, and complementary suppression to protect sensitive health information. These techniques help ensure that the dashboard provides useful public health insights without unnecessarily exposing individual patient information.

## Files Reviewed

- `pipeline/silver/build.py` — Patient de-identification and age grouping.
- `pipeline/quality/rules.py` — Data-quality validation rules.
- `serving/export.py` — Small-cell suppression, complementary suppression, and privacy auditing.
- `orchestration/run_serving.py` — Serving and export validation.
- `tests/test_privacy.py` — Privacy-related tests.

**Note:** The supplied repository does not contain a file named `text.privacy.py`. The patient de-identification implementation is located in `pipeline/silver/build.py`.
