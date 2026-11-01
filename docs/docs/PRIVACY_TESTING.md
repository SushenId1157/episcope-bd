# EpiScope Privacy Testing Documentation

## 1. Introduction

EpiScope includes privacy tests to verify that sensitive patient information is protected during data processing and publication.

These tests help ensure that patient identifiers are removed, small patient counts are suppressed, and hidden values cannot easily be reconstructed from published data.

The privacy tests are located in:

`tests/test_privacy.py`

## 2. Patient De-identification Tests

The project uses salted SHA-256 hashing to replace original patient identifiers with pseudonymous hashes.

The privacy tests check that:

- The original `patient_id` column is removed after de-identification.
- A `patient_hash` column is generated.
- The generated hash has the expected SHA-256 length of 64 characters.
- The same patient identifier produces a consistent hash when the same salt is used.
- The salt is required for the de-identification process.

These checks help ensure that direct patient identifiers are not carried into the processed data.

## 3. Small-Cell Suppression Tests

Small-cell suppression prevents the publication of patient counts between 1 and 4.

The tests verify that:

- Counts from 1 to 4 are replaced with suppressed values.
- Zero counts remain visible.
- Counts of 5 or more are not suppressed by the basic threshold rule.
- Suppressed values are represented as `null` in exported data.

These checks ensure that small patient counts are not accidentally disclosed.

## 4. Complementary Suppression Tests

Complementary suppression provides additional protection when hidden values could be calculated from published totals.

The tests check that:

- Additional values are suppressed when necessary.
- Published totals are withheld when hidden values could otherwise be reconstructed.
- Suppression works correctly across grouped data.

This reduces the risk of indirectly revealing confidential patient counts.

## 5. Privacy Audit Tests

EpiScope includes auditing functions in `serving/export.py`.

The tests cover:

### Small-Count Audit

`audit_no_small_counts` checks whether patient counts between 1 and 4 appear in published outputs.

### Derivable-Count Audit

`audit_derivable_counts` checks whether suppressed counts can be calculated using other published values.

These audits help detect possible privacy violations before dashboard data is published.

## 6. Why Privacy Testing Is Important

Privacy testing is important because public health datasets contain sensitive information.

Even when direct patient identifiers are removed, small counts or combinations of published statistics may reveal information about individuals.

By testing de-identification, small-cell suppression, complementary suppression, and privacy auditing, EpiScope provides additional confidence that published information follows its privacy requirements.

## 7. Conclusion

The privacy tests in EpiScope help verify that patient information is protected throughout the data processing and publication workflow.

They ensure that direct identifiers are removed, small counts are hidden, and complementary suppression reduces the possibility of reconstructing confidential values.

**Main Test File:** `tests/test_privacy.py`

**Implementation Files:**
- `pipeline/silver/build.py`
- `serving/export.py`

**Related Documentation:** `docs/SMALL_CELL_SUPPRESSION.md`
