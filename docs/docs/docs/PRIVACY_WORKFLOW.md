# Privacy Workflow

## 1. Introduction

EpiScope uses privacy protection methods to reduce the risk of exposing personal patient information. Privacy measures are applied during data preparation and when generating dashboard outputs.

## 2. Patient De-identification

The data preparation process removes the original patient identifier.

The project uses a salted SHA-256 hash to create a `patient_hash` value. This helps distinguish records without publishing the original patient ID.

The salt is supplied through an environment variable and should be kept secret.

## 3. Age Grouping

Instead of keeping exact patient ages in the prepared data, ages are grouped into age bands. This reduces the amount of identifying information in the dataset.

## 4. Data Quality Checks

The quality rules check important fields and conditions, including:

- Patient hash availability and format
- Age and sex values
- District and facility information
- Date and disease-code validity
- Duplicate encounters
- Outcome information

Invalid records can be quarantined instead of being included in the prepared dataset.

## 5. Small-Cell Suppression

The serving process applies a suppression threshold of five.

- Counts from 1 to 4 are suppressed.
- Zero counts remain visible.
- Suppressed values are represented as null in exported JSON.
- The dashboard displays suppressed values as "withheld".

Complementary suppression is also used where necessary to prevent hidden counts from being calculated using published totals.

## 6. Privacy Audits

The serving process includes privacy audit functions to check for small counts and potentially derivable suppressed values.

The project also includes privacy-related tests in `tests/test_privacy.py`.

## 7. Conclusion

Privacy protection is an important part of EpiScope. De-identification, age grouping, data quality checks, small-cell suppression, and privacy audits help reduce the risk of exposing sensitive patient information in published outputs.
