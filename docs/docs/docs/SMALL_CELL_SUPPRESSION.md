# Small-Cell Suppression Policy

## 1. Introduction

EpiScope uses small-cell suppression to protect patient privacy when publishing public health data. This technique prevents users from identifying individuals through very small patient counts in district-level statistics and dashboard outputs.

## 2. Suppression Threshold

The project uses a suppression threshold of 5.

- Counts from 1 to 4 are suppressed.
- Counts of 5 or more can be displayed.
- Zero counts remain visible because they indicate that no cases were recorded.

This policy helps prevent the disclosure of information about individuals in districts with very few reported cases.

## 3. How Suppression Works

The suppression mechanism is implemented in `serving/export.py`.

When the export process encounters a patient count between 1 and 4, it replaces the value with `null` in the published JSON output.

The dashboard then displays the suppressed value as **"withheld"** instead of displaying the original number.

This distinction is important because a withheld value does not mean that there were zero patients. It means that the information is intentionally hidden to protect privacy.

## 4. Complementary Suppression

EpiScope also uses complementary suppression to prevent hidden values from being calculated using published totals.

For example, if a table contains a total and several district-level counts, hiding only one small count may not be sufficient. A viewer could subtract the visible counts from the total to discover the hidden value.

To reduce this risk, the system may suppress an additional positive count or withhold the total when necessary.

## 5. Privacy Auditing

The export process includes privacy auditing to check whether suppressed values are accidentally exposed.

Two important auditing functions are:

- `audit_no_small_counts`: Checks for disclosed patient counts between 1 and 4.
- `audit_derivable_counts`: Checks whether hidden counts can be reconstructed from other published values.

These checks help ensure that the published dashboard data follows the project's privacy policy.

## 6. Why Small-Cell Suppression Matters

Small-cell suppression is important because public health datasets may contain sensitive information about individuals.

Without suppression, very small counts could increase the possibility of identifying patients, especially when combined with other publicly available information.

By hiding small counts and preventing simple calculations from revealing them, EpiScope makes public health information safer to share.

## 7. Conclusion

EpiScope combines primary suppression and complementary suppression to protect patient confidentiality in published outputs.

Displaying "withheld" instead of zero also helps users correctly understand the data while maintaining transparency about privacy restrictions.

**Implementation:** `serving/export.py`

**Related Tests:** `tests/test_privacy.py`
