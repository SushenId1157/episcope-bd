# EpiScope Dashboard Data Dictionary

## 1. Introduction

This data dictionary explains the important fields and variables used in the EpiScope dashboard.

The dashboard uses processed JSON data to display disease trends, geographical information, seasonal patterns, resource allocation, and early warning indicators across districts in Bangladesh.

This document helps users understand what each field represents and how to interpret the displayed information.

## 2. General Dashboard Fields

| Field | Description |
|---|---|
| `district_id` | Unique identifier assigned to a district. |
| `district_name` | Name of the district. |
| `division` | Administrative division to which the district belongs. |
| `disease_group` | Disease category being displayed, such as Dengue, Malaria, or Diarrhoeal disease. |
| `epi_year` | Epidemiological year associated with the observation. |
| `epi_week` | Epidemiological week associated with the observation. |

## 3. District Map Fields

The district map provides geographical information and allows users to compare disease activity across districts.

| Field | Description |
|---|---|
| `cases` | Total reported cases for the selected district and disease group. |
| `incidence_per_100k` | Disease incidence expressed per 100,000 people. |
| `alert_weeks` | Number of weeks during which the district experienced an alert. |
| `population` | Population associated with the district. |

The map allows users to switch between three metrics:

- Incidence per 100,000 people.
- Total cases.
- Alert weeks.

## 4. Time Series Fields

The time series chart displays observed disease activity, forecasts, and alert thresholds over time.

| Field | Description |
|---|---|
| `cases` | Observed weekly disease cases. |
| `forecast` | Predicted number of cases for a future epidemiological week. |
| `naive` | Baseline forecast used for comparison. |
| `lower_80` | Lower boundary of the 80% forecast interval. |
| `upper_80` | Upper boundary of the 80% forecast interval. |
| `target_year` | Year associated with a forecast target. |
| `target_week` | Epidemiological week associated with a forecast target. |

These fields help users compare historical observations with predicted disease activity.

## 5. Seasonal Heatmap Fields

The seasonal heatmap helps users understand how disease activity changes across districts and epidemiological weeks.

| Field | Description |
|---|---|
| `district_name` | Name of the district. |
| `division` | Administrative division of the district. |
| `epi_week` | Epidemiological week represented in the heatmap. |
| `mean_cases` | Average weekly case count represented by the heatmap. |
| `mean_incidence` | Average incidence per 100,000 people. |
| `disease_group` | Disease category represented in the heatmap. |

Higher values indicate greater average disease activity, while suppressed values are displayed as "withheld".

## 6. Resource Allocation Fields

The resource allocation chart presents forecast demand, available stock, and recommended resource allocation.

| Field | Description |
|---|---|
| `resource` | Type of resource being allocated. |
| `demand` | Forecast resource requirement. |
| `stock` | Usable stock currently available. |
| `allocated` | Recommended additional resource allocation. |
| `unmet` | Remaining demand that cannot be covered. |
| `district_id` | Unique identifier of the district. |
| `district_name` | Name of the district. |

The dashboard currently displays IV saline units over a four-week planning horizon.

The chart compares forecast demand with usable stock and recommended allocation to help identify remaining resource shortages.

## 7. Early Warning Table Fields

The early warning table ranks districts according to their disease activity compared with seasonal expectations.

| Field | Description |
|---|---|
| `district_name` | Name of the district being monitored. |
| `division` | Administrative division of the district. |
| `epi_week` | Epidemiological week associated with the warning. |
| `cases` | Observed disease cases. |
| `expected` | Expected cases based on seasonal patterns. |
| `severity` | Standard deviation-based measure indicating how far observed activity is above seasonal expectations. |

A higher severity value indicates that observed disease activity is further above the expected seasonal level.

## 8. Dashboard Filters and Selection Variables

The dashboard uses a shared JavaScript state to coordinate interactions across all five views.

| Variable | Description |
|---|---|
| `disease` | Currently selected disease group. |
| `division` | Currently selected administrative division. |
| `district` | Currently selected district. |
| `weekRange` | Selected epidemiological time range. |
| `metric` | Selected metric for the district map. |

When a user selects a district, the other visualizations respond to that selection.

Users can also change the disease group, filter by division, select a time range, and reset their selections.

## 9. Privacy-Related Values

EpiScope applies small-cell suppression to protect sensitive patient information.

The following rules apply to published patient counts:

- Counts from 1 to 4 are suppressed.
- Zero counts remain visible.
- Counts of 5 or more are not suppressed by the basic threshold rule.
- Suppressed values are represented as `null` in exported JSON.
- The dashboard displays suppressed values as "withheld".

The word "withheld" indicates that a value has been hidden for privacy protection. It does not mean that the actual count is zero.

Complementary suppression is also used to reduce the possibility of calculating hidden counts from published totals.

## 10. Dashboard Data Files

The dashboard loads its processed information from the following JSON files:

| File | Purpose |
|---|---|
| `meta.json` | Provides dashboard metadata and available disease groups. |
| `map_districts.json` | Contains district-level information for the map. |
| `timeseries.json` | Contains observed and forecast time series data. |
| `seasonal_heatmap.json` | Contains seasonal disease patterns across districts and weeks. |
| `allocation.json` | Contains resource demand, stock, and allocation information. |
| `early_warning.json` | Contains district-level early warning indicators. |
| `preparedness_calendar.json` | Provides preparedness calendar information. |
| `districts.geojson` | Provides geographical features used for the district map. |

## 11. Conclusion

The EpiScope data dictionary provides a reference for understanding the fields and variables used throughout the dashboard.

By explaining the meaning of each field, it makes the dashboard easier to interpret and helps users understand the relationship between disease activity, geographical information, resource allocation, and early warning indicators.

It also explains how privacy-protected values should be interpreted when viewing published dashboard data.
