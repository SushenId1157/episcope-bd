# EpiScope Dashboard User Guide

## 1. Introduction

The EpiScope dashboard provides an interactive way to explore public health information across different districts of Bangladesh.

It contains five coordinated visualizations that help users understand disease trends, geographical patterns, seasonal changes, resource allocation, and early warning signals.

The dashboard uses HTML, CSS, JavaScript, and D3.js to present processed data in an interactive format.

## 2. Understanding the Five Dashboard Views

### 2.1 District Map

The district map provides a geographical overview of disease-related information across Bangladesh.

Users can select a district directly from the map to explore its information in the other dashboard views.

### 2.2 Time Series Chart

The time series chart displays disease trends over time, including observed cases, forecasts, and alert thresholds.

Users can select a specific time range by interacting with the chart to examine changes during a particular period.

### 2.3 Seasonal Heatmap

The seasonal heatmap presents weekly case patterns across different districts.

It helps users identify seasonal trends, compare districts, and understand how disease activity changes over time.

### 2.4 Resource Allocation Chart

The resource allocation view provides information about forecast demand, available stock, and resource allocation.

It helps users understand the relationship between expected demand and available resources.

### 2.5 Early Warning Table

The early warning table ranks districts according to their disease activity compared with seasonal expectations.

It helps users identify districts that may require closer attention.

## 3. Using Dashboard Interactions

The dashboard uses a shared selection state to coordinate its five visualizations.

Users can:

- Click on a district to select it.
- View the selected district across the other dashboard views.
- Use available filters to explore specific data.
- Select a time range in the time series chart.
- Clear the selection to return to the overall view.

These interactions make it easier to explore the data without navigating between separate pages.

## 4. Understanding Dashboard Filters

The dashboard provides filtering options to help users focus on relevant information.

Depending on the available controls, users can filter information by:

- District
- Division
- Disease group
- Time period

Applying filters helps users examine specific parts of the dataset and understand regional differences.

## 5. Understanding Privacy Labels

EpiScope applies privacy protection to published patient counts.

The following rules are important when interpreting dashboard values:

- Counts from 1 to 4 are suppressed.
- Zero counts remain visible.
- Counts of 5 or more are not suppressed by the basic threshold rule.
- Suppressed values are displayed as "withheld" rather than zero.

A "withheld" value means that information has been hidden to protect patient privacy. It does not mean that there were no cases.

## 6. Tips for Users

- Select a district to explore its information across the dashboard.
- Compare different visualizations to understand disease patterns.
- Use the available filters to focus on specific regions or periods.
- Pay attention to early warning information.
- Remember that withheld values represent privacy-protected information.

## 7. Conclusion

The EpiScope dashboard makes public health information easier to explore through interactive charts and visualizations.

By combining geographical information, time-based trends, seasonal patterns, resource allocation, and early warning indicators, it helps users understand public health conditions while maintaining patient privacy.
