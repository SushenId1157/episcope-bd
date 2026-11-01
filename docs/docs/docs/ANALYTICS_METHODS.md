# Analytics Methods

## 1. Introduction

EpiScope uses analytics to identify unusual disease activity, estimate future cases, and support the allocation of limited healthcare resources. The analytics components are located in the `analytics/` directory.

## 2. Outbreak Detection

The project uses the EARS (Early Aberration Reporting System) approach to detect unusual changes in disease activity.

The implementation includes three methods:

- **C1:** Detects unusual activity using recent observations.
- **C2:** Uses a reference period to identify increases in activity.
- **C3:** Uses a longer recent observation window to identify sustained increases.

The detection process works with seasonally adjusted disease counts at district level. Its performance is evaluated using injected outbreak scenarios.

## 3. Disease Forecasting

EpiScope uses Negative Binomial Generalized Linear Models (NB-GLM) for forecasting.

The forecasting component:
- Builds forecasts for district-level disease time series.
- Uses historical information to estimate future cases.
- Produces forecasts for one to four weeks ahead.
- Includes 80% prediction intervals to represent forecast uncertainty.

The project evaluates forecasting performance through rolling-origin backtesting and compares results with a seasonal-naive baseline.

## 4. Resource Allocation

The allocation component uses a linear programming optimisation model with the CBC solver.

It considers resource demand, available stock, and a fixed supply budget to determine allocations.

The output includes:
- Resource demand
- Available stock
- Allocated quantity
- Unmet demand

The project also compares allocation outcomes with a population-proportional baseline and examines different budget and equity scenarios.

## 5. Preparedness Calendar

The preparedness calendar uses seasonal decomposition to identify patterns in disease activity.

It provides information that can help users understand when additional preparation may be useful.

## 6. Evaluation

The project includes separate evaluation components:

- Outbreak detection evaluation measures sensitivity and false-alert performance.
- Forecast backtesting compares predictions with observed values across rolling time periods.
- Allocation experiments compare resource distribution and unmet demand under different scenarios.

## 7. Conclusion

The analytics components connect disease surveillance with forecasting and resource planning. Together, they provide analytical outputs that support the EpiScope dashboard.
