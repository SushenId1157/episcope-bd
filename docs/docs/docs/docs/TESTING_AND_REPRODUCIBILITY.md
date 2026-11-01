# Testing and Reproducibility

## 1. Introduction

EpiScope includes automated tests and reproducible pipeline commands. These features help developers check the project and repeat experiments under consistent settings.

## 2. Environment Requirements

The project requires:

- Python 3.11 or later
- Java 17 or later for Spark
- At least 8 GB RAM
- Approximately 2 GB of free disk space for a full-scale run

Python dependencies are listed in `requirements.txt`.

## 3. Reproducible Data Generation

The synthetic data generator uses a configurable random seed.

The default seed is:

`20260101`

Using the same seed and configuration helps produce consistent synthetic data across repeated runs.

The scale factor can also be changed to run experiments with different dataset sizes.

For example:

```bash
make all SF=0.25
```

This runs the pipeline at a quarter of the default scale.

## 4. Automated Testing

The project uses pytest for automated testing.

Run the test suite with:

```bash
make test
```

The command runs the tests located in the `tests/` directory.

These tests help check important project behaviours, including data processing, privacy protection, and pipeline reliability.

## 5. Pipeline Execution

The Makefile provides commands for running individual stages:

| Command | Purpose |
|---|---|
| `make generate` | Generate synthetic data |
| `make pipeline` | Run bronze, silver, and gold stages |
| `make analytics` | Run detection, forecasting, optimisation, and calendar tasks |
| `make serve` | Export dashboard data |
| `make all` | Run the complete workflow |
| `make clean` | Remove generated lake data |

## 6. Incremental Processing

EpiScope supports incremental ingestion through:

```bash
make incremental DROP=/path/to/drop
```

The incremental loader identifies previously ingested content using a content hash. It skips a drop if the same content has already been processed.

This helps avoid duplicate ingestion when the same data is submitted more than once.

## 7. Experiment Records

Pipeline stage timings and experiment results are recorded under `docs/experiments/`.

These records provide evidence for performance measurements and comparisons described in the project report.

## 8. Conclusion

Testing and reproducibility help make EpiScope easier to verify and maintain. Configurable seeds, scale factors, automated tests, incremental processing, and experiment records support repeatable project evaluation.
