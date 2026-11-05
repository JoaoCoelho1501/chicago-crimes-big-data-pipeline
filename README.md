# Chicago Crimes — Big Data Pipeline

An end-to-end batch data pipeline that processes Chicago crime records using **Apache Spark and HDFS**, following a Medallion-style architecture (Bronze, Silver, Gold).

**Academic context:** MSc in Business Intelligence & Analytics, ISCAP (2025/26)  
**Author:** João Coelho

## Project overview

This project explores more than **one million crime records** from the City of Chicago's public data portal, covering records from 2022 onward in the project snapshot. The goal is to turn raw API data into structured, analysis-ready datasets and reusable aggregations.

## Architecture

```text
Chicago Open Data API
        |
        v
BRONZE — raw JSON Lines, partitioned by year
        |
        v
SILVER — cleaned and typed data, stored as Parquet
        |
        v
GOLD — analytical aggregations for reporting
```

### Processing stages

- **Bronze ingestion:** paginated API extraction, grouped by year and stored in HDFS.
- **Silver transformation:** column selection and renaming, type conversion, timestamp parsing, derived time attributes, null handling, and Parquet output partitioned by year.
- **Gold analytics:** aggregated datasets covering crime type, district, hour, weekday, month, community area, location, domestic incidents, and arrest rates.
- **Incremental processing:** separate scripts support ingestion and transformation of newly available records.

## Technology stack

- Python and Requests
- Apache Spark / PySpark
- HDFS
- JSON Lines and Apache Parquet (Snappy)
- Matplotlib and Seaborn for exploratory visualizations

## Repository structure

```text
.
├── src/
│   ├── chicago_crimes.py
│   ├── ingest_bronze.py
│   ├── ingest_bronze_incremental.py
│   ├── silver.py
│   ├── silver_incremental.py
│   ├── gold.py
│   ├── gold_update.py
│   └── update_pipeline.py
├── notebooks/
│   └── gold_visualizacoes.ipynb
└── docs/
    └── project report
```

## Data source

The pipeline uses the [City of Chicago Crimes dataset](https://data.cityofchicago.org/resource/ijzp-q8t2.json). The source is maintained by the City of Chicago and may change over time.

## Running the pipeline

The scripts expect a working Spark and HDFS environment. Configure the HDFS destination before running:

```bash
export HDFS_ROOT="hdfs://<host>:<port>/<user>"
python src/chicago_crimes.py
spark-submit src/ingest_bronze.py
spark-submit src/silver.py
spark-submit src/gold.py
```

For subsequent updates, use the incremental scripts and the orchestration entry point in `src/update_pipeline.py`.

## Analytical questions

The Gold layer is designed to support questions such as:

- How does recorded crime volume vary by year and crime category?
- Which districts and community areas have the highest record counts?
- At what hours and on which weekdays are incidents most frequently recorded?
- How do arrest rates differ across crime categories and districts?

## Scope and limitations

- The analysis covers the date range and records retrieved at the time of execution; the public dataset is updated continuously.
- Missing geographic fields are excluded in the current Silver transformation, which can affect geographic summaries.
- Aggregated counts describe recorded incidents and should not be interpreted as a complete measure of crime prevalence.
- Re-running the pipeline requires compatible Spark/HDFS infrastructure and access to the public API.
