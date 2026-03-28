-----

# Sri Lanka Fuel Price Intelligence Pipeline

An end-to-end AWS data pipeline that ingests global crude oil prices, USD/LKR exchange rates, and Sri Lankan retail fuel prices, then transforms and loads the data into a star‑schema data warehouse for BI analytics.

## Overview

This project implements a cloud-native data pipeline for tracking the relationship between international energy markets and domestic fuel prices in Sri Lanka. It ingests data from three heterogeneous sources:

  * **Brent Crude Oil Prices** – U.S. Energy Information Administration (EIA) Open Data API
  * **USD/LKR Exchange Rates** – ExchangeRate.host API
  * **Sri Lanka Retail Fuel Prices** – Web scrape of Ceylon Petroleum Corporation (CPC) current prices + Kaggle historical CSV (2015–2024)

Raw data is staged in an **Amazon S3** data lake, transformed using **AWS Glue (PySpark)**, and loaded into **Amazon Redshift Serverless**. The final star schema is designed to support analytical dashboards in **Amazon QuickSight**.

-----

## Repository Structure

```text
.
├── ingestion/
│   ├── ingest_crude_oil.py      # EIA API → S3 (raw/crude_oil/)
│   ├── ingest_fx_rates.py       # ExchangeRate.host API → S3 (raw/fx_rates/)
│   └── ingest_sl_fuel_prices.py # CPC web scrape + Kaggle CSV → S3 (raw/sl_fuel_prices/)
├── etl/
│   └── fuel_prices_etl.py       # AWS Glue PySpark job – transformations & loading
├── orchestration/
│   └── orchestrator.py          # Python script that runs the entire pipeline sequentially
├── sql/
│   └── redshift_schema.sql      # DDL for star schema (fact & dimension tables)
├── config/
│   └── config.yaml              # Configuration (S3 paths, API keys, Glue job name, etc.)
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variables template
└── README.md
```

-----

## Architecture

The pipeline follows a Lambda‑inspired pattern with these components:

1.  **Data Sources:** REST APIs, web scrape, and static CSV.
2.  **Ingestion:** Python scripts with source‑specific validation; uploads JSON/CSV to S3 raw zone.
3.  **Data Lake:** Amazon S3 (`raw/` zone) with versioning enabled.
4.  **ETL:** AWS Glue (PySpark) performs duplicate removal, missing value handling, data type conversion, standardization, joins, and normalization.
5.  **Data Warehouse:** Amazon Redshift Serverless, utilizing a star schema (`fact_fuel_prices`, `dim_date`, `dim_fuel_type`).
6.  **Orchestration:** `orchestrator.py` manages the workflow via `boto3` with error handling and logging.
7.  **BI Layer:** Amazon QuickSight queries Redshift directly for visualization.

-----

## Prerequisites

  * **AWS Account:** Permissions for S3, Glue, Redshift Serverless, and IAM.
  * **Python 3.9+:** Local environment with `boto3`, `requests`, `beautifulsoup4`, and `pandas`.
  * **API Keys:**
      * [EIA API Key](https://www.eia.gov/opendata/) (Free)
      * [ExchangeRate.host API Key](https://exchangerate.host/) (Free tier)
  * **Kaggle Dataset:** Historical fuel prices CSV (2015–2024).

-----

## Setup

### 1\. Clone the Repository

```bash
git clone https://github.com/yourusername/Sri-Lanka-fuel-pipeline.git
cd sri-lanka-fuel-pipeline
```

### 2\. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3\. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

**Required variables:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `EIA_API_KEY`, `EXCHANGE_RATE_API_KEY`, `S3_RAW_BUCKET`, `REDSHIFT_DB`, etc.

### 4\. Set Up AWS Resources

1.  **S3:** Create buckets for raw and transformed data.
2.  **Glue:** Create an ETL job and upload the script from `etl/fuel_prices_etl.py`.
3.  **Redshift:** Run the DDL provided in `sql/redshift_schema.sql` to initialize the tables.
4.  **IAM:** Ensure your execution role has `AmazonS3FullAccess`, `AWSGlueServiceRole`, and `AmazonRedshiftFullAccess`.

-----

## Running the Pipeline

The entire pipeline can be executed with a single command:

```bash
python orchestration/orchestrator.py
```

**The orchestrator will:**

1.  Trigger ingestion modules to fetch today’s data.
2.  Start the AWS Glue ETL job and poll for completion.
3.  Log all activities to `logs/pipeline_<timestamp>.log`.

-----

## Key Transformations (Glue PySpark)

The ETL job handles complex logic to ensure data quality:

```python
# Duplicate removal
crude_df = crude_df.dropDuplicates(['period'])
fx_df = fx_df.dropDuplicates(['date'])

# Missing value handling – forward fill for FX rates
window = Window.orderBy('date')
fx_df = fx_df.withColumn('rate', last('rate', ignorenulls=True).over(window))

# Join and calculation
joined_df = crude_df.join(fx_df, on='period') \
                   .join(fuel_df, on='period') \
                   .withColumn('price_usd', col('price_lkr') / col('rate'))

# Load to Redshift
joined_df.write \
    .format("jdbc") \
    .option("url", redshift_jdbc_url) \
    .option("dbtable", "fact_fuel_prices") \
    .mode("append") \
    .save()
```

-----

## Assumptions & Limitations

  * **Scraping:** The pipeline assumes the CPC website structure remains stable.
  * **FX Rates:** The `ExchangeRate.host` free tier is limited to 2 years of history; a paid tier is required for a full historical backfill.
  * **Data Updates:** Historical CSV data is static; updates are handled via the web scraper.

## Future Enhancements

  * **Scheduling:** Implement **Amazon EventBridge** for monthly automated triggers.
  * **Data Quality:** Integrate **Great Expectations** or **AWS Glue Data Quality** for automated validation.
  * **Forecasting:** Use the clean data to train a time-series model (e.g., Prophet) to predict future price trends.

-----
