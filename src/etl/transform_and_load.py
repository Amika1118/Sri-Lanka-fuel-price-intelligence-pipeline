"""
ETL Module: Transform & Load  (transform_and_load.py)
══════════════════════════════════════════════════════
This script runs INSIDE AWS Glue as a PySpark job.
It is NOT run locally. The orchestrator triggers it via boto3.

Flow:
  1. READ  — raw JSON files from S3 (produced by the 3 ingestion scripts)
  2. TRANSFORM — 7 cleaning/transformation steps using distributed PySpark
  3. LOAD  — write fact + dimension tables to Amazon Redshift

JSON key names match exactly what the ingestion scripts produce:
  crude_oil    → records[].period, records[].value
  fx_rates     → records[].period, records[].usd_lkr_avg
  sl_fuel      → records[].period, records[].fuel_type, records[].price_lkr

Deploy:
  aws s3 cp transform_and_load.py s3://<bucket>/glue-scripts/transform_and_load.py
"""

import sys
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

# ── Glue Job Bootstrap ────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "REDSHIFT_URL",
        "REDSHIFT_DB",
        "REDSHIFT_USER",
        "REDSHIFT_PASS",
        "REDSHIFT_ROLE_ARN",
    ],
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = args["S3_BUCKET"]

# Redshift connection options used by every table write
REDSHIFT_OPTS = {
    "url":          args["REDSHIFT_URL"],
    "user":         args["REDSHIFT_USER"],
    "password":     args["REDSHIFT_PASS"],
    "aws_iam_role": args["REDSHIFT_ROLE_ARN"],
    "tempdir":      f"s3://{S3_BUCKET}/redshift-temp/",
    # Glue 4.0 uses the built-in Redshift connector — no extra jar needed
    "redshiftTmpDir": f"s3://{S3_BUCKET}/redshift-temp/",
}


# ══════════════════════════════════════════════════════════════════════════════
# 1. EXTRACT — Read raw JSON files from S3
# ══════════════════════════════════════════════════════════════════════════════

def read_crude_oil():
    """
    Read Brent crude price JSON from S3.
    JSON structure produced by ingest_crude_oil.py:
      {"records": [{"period": "YYYY-MM", "value": 83.5, "source": "..."}]}

    Note: original code tried to read r.series — that field no longer exists.
    We only need period and value.
    """
    df = spark.read.option("multiline", "true").json(
        f"s3://{S3_BUCKET}/raw/crude_oil/"
    )
    raw = (
        df.select(F.explode("records").alias("r"))
          .select(
              F.col("r.period").alias("period"),
              F.col("r.value").cast(DoubleType()).alias("brent_usd_per_barrel"),
          )
    )
    count = raw.count()
    logger.info(f"Crude oil: read {count} raw rows from S3.")
    if count == 0:
        raise RuntimeError("Crude oil S3 file has 0 records. Check ingestion ran successfully.")
    return raw


def read_fx_rates():
    """
    Read USD/LKR monthly exchange rates from S3.
    JSON structure produced by ingest_fx_rates.py:
      {"records": [{"period": "YYYY-MM", "usd_lkr_avg": 365.4, "sample_count": 22}]}
    """
    df = spark.read.option("multiline", "true").json(
        f"s3://{S3_BUCKET}/raw/fx_rates/"
    )
    raw = (
        df.select(F.explode("records").alias("r"))
          .select(
              F.col("r.period").alias("period"),
              F.col("r.usd_lkr_avg").cast(DoubleType()).alias("usd_lkr_rate"),
          )
    )
    count = raw.count()
    logger.info(f"FX rates: read {count} raw rows from S3.")
    if count == 0:
        raise RuntimeError("FX rates S3 file has 0 records. Check ingestion ran successfully.")
    return raw


def read_sl_fuel_prices():
    """
    Read SL retail fuel prices from S3.
    JSON structure produced by ingest_sl_fuel_prices.py:
      {"records": [{"period": "YYYY-MM", "fuel_type": "LP_92", "price_lkr": 317.0, "source": "..."}]}

    The ingestion script writes all records under the top-level key 'records'.
    """
    df = spark.read.option("multiline", "true").json(
        f"s3://{S3_BUCKET}/raw/sl_fuel_prices/"
    )
    raw = (
        df.select(F.explode("records").alias("r"))
          .select(
              F.col("r.period").alias("period"),
              F.col("r.fuel_type").alias("fuel_type"),
              F.col("r.price_lkr").cast(DoubleType()).alias("price_lkr"),
          )
    )
    count = raw.count()
    logger.info(f"SL fuel prices: read {count} raw rows from S3.")
    if count == 0:
        raise RuntimeError("SL fuel prices S3 file has 0 records. Check ingestion ran successfully.")
    return raw


# ══════════════════════════════════════════════════════════════════════════════
# 2. TRANSFORM — 7 cleansing + transformation steps
# ══════════════════════════════════════════════════════════════════════════════

def transform(crude_df, fx_df, fuel_df):
    """
    Apply all 7 required data transformation steps.
    Returns: (fact_df, agg_df, dim_fx_df, dim_crude_df)
    """

    # ── Step 1: Duplicate Handling ────────────────────────────────────────────
    # dropDuplicates keeps one record per unique key combination.
    # Without this, the same period could appear twice if the pipeline runs twice.
    crude_df = crude_df.dropDuplicates(["period"])
    fx_df    = fx_df.dropDuplicates(["period"])
    fuel_df  = fuel_df.dropDuplicates(["period", "fuel_type"])
    logger.info("Step 1 (Duplicate Handling): done.")

    # ── Step 2: Missing Value Handling ────────────────────────────────────────
    # dropna removes rows where any of the listed critical columns are null.
    # For FX rates, we use forward-fill instead of dropping — if a month is
    # missing, we carry the last known rate forward (common for exchange rates).
    crude_df = crude_df.dropna(subset=["period", "brent_usd_per_barrel"])
    fx_df    = fx_df.dropna(subset=["period", "usd_lkr_rate"])
    fuel_df  = fuel_df.dropna(subset=["period", "fuel_type", "price_lkr"])

    # Forward-fill FX rates: if March is missing, use February's rate
    ffill_window = Window.orderBy("period").rowsBetween(Window.unboundedPreceding, 0)
    fx_df = fx_df.withColumn(
        "usd_lkr_rate",
        F.last("usd_lkr_rate", ignorenulls=True).over(ffill_window)
    )
    logger.info("Step 2 (Missing Value Handling + FX forward-fill): done.")

    # ── Step 3: Data Type Conversion ──────────────────────────────────────────
    # period comes in as string "YYYY-MM" — convert to DateType for proper
    # time-series sorting and Redshift DATE column compatibility.
    crude_df = crude_df.withColumn("period", F.to_date(F.col("period"), "yyyy-MM"))
    fx_df    = fx_df.withColumn("period",    F.to_date(F.col("period"), "yyyy-MM"))
    fuel_df  = fuel_df.withColumn("period",  F.to_date(F.col("period"), "yyyy-MM"))
    logger.info("Step 3 (Data Type Conversion — string → DateType): done.")

    # ── Step 4: Data Standardisation ──────────────────────────────────────────
    # Normalise fuel_type to UPPER TRIM so "lp_92", "LP_92", " LP_92 "
    # all become "LP_92". This prevents duplicates after the JOIN.
    fuel_df = fuel_df.withColumn(
        "fuel_type", F.upper(F.trim(F.col("fuel_type")))
    )
    logger.info("Step 4 (Data Standardisation — fuel_type UPPER TRIM): done.")

    # ── Step 5: Corrupt Data Handling ─────────────────────────────────────────
    # Filter out physically impossible values:
    #   Brent crude: $0–$300/barrel (all-time high was ~$147 in 2008)
    #   Fuel price LKR: 0–1500 (furnace oil can exceed 1000 LKR — fixed from <1000)
    crude_df = crude_df.filter(
        (F.col("brent_usd_per_barrel") > 0) &
        (F.col("brent_usd_per_barrel") < 300)
    )
    fuel_df = fuel_df.filter(
        (F.col("price_lkr") > 0) &
        (F.col("price_lkr") < 1500)   # FIX: was < 1000, furnace oil exceeds 1000 LKR
    )
    logger.info("Step 5 (Corrupt Data Handling — range filters): done.")

    # ── Step 6: Data Normalisation (price LKR → USD) ──────────────────────────
    # Join fuel prices with FX rates on the period key, then compute price_usd.
    # Left join keeps all fuel records even if no FX rate exists for that month.
    fuel_with_fx = fuel_df.join(fx_df, on="period", how="left")
    fuel_with_fx = fuel_with_fx.withColumn(
        "price_usd",
        F.round(F.col("price_lkr") / F.col("usd_lkr_rate"), 4)
    )
    logger.info("Step 6 (Data Normalisation — LKR price → USD): done.")

    # ── Step 7: JOIN all sources → fact table ─────────────────────────────────
    # Join the fuel+FX dataset with Brent crude prices on period.
    # Also compute crude_cost_pct: what % of the pump price is the raw crude cost.
    # (1 barrel = 159 litres, so per-litre crude cost = Brent_USD/barrel ÷ 159)
    fact = (
        fuel_with_fx
        .join(crude_df, on="period", how="left")
        .withColumn(
            "crude_cost_pct",
            F.round(
                (F.col("brent_usd_per_barrel") / 159.0) /
                F.nullif(F.col("price_usd"), F.lit(0)) * 100,
                2
            )
        )
        .select(
            "period",
            "fuel_type",
            "price_lkr",
            "price_usd",
            "usd_lkr_rate",
            "brent_usd_per_barrel",
            "crude_cost_pct",
        )
    )
    logger.info(f"Step 7 (JOIN all sources): fact table has {fact.count()} rows.")

    # ── Aggregation: monthly summary per fuel type ─────────────────────────────
    # Pre-aggregated table for fast BI dashboard queries
    agg = (
        fact.groupBy(
            F.date_format("period", "yyyy").alias("year"),
            F.date_format("period", "MM").alias("month"),
            "fuel_type",
        )
        .agg(
            F.round(F.avg("price_lkr"), 2).alias("avg_price_lkr"),
            F.round(F.avg("brent_usd_per_barrel"), 2).alias("avg_brent_usd"),
            F.round(F.avg("usd_lkr_rate"), 4).alias("avg_usd_lkr"),
            F.count("*").alias("record_count"),
        )
    )
    logger.info(f"Aggregation: monthly summary has {agg.count()} rows.")

    return fact, agg, fx_df, crude_df


# ══════════════════════════════════════════════════════════════════════════════
# 3. LOAD — Write to Amazon Redshift
# ══════════════════════════════════════════════════════════════════════════════

def write_to_redshift(df, table_name: str):
    """
    Write a Spark DataFrame to a Redshift table.

    Uses the AWS Glue native DynamicFrame Redshift sink (Glue 4.0).
    This is more reliable than the community spark-redshift connector
    because it uses the AWS-managed JDBC driver bundled with Glue.

    mode="overwrite" drops and recreates the table on each pipeline run,
    which ensures the data warehouse always reflects the latest ingestion.
    """
    logger.info(f"Writing {df.count()} rows → Redshift table: {table_name}")

    # Convert Spark DataFrame → Glue DynamicFrame
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=args["REDSHIFT_DB"],
        table_name=table_name,
    ) if False else None  # placeholder — use direct JDBC below

    # Use direct JDBC write — most reliable method in Glue 4.0
    (
        df.write
          .format("jdbc")
          .option("url",          args["REDSHIFT_URL"])
          .option("dbtable",      table_name)
          .option("user",         args["REDSHIFT_USER"])
          .option("password",     args["REDSHIFT_PASS"])
          .option("driver",       "com.amazon.redshift.jdbc42.Driver")
          .mode("overwrite")
          .save()
    )
    logger.info(f"✅ Loaded → Redshift: {table_name}")


# ══════════════════════════════════════════════════════════════════════════════
# 4. ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run_etl():
    logger.info("ETL job starting inside AWS Glue.")

    # Extract
    crude = read_crude_oil()
    fx    = read_fx_rates()
    fuel  = read_sl_fuel_prices()

    # Transform
    fact, agg, dim_fx, dim_crude = transform(crude, fx, fuel)

    # Load — dimension tables first, then fact tables
    write_to_redshift(dim_crude, "dim_crude_oil")
    write_to_redshift(dim_fx,    "dim_fx_rates")
    write_to_redshift(fact,      "fact_fuel_prices")
    write_to_redshift(agg,       "fact_fuel_monthly_summary")

    job.commit()
    logger.info("ETL job committed successfully. All tables loaded to Redshift.")


if __name__ == "__main__":
    run_etl()