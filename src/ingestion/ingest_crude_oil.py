"""
Ingestion Module: Crude Oil Prices  (ingest_crude_oil.py)
═══════════════════════════════════════════════════════════
Source  : EIA Open Data API v2  — Brent Crude spot price (RBRTE series)
          Endpoint: /v2/petroleum/pri/spt/data/
          NOTE: The original code used /v2/crude-oil-imports/data/ which is a
                DIFFERENT endpoint that tracks physical import volumes by country,
                NOT prices. That endpoint returns 'quantity' (barrels), not a price.
                This file uses the correct price endpoint which returns 'value' (USD/bbl).

Fallback: If the API call fails for any reason, the script reads a local backup
          CSV file (data/brent_crude_backup.csv) so the pipeline never stops.

Sink    : Amazon S3 → s3://<bucket>/raw/crude_oil/YYYY-MM-DD.json

Config  : All sensitive values read from .env file — nothing is hardcoded.
"""

import os
import csv
import json
import logging
import time
from datetime import datetime, timedelta

import boto3
import requests
from dotenv import load_dotenv

# ── Load .env file ─────────────────────────────────────────────────────────────
# load_dotenv() reads the .env file in your project root and puts all values
# into environment variables so os.getenv() can read them.
load_dotenv()

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config — all values come from .env, nothing hardcoded ─────────────────────
S3_BUCKET       = os.getenv("S3_BUCKET", "cm2606-fuel-pipeline-2425479")
S3_KEY_PREFIX   = "raw/crude_oil"           # matches redshift_schema.sql and ETL script
EIA_API_KEY     = os.getenv("EIA_API_KEY")  # set this in your .env file
LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "730"))

# ── Correct EIA endpoint for Brent crude SPOT PRICE ───────────────────────────
# /v2/petroleum/pri/spt/data/ = Petroleum Prices → Spot Prices
# series RBRTE = Europe Brent Spot Price FOB (Dollars per Barrel)
EIA_URL     = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
SERIES_ID   = "RBRTE"

# ── Local backup CSV ───────────────────────────────────────────────────────────
# If the API fails, the script falls back to this file.
# CSV must have columns: period (YYYY-MM), value (number)
# You can download historical Brent data from:
# https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=RBRTE&f=M
BACKUP_CSV = os.path.join(os.path.dirname(__file__), "data", "brent_crude_backup.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 1. FETCH from EIA API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_from_eia_api() -> list[dict]:
    """
    Call the EIA v2 API and return a list of monthly Brent price records.
    Each record: {"period": "YYYY-MM", "value": float, "source": "EIA_API"}

    Retries up to 3 times with a 5-second wait between attempts.
    Returns empty list if all attempts fail.
    """
    if not EIA_API_KEY:
        logger.error("EIA_API_KEY is not set in your .env file. Cannot call API.")
        return []

    end_month   = datetime.utcnow()
    start_month = end_month - timedelta(days=LOOKBACK_DAYS)

    params = {
        "api_key":              EIA_API_KEY,
        "frequency":            "monthly",
        "data[0]":              "value",        # 'value' = price in USD/barrel
        "facets[series][]":     SERIES_ID,      # RBRTE = Brent crude
        "start":                start_month.strftime("%Y-%m"),
        "end":                  end_month.strftime("%Y-%m"),
        "sort[0][column]":      "period",
        "sort[0][direction]":   "asc",
        "offset":               0,
        "length":               5000,
    }

    for attempt in range(1, 4):
        try:
            logger.info(f"EIA API call attempt {attempt}/3 ...")
            response = requests.get(EIA_URL, params=params, timeout=30)
            response.raise_for_status()

            raw = response.json()
            items = raw.get("response", {}).get("data", [])

            if not items:
                logger.warning("EIA API returned 0 records.")
                return []

            records = []
            for item in items:
                period = item.get("period")   # "YYYY-MM"
                value  = item.get("value")    # price USD/barrel as string or number
                if not period or value is None:
                    continue
                try:
                    records.append({
                        "period": period,
                        "value":  float(value),
                        "source": "EIA_API"
                    })
                except (ValueError, TypeError):
                    logger.debug(f"Skipping non-numeric value: {value}")

            logger.info(f"EIA API returned {len(records)} Brent price records.")
            return records

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error from EIA API: {e}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error reaching EIA API: {e}")
        except requests.exceptions.Timeout:
            logger.error("EIA API request timed out.")
        except Exception as e:
            logger.error(f"Unexpected error calling EIA API: {e}")

        if attempt < 3:
            logger.info("Waiting 5 seconds before retry...")
            time.sleep(5)

    logger.warning("All 3 EIA API attempts failed.")
    return []


# ══════════════════════════════════════════════════════════════════════════════
# 2. FALLBACK — read from local backup CSV
# ══════════════════════════════════════════════════════════════════════════════

def fetch_from_backup_csv() -> list[dict]:
    """
    Read Brent crude prices from a local backup CSV file.

    Expected CSV format (two columns, with header row):
        period,value
        2022-01,83.41
        2022-02,95.49
        ...

    You can get this CSV from EIA website:
    https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=RBRTE&f=M
    Save it as: data/brent_crude_backup.csv inside your project folder.
    """
    if not os.path.exists(BACKUP_CSV):
        logger.error(
            f"Backup CSV not found at: {BACKUP_CSV}\n"
            f"Download Brent historical data from EIA and save it there."
        )
        return []

    records = []
    try:
        with open(BACKUP_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Try common column name variations
                period = (row.get("period") or row.get("Period") or
                          row.get("date")   or row.get("Date", "")).strip()
                value  = (row.get("value")  or row.get("Value") or
                          row.get("price")  or row.get("Price", "")).strip()

                if not period or not value:
                    continue

                # Normalise period to YYYY-MM
                # Handles both YYYY-MM and MM/DD/YYYY formats
                if "/" in period:
                    try:
                        parts = period.split("/")
                        period = f"{parts[2]}-{parts[0].zfill(2)}"
                    except Exception:
                        continue

                try:
                    records.append({
                        "period": period[:7],  # keep only YYYY-MM
                        "value":  float(value.replace(",", "")),
                        "source": "backup_CSV"
                    })
                except (ValueError, TypeError):
                    continue

        logger.info(f"Loaded {len(records)} records from backup CSV: {BACKUP_CSV}")
        return records

    except Exception as e:
        logger.error(f"Failed to read backup CSV: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 3. DATA QUALITY CHECK
# ══════════════════════════════════════════════════════════════════════════════

def validate_records(records: list[dict]) -> list[dict]:
    """
    Remove records that fail basic sanity checks:
    - period must be a string in YYYY-MM format
    - value must be a positive number less than 300 (Brent crude has never exceeded $150)
    Logs how many records were removed.
    """
    valid   = []
    removed = 0
    for rec in records:
        period = rec.get("period", "")
        value  = rec.get("value")

        # Check period format YYYY-MM
        if len(period) != 7 or period[4] != "-":
            removed += 1
            continue

        # Check price is a realistic number
        if value is None or not (0 < value < 300):
            removed += 1
            continue

        valid.append(rec)

    if removed:
        logger.warning(f"Data quality: removed {removed} invalid records.")
    logger.info(f"Data quality: {len(valid)} records passed validation.")
    return valid


# ══════════════════════════════════════════════════════════════════════════════
# 4. UPLOAD to S3
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_s3(records: list[dict]) -> str:
    """
    Serialise records to JSON and write to S3 raw zone.
    Key pattern: raw/crude_oil/YYYY-MM-DD.json
    Overwrites today's file if the pipeline is re-run (idempotent).
    Returns the full S3 key.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key   = f"{S3_KEY_PREFIX}/{today}.json"

    payload = json.dumps(
        {
            "source":     "EIA_Brent_RBRTE",
            "fetched_at": today,
            "records":    records,
        },
        indent=2,
    )

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = key,
        Body        = payload.encode("utf-8"),
        ContentType = "application/json",
    )
    logger.info(f"Uploaded {len(records)} records → s3://{S3_BUCKET}/{key}")
    return key


# ══════════════════════════════════════════════════════════════════════════════
# 5. MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("=" * 60)
    logger.info("Crude Oil Ingestion — START")
    logger.info("=" * 60)

    # Step 1: try EIA API first
    records = fetch_from_eia_api()

    # Step 2: if API failed, fall back to local CSV
    if not records:
        logger.warning("API returned no data. Falling back to local backup CSV.")
        records = fetch_from_backup_csv()

    # Step 3: if still nothing, abort with a clear error
    if not records:
        logger.error(
            "No crude oil data obtained from API or backup CSV. "
            "Check your EIA_API_KEY in .env and ensure data/brent_crude_backup.csv exists."
        )
        return

    # Step 4: data quality check
    records = validate_records(records)
    if not records:
        logger.error("All records failed data quality checks. Nothing uploaded.")
        return

    # Step 5: upload to S3
    upload_to_s3(records)

    logger.info("Crude Oil Ingestion — DONE")


if __name__ == "__main__":
    main()