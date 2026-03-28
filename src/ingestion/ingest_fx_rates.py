"""
Ingestion Module: USD/LKR Exchange Rates  (ingest_fx_rates.py)
═══════════════════════════════════════════════════════════════
Primary  : Scrape CBSL (Central Bank of Sri Lanka) exchange rate page.
           CBSL often blocks automated requests (403, empty table).
           The scraper uses a robust POST request with realistic headers.

Fallback : If CBSL scraping fails (very common), read from a local CSV file.
           Download the official CBSL USD/LKR data from:
           https://www.cbsl.gov.lk/cbsl_custom/statistics/ExchangeRates/
           Save it as: data/cbsl_usd_lkr.csv  (NOT cbsl_usd_lkr.csv.csv)

Output   : Monthly averages in YYYY-MM format — the ETL script joins on
           this period key. Daily records are NOT stored — they are
           aggregated here before upload.

Sink     : Amazon S3 → s3://<bucket>/raw/fx_rates/YYYY-MM-DD.json

Fixes vs original:
  - Removed double .csv.csv extension bug in CSV path
  - Added flexible column name detection (handles CBSL's various CSV formats)
  - Added monthly aggregation (original stored raw daily records — ETL needs YYYY-MM)
  - Added CBSL scrape fallback cascade: POST → GET → CSV
  - Added .env loading — bucket name no longer hardcoded
  - Added validation: no zero/negative rates
"""

import os
import csv
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

import boto3
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── Load .env ──────────────────────────────────────────────────────────────────
load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
S3_BUCKET     = os.getenv("S3_BUCKET", "cm2606-fuel-pipeline-2425479")
S3_KEY_PREFIX = "raw/fx_rates"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "730"))

# CBSL exchange rate URLs — try both POST and GET forms
CBSL_POST_URL = "https://www.cbsl.gov.lk/cbsl_custom/exrates/exrates_spot.php"
CBSL_GET_URL  = "https://www.cbsl.gov.lk/rates-and-indicators/exchange-rates/daily-exchange-rates"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Local fallback CSV — CORRECT path, single .csv extension
# Download from CBSL statistics page and save here
BACKUP_CSV = os.path.join(os.path.dirname(__file__), "data", "cbsl_usd_lkr.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 1. CBSL SCRAPE — POST method (original approach, hardened)
# ══════════════════════════════════════════════════════════════════════════════

def scrape_cbsl_post() -> list[dict]:
    """
    Attempt CBSL via POST request. Returns list of daily records
    [{"date": "YYYY-MM-DD", "usd_lkr": float}, ...]
    Returns empty list if the page does not return a usable table.
    """
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    payload = {
        "from":     start_date.strftime("%Y-%m-%d"),
        "to":       end_date.strftime("%Y-%m-%d"),
        "currency": "USD",
    }

    try:
        resp = requests.post(
            CBSL_POST_URL,
            data    = payload,
            headers = HEADERS,
            timeout = 25,
        )
        resp.raise_for_status()

        soup  = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning("CBSL POST: no <table> found in response.")
            return []

        rows    = table.find_all("tr")
        records = []
        for row in rows[1:]:           # skip header
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            raw_date = cols[0].get_text(strip=True)
            raw_rate = cols[1].get_text(strip=True)
            parsed   = _parse_date_and_rate(raw_date, raw_rate)
            if parsed:
                records.append(parsed)

        logger.info(f"CBSL POST scrape: got {len(records)} daily records.")
        return records

    except Exception as e:
        logger.warning(f"CBSL POST scrape failed: {e}")
        return []


def scrape_cbsl_get() -> list[dict]:
    """
    Alternative: try the CBSL daily rates page via GET.
    Returns list of daily records or empty list.
    """
    try:
        resp = requests.get(CBSL_GET_URL, headers=HEADERS, timeout=25)
        resp.raise_for_status()

        soup   = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")
        records = []

        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                raw_date = cols[0].get_text(strip=True)
                raw_rate = cols[1].get_text(strip=True)
                parsed   = _parse_date_and_rate(raw_date, raw_rate)
                if parsed:
                    records.append(parsed)

        logger.info(f"CBSL GET scrape: got {len(records)} daily records.")
        return records

    except Exception as e:
        logger.warning(f"CBSL GET scrape failed: {e}")
        return []


def _parse_date_and_rate(raw_date: str, raw_rate: str) -> dict | None:
    """
    Parse one row from the CBSL table.
    Handles date formats: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD.MM.YYYY
    Returns {"date": "YYYY-MM-DD", "usd_lkr": float} or None.
    """
    # Clean the rate string
    rate_str = raw_rate.replace(",", "").strip()
    try:
        rate = float(rate_str)
        if rate <= 0 or rate > 1000:   # sanity check: LKR/USD always 0–1000
            return None
    except (ValueError, TypeError):
        return None

    # Parse the date — try multiple formats
    date_clean = raw_date.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(date_clean, fmt)
            return {"date": dt.strftime("%Y-%m-%d"), "usd_lkr": rate}
        except ValueError:
            continue

    logger.debug(f"Could not parse date: '{raw_date}'")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# 2. FALLBACK — local CSV
# ══════════════════════════════════════════════════════════════════════════════

def fetch_from_csv() -> list[dict]:
    """
    Read USD/LKR rates from the local backup CSV.

    This handles CBSL's various CSV export formats:
      Format A: Currency, Date, Exchange Rate
      Format B: date, usd_lkr
      Format C: Date, Buying Rate, Selling Rate, Mid Rate
    For Format C, we use the Mid Rate column.

    Fix vs original: path was data/cbsl_usd_lkr.csv.csv (double extension).
    Now correctly: data/cbsl_usd_lkr.csv
    """
    if not os.path.exists(BACKUP_CSV):
        logger.error(
            f"Backup CSV not found: {BACKUP_CSV}\n"
            "Download from: https://www.cbsl.gov.lk/cbsl_custom/statistics/ExchangeRates/\n"
            "Save as: data/cbsl_usd_lkr.csv inside your project folder."
        )
        return []

    records = []
    try:
        with open(BACKUP_CSV, "r", encoding="utf-8-sig") as f:  # utf-8-sig handles BOM
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            logger.info(f"CSV columns found: {headers}")

            for row in reader:
                # Skip non-USD rows if a Currency column exists
                currency = row.get("Currency", row.get("currency", "USD")).strip().upper()
                if currency and currency != "USD":
                    continue

                # Flexible date column detection
                raw_date = (
                    row.get("Date") or row.get("date") or
                    row.get("DATE") or row.get("Period") or ""
                ).strip()

                # Flexible rate column detection — prefer Mid Rate, then Selling, then generic
                raw_rate = (
                    row.get("Mid Rate")      or row.get("mid_rate")    or
                    row.get("Exchange Rate") or row.get("exchange_rate") or
                    row.get("Selling Rate")  or row.get("selling_rate") or
                    row.get("usd_lkr")       or row.get("Rate")         or ""
                ).strip()

                if not raw_date or not raw_rate:
                    continue

                parsed = _parse_date_and_rate(raw_date, raw_rate)
                if parsed:
                    records.append(parsed)

        logger.info(f"Loaded {len(records)} daily records from CSV: {BACKUP_CSV}")
        return records

    except Exception as e:
        logger.error(f"Failed to read backup CSV: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 3. MONTHLY AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_to_monthly(daily_records: list[dict]) -> list[dict]:
    """
    Convert daily USD/LKR records into monthly averages.
    The ETL script joins on YYYY-MM period — daily records would not match.

    Input  : [{"date": "2023-01-05", "usd_lkr": 365.2}, ...]
    Output : [{"period": "2023-01", "usd_lkr_avg": 366.4, "sample_count": 22}, ...]
    """
    buckets: dict[str, list[float]] = defaultdict(list)

    for rec in daily_records:
        date   = rec.get("date", "")
        rate   = rec.get("usd_lkr")
        if len(date) >= 7 and rate:
            month_key = date[:7]   # "YYYY-MM"
            buckets[month_key].append(float(rate))

    monthly = []
    for month, rates in sorted(buckets.items()):
        monthly.append({
            "period":       month,
            "usd_lkr_avg":  round(sum(rates) / len(rates), 4),
            "sample_count": len(rates),
        })

    logger.info(f"Aggregated {len(daily_records)} daily records → {len(monthly)} monthly averages.")
    return monthly


# ══════════════════════════════════════════════════════════════════════════════
# 4. UPLOAD to S3
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_s3(monthly_records: list[dict]) -> str:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key   = f"{S3_KEY_PREFIX}/{today}.json"

    payload = json.dumps(
        {
            "source":     "CBSL_USD_LKR",
            "fetched_at": today,
            "records":    monthly_records,
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
    logger.info(f"Uploaded {len(monthly_records)} monthly records → s3://{S3_BUCKET}/{key}")
    return key


# ══════════════════════════════════════════════════════════════════════════════
# 5. MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("=" * 60)
    logger.info("FX Rates Ingestion — START")
    logger.info("=" * 60)

    # Try CBSL POST scrape first
    daily = scrape_cbsl_post()

    # If that failed, try CBSL GET scrape
    if not daily:
        logger.info("Trying CBSL GET scrape as secondary source...")
        daily = scrape_cbsl_get()

    # If both scrapes failed, fall back to local CSV
    if not daily:
        logger.warning("Both CBSL scrapes failed. Falling back to local CSV.")
        daily = fetch_from_csv()

    # If all three failed, stop with a clear error
    if not daily:
        logger.error(
            "No FX data from any source. Pipeline cannot continue.\n"
            "Action required: save data/cbsl_usd_lkr.csv with USD/LKR historical rates."
        )
        return

    # Aggregate daily → monthly (required for ETL join on YYYY-MM period)
    monthly = aggregate_to_monthly(daily)

    if not monthly:
        logger.error("Monthly aggregation produced 0 records. Check your input data.")
        return

    # Upload to S3
    upload_to_s3(monthly)

    logger.info("FX Rates Ingestion — DONE")


if __name__ == "__main__":
    main()