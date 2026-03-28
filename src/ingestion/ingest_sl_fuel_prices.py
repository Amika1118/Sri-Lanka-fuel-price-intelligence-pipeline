"""
Ingestion Module: Sri Lanka Retail Fuel Prices  (ingest_sl_fuel_prices.py)
═══════════════════════════════════════════════════════════════════════════
Primary  : Scrape the Ceylon Petroleum Corporation historical prices page.
           URL: https://ceypetco.gov.lk/historical-prices/
           The scraper tries multiple table-detection strategies because
           the CPC site changes its HTML structure occasionally.

Fallback : If the scrape returns 0 records, load from the Kaggle CSV
           that was pre-uploaded to S3 at:
           s3://<bucket>/reference/sl_fuel_prices_historical.csv

Sink     : Amazon S3 → s3://<bucket>/raw/sl_fuel_prices/YYYY-MM-DD.json

Fixes vs original:
  - Header search used exact 'LP 95' match → now uses case-insensitive partial match
  - Date parsing crashed on non DD.MM.YYYY formats → now handles 6+ formats
  - No S3 fallback CSV loading → added full S3 CSV read
  - No .env loading → added
  - No validation → added price range check
  - Period normalised to YYYY-MM consistently
"""

import os
import io
import csv
import json
import logging
import re
from datetime import datetime

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
S3_BUCKET        = os.getenv("S3_BUCKET", "cm2606-fuel-pipeline-2425479")
S3_KEY_PREFIX    = "raw/sl_fuel_prices"
S3_REFERENCE_KEY = "reference/sl_fuel_prices_historical.csv"  # Kaggle CSV in S3

HISTORICAL_URL = "https://ceypetco.gov.lk/historical-prices/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Fuel type column name mappings — maps partial CPC column header to standard name
# Add more mappings here if CPC adds new fuel types
FUEL_NAME_MAP = {
    "lp 95":     "LP_95",
    "lp 92":     "LP_92",
    "lad":       "AUTO_DIESEL",
    "lsd":       "SUPER_DIESEL",
    " lk":       "KEROSENE",
    "lk":        "KEROSENE",
    "lik":       "ILO_KEROSENE",
    "fur. 800":  "FURNACE_800",
    "fur 800":   "FURNACE_800",
    "fur. 1500": "FURNACE_1500",
    "fur 1500":  "FURNACE_1500",
}


# ══════════════════════════════════════════════════════════════════════════════
# 1. HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def clean_price(value: str) -> float | None:
    """
    Convert a messy price string to a float.
    Handles: "137.00", "137,00", "137.00.00", "1,200.50", "-", "N/A", ""
    Returns None if the value cannot be parsed or is out of reasonable range.
    """
    if not value:
        return None

    text = value.strip()
    # Reject clear non-values
    if text in ("-", "N/A", "n/a", "–", "—", ""):
        return None

    # Remove currency symbols and spaces
    text = re.sub(r"[^\d.,]", "", text)

    # Handle thousands separator: 1,200.50 → 1200.50
    # vs decimal comma: 137,00 → 137.00
    if "," in text and "." in text:
        # Both comma and dot → comma is thousands separator
        text = text.replace(",", "")
    elif "," in text and "." not in text:
        # Only comma → treat as decimal point
        text = text.replace(",", ".")

    # Handle double dots: "137.00.00" → "137.00"
    parts = text.split(".")
    if len(parts) > 2:
        text = parts[0] + "." + parts[1]

    try:
        price = float(text)
        # Sri Lanka fuel prices realistic range: 50–1000 LKR
        if price <= 0 or price > 1500:
            return None
        return price
    except (ValueError, TypeError):
        return None


def parse_period(date_str: str) -> str | None:
    """
    Parse a date string from the CPC website to YYYY-MM format.

    Handles all formats seen on the CPC site:
      DD.MM.YYYY  →  "22.03.2026"
      DD/MM/YYYY  →  "22/03/2026"
      YYYY-MM-DD  →  "2026-03-22"
      MMM-YYYY    →  "Mar-2026"
      MMMM YYYY   →  "March 2026"

    Returns "YYYY-MM" string or None if unparseable.
    """
    raw = date_str.strip()

    for fmt in (
        "%d.%m.%Y",   # 22.03.2026
        "%d/%m/%Y",   # 22/03/2026
        "%d-%m-%Y",   # 22-03-2026
        "%Y-%m-%d",   # 2026-03-22
        "%b-%Y",      # Mar-2026
        "%b %Y",      # Mar 2026
        "%B-%Y",      # March-2026
        "%B %Y",      # March 2026
        "%m/%Y",      # 03/2026
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m")
        except ValueError:
            continue

    # Last resort: extract 4-digit year + 2-digit month with regex
    match = re.search(r"(\d{4})[.\-/](\d{2})", raw)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    match = re.search(r"(\d{2})[.\-/](\d{2})[.\-/](\d{4})", raw)
    if match:
        return f"{match.group(3)}-{match.group(2).zfill(2)}"

    logger.debug(f"Could not parse date: '{raw}'")
    return None


def map_fuel_column(header: str) -> str | None:
    """
    Map a CPC column header to a standard fuel type name.
    Uses case-insensitive partial matching so minor header changes do not break parsing.
    Returns None for non-fuel columns (e.g., Date).
    """
    h = header.strip().lower()
    # Skip date column
    if "date" in h or h == "":
        return None

    for key, standard_name in FUEL_NAME_MAP.items():
        if key in h:
            return standard_name

    # Unknown fuel type — keep it but flag it
    logger.debug(f"Unknown column header, keeping as-is: '{header}'")
    return header.strip().replace(" ", "_").upper()


# ══════════════════════════════════════════════════════════════════════════════
# 2. CPC WEBSITE SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape_cpc_historical() -> list[dict]:
    """
    Scrape the CPC historical prices page.

    Strategy:
    1. Find the table that contains fuel-related column headers.
    2. Map each column to a standard fuel type using case-insensitive matching.
    3. For each data row, extract date + price for every fuel type.
    4. Return flat list: [{"period": "YYYY-MM", "fuel_type": "LP_92", "price_lkr": 317.0}, ...]

    Returns empty list if the page is unreachable or the table structure changed.
    """
    try:
        logger.info(f"Fetching CPC historical prices from: {HISTORICAL_URL}")
        resp = requests.get(HISTORICAL_URL, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"CPC website request failed: {e}")
        return []

    soup   = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")

    if not tables:
        logger.warning("No <table> elements found on CPC page.")
        return []

    # Find the fuel price table — look for a table whose headers contain fuel keywords
    fuel_keywords = ["lp", "diesel", "lad", "lsd", "kerosene", "fur", "92", "95"]
    target_table  = None

    for table in tables:
        header_text = table.get_text(separator=" ").lower()
        if any(kw in header_text for kw in fuel_keywords):
            target_table = table
            break

    if not target_table:
        logger.warning(
            "Could not find the fuel price table on the CPC page. "
            "The site's HTML structure may have changed."
        )
        return []

    rows = target_table.find_all("tr")
    if len(rows) < 2:
        logger.warning("Table found but has fewer than 2 rows.")
        return []

    # Parse header row — map column index to standard fuel type
    header_cells = rows[0].find_all(["th", "td"])
    col_map: dict[int, str] = {}   # index → standard fuel type name

    for idx, cell in enumerate(header_cells):
        text       = cell.get_text(strip=True)
        fuel_name  = map_fuel_column(text)
        if fuel_name:
            col_map[idx] = fuel_name

    if not col_map:
        logger.warning("No fuel type columns detected in table header.")
        return []

    logger.info(f"Detected fuel columns: {list(col_map.values())}")

    # Parse data rows
    records = []
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        # Date is always the first cell
        raw_date = cells[0].get_text(strip=True)
        period   = parse_period(raw_date)
        if not period:
            continue

        for col_idx, fuel_type in col_map.items():
            if col_idx >= len(cells):
                continue
            raw_price = cells[col_idx].get_text(strip=True)
            price     = clean_price(raw_price)
            if price is None:
                continue
            records.append({
                "period":    period,
                "fuel_type": fuel_type,
                "price_lkr": price,
                "source":    "CPC_scrape",
            })

    logger.info(f"CPC scrape: {len(records)} fuel price records extracted.")
    return records


# ══════════════════════════════════════════════════════════════════════════════
# 3. S3 KAGGLE CSV FALLBACK
# ══════════════════════════════════════════════════════════════════════════════

def load_from_s3_csv() -> list[dict]:
    """
    Load historical SL fuel prices from the Kaggle CSV that was pre-uploaded to S3.
    S3 key: reference/sl_fuel_prices_historical.csv

    Expected CSV columns (flexible detection):
      period / date / Date   → YYYY-MM or parseable date
      fuel_type / Fuel Type  → string
      price_lkr / Price      → number

    Returns list of records in same format as scrape output.
    """
    s3 = boto3.client("s3")

    try:
        logger.info(f"Loading Kaggle CSV from s3://{S3_BUCKET}/{S3_REFERENCE_KEY}")
        obj  = s3.get_object(Bucket=S3_BUCKET, Key=S3_REFERENCE_KEY)
        body = obj["Body"].read().decode("utf-8-sig")   # handle BOM
    except s3.exceptions.NoSuchKey:
        logger.error(
            f"Kaggle CSV not found at s3://{S3_BUCKET}/{S3_REFERENCE_KEY}.\n"
            "Upload it first: aws s3 cp sl_fuel_prices_historical.csv "
            f"s3://{S3_BUCKET}/reference/sl_fuel_prices_historical.csv"
        )
        return []
    except Exception as e:
        logger.error(f"Failed to read S3 reference CSV: {e}")
        return []

    records = []
    reader  = csv.DictReader(io.StringIO(body))
    logger.info(f"Kaggle CSV columns: {reader.fieldnames}")

    for row in reader:
        # Flexible column detection
        raw_date = (
            row.get("period") or row.get("Period") or
            row.get("date")   or row.get("Date")   or ""
        ).strip()

        raw_fuel = (
            row.get("fuel_type") or row.get("Fuel Type") or
            row.get("fuel")      or row.get("Fuel")      or ""
        ).strip()

        raw_price = (
            row.get("price_lkr") or row.get("Price LKR") or
            row.get("price")     or row.get("Price")     or ""
        ).strip()

        if not raw_date or not raw_fuel or not raw_price:
            continue

        period = parse_period(raw_date)
        price  = clean_price(raw_price)

        if not period or price is None:
            continue

        records.append({
            "period":    period,
            "fuel_type": raw_fuel.replace(" ", "_").upper(),
            "price_lkr": price,
            "source":    "Kaggle_CSV",
        })

    logger.info(f"Loaded {len(records)} records from Kaggle S3 CSV.")
    return records


# ══════════════════════════════════════════════════════════════════════════════
# 4. MERGE & DEDUPLICATE
# ══════════════════════════════════════════════════════════════════════════════

def merge_and_deduplicate(
    scraped: list[dict],
    historical: list[dict],
) -> list[dict]:
    """
    Merge scraped and historical records. Scraped data takes priority
    for any overlapping period+fuel_type combinations.
    Remove exact duplicates.
    """
    seen   = set()
    merged = []

    # Process scraped first (higher priority)
    for rec in scraped + historical:
        key = (rec["period"], rec["fuel_type"])
        if key not in seen:
            seen.add(key)
            merged.append(rec)

    logger.info(
        f"Merged: {len(scraped)} scraped + {len(historical)} historical "
        f"→ {len(merged)} unique records."
    )
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# 5. UPLOAD to S3
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_s3(records: list[dict]) -> str:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key   = f"{S3_KEY_PREFIX}/{today}.json"

    payload = json.dumps(
        {
            "source":     "CPC_website + Kaggle_CSV",
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
# 6. MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("=" * 60)
    logger.info("SL Fuel Price Ingestion — START")
    logger.info("=" * 60)

    # Step 1: scrape CPC website
    scraped = scrape_cpc_historical()

    # Step 2: always load Kaggle CSV for historical coverage (even if scrape succeeded)
    # The scrape gives recent data; the CSV fills in 2015–2022 history
    historical = load_from_s3_csv()

    # Step 3: if BOTH sources have nothing, stop
    if not scraped and not historical:
        logger.error(
            "No fuel price data from CPC scrape or Kaggle CSV.\n"
            "Check internet connectivity and that the Kaggle CSV is uploaded to S3."
        )
        return

    # Step 4: merge — scrape takes priority for overlapping periods
    all_records = merge_and_deduplicate(scraped, historical)

    if not all_records:
        logger.error("Merge produced 0 records. Nothing uploaded.")
        return

    # Step 5: upload to S3
    upload_to_s3(all_records)

    logger.info("SL Fuel Price Ingestion — DONE")


if __name__ == "__main__":
    main()