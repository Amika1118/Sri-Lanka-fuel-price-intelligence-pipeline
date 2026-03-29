"""
CM2606 Data Engineering — Sri Lanka Fuel Price Intelligence Pipeline
orchestrator.py — Single entry point. Run with: python orchestrator.py

What this script does:
  Step 1 — Calls ingest_crude_oil.main()  → fetches EIA Brent prices → uploads to S3
  Step 2 — Calls ingest_fx_rates.main()   → fetches CBSL USD/LKR     → uploads to S3
  Step 3 — Calls ingest_sl_fuel_prices.main() → scrapes CPC + Kaggle  → uploads to S3
  Step 4 — Triggers the AWS Glue ETL job via boto3, waits for it to finish

IMPORTANT: Step 4 triggers the Glue job remotely via boto3.
  The transform_and_load.py script runs INSIDE AWS Glue (not locally).
  This script just fires the job and waits for the result.
"""

import logging
import os
import sys
import time
from datetime import datetime

import boto3
from dotenv import load_dotenv

# ── Load .env first — before any other imports ─────────────────────────────────
load_dotenv()

# ── Now import ingestion modules (they also call load_dotenv internally) ───────
from ingestion.ingest_crude_oil       import main as ingest_crude_oil
from ingestion.ingest_fx_rates        import main as ingest_fx_rates
from ingestion.ingest_sl_fuel_prices  import main as ingest_sl_fuel_prices

# ── Logging ────────────────────────────────────────────────────────────────────
log_filename = f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename),
    ],
)
logger = logging.getLogger("Orchestrator")

# ── Config from .env ───────────────────────────────────────────────────────────
GLUE_JOB_NAME   = os.getenv("GLUE_JOB_NAME",   "sl-fuel-price-etl")
GLUE_ROLE_ARN   = os.getenv("GLUE_ROLE_ARN")
S3_BUCKET       = os.getenv("S3_BUCKET",        "cm2606-fuel-pipeline-2425479")
REDSHIFT_URL    = os.getenv("REDSHIFT_URL")
REDSHIFT_DB     = os.getenv("REDSHIFT_DB",      "fuel_intelligence")
REDSHIFT_USER   = os.getenv("REDSHIFT_USER",    "admin")
REDSHIFT_PASS   = os.getenv("REDSHIFT_PASS")
REDSHIFT_ROLE   = os.getenv("REDSHIFT_ROLE_ARN")
AWS_REGION      = os.getenv("AWS_REGION",       "eu-west-1")

# How long to wait between Glue status polls (seconds)
GLUE_POLL_INTERVAL = 30
# Max total wait time for Glue job (seconds) — 20 minutes
GLUE_TIMEOUT       = 1200


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 HELPER — Trigger and wait for Glue job
# ══════════════════════════════════════════════════════════════════════════════

def trigger_glue_job() -> bool:
    """
    Start the AWS Glue ETL job via boto3 and poll until it finishes.

    The Glue job runs transform_and_load.py inside AWS's managed Spark environment.
    We pass all required arguments (S3 bucket, Redshift credentials) as job arguments.

    Returns True if job succeeded, False if it failed or timed out.
    """
    if not REDSHIFT_URL or not REDSHIFT_PASS or not GLUE_ROLE_ARN:
        logger.error(
            "Missing required .env values for Glue trigger:\n"
            "  REDSHIFT_URL, REDSHIFT_PASS, GLUE_ROLE_ARN must all be set."
        )
        return False

    glue = boto3.client("glue", region_name=AWS_REGION)

    # Job arguments are passed into the Glue script as sys.argv
    job_args = {
        "--JOB_NAME":          GLUE_JOB_NAME,
        "--S3_BUCKET":         S3_BUCKET,
        "--REDSHIFT_URL":      REDSHIFT_URL,
        "--REDSHIFT_DB":       REDSHIFT_DB,
        "--REDSHIFT_USER":     REDSHIFT_USER,
        "--REDSHIFT_PASS":     REDSHIFT_PASS,
        "--REDSHIFT_ROLE_ARN": REDSHIFT_ROLE,
    }

    try:
        logger.info(f"Triggering Glue job: {GLUE_JOB_NAME}")
        response   = glue.start_job_run(JobName=GLUE_JOB_NAME, Arguments=job_args)
        run_id     = response["JobRunId"]
        logger.info(f"Glue job started. Run ID: {run_id}")
        logger.info("Polling for job completion (this takes 3–8 minutes)...")
    except glue.exceptions.EntityNotFoundException:
        logger.error(
            f"Glue job '{GLUE_JOB_NAME}' does not exist.\n"
            "Create it first using the AWS CLI command in the README."
        )
        return False
    except Exception as e:
        logger.error(f"Failed to start Glue job: {e}")
        return False

    # Poll until SUCCEEDED, FAILED, STOPPED, or timeout
    elapsed = 0
    while elapsed < GLUE_TIMEOUT:
        time.sleep(GLUE_POLL_INTERVAL)
        elapsed += GLUE_POLL_INTERVAL

        try:
            status_resp = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
            state       = status_resp["JobRun"]["JobRunState"]
            duration    = status_resp["JobRun"].get("ExecutionTime", elapsed)
            logger.info(f"Glue job state: {state} (elapsed: {elapsed}s)")
        except Exception as e:
            logger.warning(f"Could not get Glue job status: {e}. Retrying...")
            continue

        if state == "SUCCEEDED":
            logger.info(f"Glue job SUCCEEDED in {duration}s.")
            return True

        if state in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
            # Get error message from Glue
            error_msg = status_resp["JobRun"].get("ErrorMessage", "No error message returned")
            logger.error(f"Glue job {state}: {error_msg}")
            logger.error(
                f"Check full logs in AWS Console → Glue → Jobs → {GLUE_JOB_NAME} → Run history"
            )
            return False

    logger.error(f"Glue job timed out after {GLUE_TIMEOUT}s. Run ID: {run_id}")
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    start = time.time()
    logger.info("=" * 60)
    logger.info("  Sri Lanka Fuel Price Intelligence Pipeline — START")
    logger.info("=" * 60)

    # ── Validate .env is loaded ────────────────────────────────────────────────
    missing = [v for v in ["S3_BUCKET", "EIA_API_KEY", "REDSHIFT_URL",
                            "REDSHIFT_PASS", "GLUE_ROLE_ARN", "GLUE_JOB_NAME"]
               if not os.getenv(v)]
    if missing:
        logger.error(
            f"Missing required .env variables: {missing}\n"
            "Copy config/.env.example to .env and fill in all values."
        )
        sys.exit(1)

    # ── Steps 1–3: Ingestion (run locally, upload to S3) ──────────────────────
    ingestion_steps = [
        ("Crude Oil Prices  → S3 raw/crude_oil/",    ingest_crude_oil),
        ("FX Rates USD/LKR  → S3 raw/fx_rates/",     ingest_fx_rates),
        ("SL Fuel Prices    → S3 raw/sl_fuel_prices/", ingest_sl_fuel_prices),
    ]

    for i, (label, fn) in enumerate(ingestion_steps, 1):
        logger.info(f"[Step {i}/4] {label}")
        try:
            fn()
            logger.info(f"[Step {i}/4] ✅ Completed")
        except Exception as e:
            logger.error(f"[Step {i}/4] ❌ FAILED: {e}", exc_info=True)
            logger.error("Pipeline aborted at ingestion stage.")
            sys.exit(1)

    # ── Step 4: ETL — trigger AWS Glue job (runs in AWS, not locally) ─────────
    logger.info("[Step 4/4] ETL: S3 Raw → AWS Glue PySpark → Redshift")
    logger.info("          (Glue cold start takes 3–5 minutes — please wait)")

    success = trigger_glue_job()

    if not success:
        logger.error("[Step 4/4] ❌ Glue ETL job FAILED. Check AWS Console for details.")
        sys.exit(1)

    logger.info("[Step 4/4] ✅ Completed")

    # ── Done ───────────────────────────────────────────────────────────────────
    elapsed = round(time.time() - start, 1)
    logger.info("=" * 60)
    logger.info(f"  Pipeline completed successfully in {elapsed}s")
    logger.info(f"  Log saved to: {log_filename}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()