import boto3
import pandas as pd
import io
import logging
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REGION          = "ap-south-2"
BRONZE_BUCKET   = "de-project-bronze-hyd-16"
SILVER_BUCKET   = "de-project-silver-hyd-16"

FILES = [
    {"source_key": "source_crm/cust_info.csv",      "target_prefix": "source_crm/cust_info"},
    {"source_key": "source_crm/prd_info.csv",        "target_prefix": "source_crm/prd_info"},
    {"source_key": "source_crm/sales_details.csv",   "target_prefix": "source_crm/sales_details"},
    {"source_key": "source_erp/CUST_AZ12.csv",       "target_prefix": "source_erp/CUST_AZ12"},
    {"source_key": "source_erp/LOC_A101.csv",        "target_prefix": "source_erp/LOC_A101"},
    {"source_key": "source_erp/PX_CAT_G1V2.csv",    "target_prefix": "source_erp/PX_CAT_G1V2"},
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
s3 = boto3.client("s3", region_name=REGION)


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()))


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def log_banner(label: str) -> None:
    line = "=" * 60
    logger.info(line)
    logger.info("  %s", label)
    logger.info(line)


# ---------------------------------------------------------------------------
# Per-file load routine  (mirrors SQL procedure try/catch + timing block)
# ---------------------------------------------------------------------------
def load_file(source_key: str, target_prefix: str) -> dict:
    """
    Returns a summary dict for the pipeline log table.
    Mirrors:  BEGIN TRY / BEGIN CATCH  +  start_time / end_time / duration
    """
    result = {
        "source_key":   source_key,
        "target_key":   f"{target_prefix}.parquet",
        "status":       None,
        "rows_loaded":  None,
        "start_time":   None,
        "end_time":     None,
        "duration_sec": None,
        "error":        None,
    }

    log_banner(f"Loading: {source_key}")

    # -- BEGIN TRY ---------------------------------------------------------
    try:
        start_time = datetime.utcnow()
        result["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("Start time : %s", result["start_time"])

        # Read
        logger.info("Reading CSV from s3://%s/%s", BRONZE_BUCKET, source_key)
        df = read_csv_from_s3(BRONZE_BUCKET, source_key)
        logger.info("Rows read  : %d", len(df))

        # Write
        target_key = result["target_key"]
        logger.info("Writing Parquet to s3://%s/%s", SILVER_BUCKET, target_key)
        write_parquet_to_s3(df, SILVER_BUCKET, target_key)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        result["status"]       = "SUCCESS"
        result["rows_loaded"]  = len(df)
        result["end_time"]     = end_time.strftime("%Y-%m-%d %H:%M:%S")
        result["duration_sec"] = round(duration, 3)

        logger.info("End time   : %s", result["end_time"])
        logger.info("Duration   : %.3f seconds", duration)
        logger.info("Status     : SUCCESS")

    # -- BEGIN CATCH -------------------------------------------------------
    except Exception as exc:
        end_time = datetime.utcnow()
        result["status"]       = "FAILED"
        result["end_time"]     = end_time.strftime("%Y-%m-%d %H:%M:%S")
        result["duration_sec"] = round(
            (end_time - datetime.strptime(result["start_time"], "%Y-%m-%d %H:%M:%S")).total_seconds(), 3
        ) if result["start_time"] else None
        result["error"] = str(exc)

        logger.error("FAILED to load %s", source_key)
        logger.error("Error      : %s", exc)

    return result


# ---------------------------------------------------------------------------
# Pipeline summary
# ---------------------------------------------------------------------------
def print_summary(results: list[dict]) -> None:
    log_banner("PIPELINE SUMMARY")
    header = f"{'File':<45} {'Status':<10} {'Rows':>8}  {'Duration(s)':>12}  Error"
    logger.info(header)
    logger.info("-" * 110)
    for r in results:
        fname = r["source_key"]
        status = r["status"] or "UNKNOWN"
        rows = str(r["rows_loaded"]) if r["rows_loaded"] is not None else "N/A"
        dur = f"{r['duration_sec']:.3f}" if r["duration_sec"] is not None else "N/A"
        err = r["error"] or ""
        logger.info("%-45s %-10s %8s  %12s  %s", fname, status, rows, dur, err)

    total     = len(results)
    succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
    failed    = total - succeeded
    logger.info("-" * 110)
    logger.info("Total: %d  |  Succeeded: %d  |  Failed: %d", total, succeeded, failed)

    if failed:
        raise RuntimeError(
            f"{failed} file(s) failed to load. Check the log above for details."
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    pipeline_start = datetime.utcnow()
    log_banner(f"BRONZE → SILVER  |  Job started at {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    results = [load_file(**f) for f in FILES]

    pipeline_end = datetime.utcnow()
    total_duration = (pipeline_end - pipeline_start).total_seconds()
    logger.info("Total pipeline duration: %.3f seconds", total_duration)

    print_summary(results)


if __name__ == "__main__":
    main()
