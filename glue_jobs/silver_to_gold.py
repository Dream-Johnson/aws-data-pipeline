import os
import logging
import psycopg2
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
# Config — Redshift credentials from environment variables
# ---------------------------------------------------------------------------
def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return val

REDSHIFT_HOST     = _require_env("REDSHIFT_HOST")
REDSHIFT_PORT     = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DB       = os.environ.get("REDSHIFT_DB", "dev")
REDSHIFT_USER     = _require_env("REDSHIFT_USER")
REDSHIFT_PASSWORD = _require_env("REDSHIFT_PASSWORD")
REDSHIFT_IAM_ROLE = _require_env("REDSHIFT_IAM_ROLE")   # ARN of Redshift IAM role with S3 read

S3_BUCKET = "de-project-silver-hyd-16"
S3_REGION = "ap-south-2"   # Silver bucket region (cross-region COPY to ap-south-1 Redshift)

# ---------------------------------------------------------------------------
# Table DDL — CREATE TABLE IF NOT EXISTS for each Gold table
# ---------------------------------------------------------------------------
TABLE_DDL = {
    "gold.crm_cust_info": """
        CREATE TABLE IF NOT EXISTS gold.crm_cust_info (
            cst_id              INTEGER,
            cst_key             VARCHAR(50),
            cst_firstname       VARCHAR(100),
            cst_lastname        VARCHAR(100),
            cst_marital_status  VARCHAR(20),
            cst_gndr            VARCHAR(20),
            cst_create_date     DATE
        )
    """,

    "gold.crm_prd_info": """
        CREATE TABLE IF NOT EXISTS gold.crm_prd_info (
            prd_id        INTEGER,
            cat_id        VARCHAR(50),
            prd_key       VARCHAR(100),
            prd_nm        VARCHAR(300),
            prd_cost      FLOAT8,
            prd_line      VARCHAR(50),
            prd_start_dt  DATE,
            prd_end_dt    DATE
        )
    """,

    "gold.crm_sales_details": """
        CREATE TABLE IF NOT EXISTS gold.crm_sales_details (
            sls_ord_num   VARCHAR(50),
            sls_prd_key   VARCHAR(100),
            sls_cust_id   INTEGER,
            sls_order_dt  DATE,
            sls_ship_dt   DATE,
            sls_due_dt    DATE,
            sls_sales     FLOAT8,
            sls_quantity  INTEGER,
            sls_price     FLOAT8
        )
    """,

    "gold.erp_cust_az12": """
        CREATE TABLE IF NOT EXISTS gold.erp_cust_az12 (
            cid    VARCHAR(50),
            bdate  DATE,
            gen    VARCHAR(20)
        )
    """,

    "gold.erp_loc_a101": """
        CREATE TABLE IF NOT EXISTS gold.erp_loc_a101 (
            cid    VARCHAR(50),
            cntry  VARCHAR(100)
        )
    """,

    "gold.erp_px_cat_g1v2": """
        CREATE TABLE IF NOT EXISTS gold.erp_px_cat_g1v2 (
            id           VARCHAR(50),
            cat          VARCHAR(100),
            subcat       VARCHAR(100),
            maintenance  VARCHAR(20)
        )
    """,
}

# ---------------------------------------------------------------------------
# Table load registry
# ---------------------------------------------------------------------------
TABLES = [
    {"table": "gold.crm_cust_info",     "s3_key": "source_crm/cust_info.parquet"},
    {"table": "gold.crm_prd_info",      "s3_key": "source_crm/prd_info.parquet"},
    {"table": "gold.crm_sales_details", "s3_key": "source_crm/sales_details.parquet"},
    {"table": "gold.erp_cust_az12",     "s3_key": "source_erp/CUST_AZ12.parquet"},
    {"table": "gold.erp_loc_a101",      "s3_key": "source_erp/LOC_A101.parquet"},
    {"table": "gold.erp_px_cat_g1v2",   "s3_key": "source_erp/PX_CAT_G1V2.parquet"},
]

# ---------------------------------------------------------------------------
# Redshift connection
# ---------------------------------------------------------------------------
def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        connect_timeout=30,
        sslmode="require",
    )

# ---------------------------------------------------------------------------
# Bootstrap: create schema + all tables once before the load loop
# ---------------------------------------------------------------------------
def bootstrap_gold_schema(conn: psycopg2.extensions.connection) -> None:
    logger.info(">> Creating schema and tables (IF NOT EXISTS) …")
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        for table_name, ddl in TABLE_DDL.items():
            logger.info("   Table: %s", table_name)
            cur.execute(ddl)
    conn.commit()
    logger.info(">> Schema bootstrap complete.")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log_banner(msg: str, char: str = "=") -> None:
    line = char * 60
    logger.info(line)
    logger.info("  %s", msg)
    logger.info(line)


def _row_count(conn: psycopg2.extensions.connection, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]

# ---------------------------------------------------------------------------
# Per-table load  (TRUNCATE + COPY FROM S3 Parquet)
# ---------------------------------------------------------------------------
def load_table(conn: psycopg2.extensions.connection, table: str, s3_key: str) -> dict:
    result = {
        "table":        table,
        "s3_key":       s3_key,
        "status":       None,
        "rows_loaded":  None,
        "start_time":   None,
        "end_time":     None,
        "duration_sec": None,
        "error":        None,
    }

    logger.info("-" * 60)
    logger.info(">> Truncating Table : %s", table)
    logger.info(">> Inserting Data   : %s  ←  s3://%s/%s", table, S3_BUCKET, s3_key)

    try:
        start = datetime.utcnow()
        result["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")

        with conn.cursor() as cur:
            # Mirror SQL TRUNCATE before INSERT
            cur.execute(f"TRUNCATE TABLE {table}")

            # Redshift COPY from Parquet — maps columns by name, handles nulls natively.
            # REGION required because Silver bucket (ap-south-2) differs from
            # Redshift cluster region (ap-south-1).
            copy_sql = f"""
                COPY {table}
                FROM 's3://{S3_BUCKET}/{s3_key}'
                IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                FORMAT AS PARQUET
                REGION '{S3_REGION}'
            """
            cur.execute(copy_sql)

        conn.commit()

        end = datetime.utcnow()
        duration = (end - start).total_seconds()

        rows = _row_count(conn, table)

        result["status"]       = "SUCCESS"
        result["rows_loaded"]  = rows
        result["end_time"]     = end.strftime("%Y-%m-%d %H:%M:%S")
        result["duration_sec"] = round(duration, 3)

        logger.info(">> Rows Loaded  : %d", rows)
        logger.info(">> Load Duration: %.3f seconds", duration)

    except Exception as exc:
        conn.rollback()
        end = datetime.utcnow()
        result["status"]   = "FAILED"
        result["end_time"] = end.strftime("%Y-%m-%d %H:%M:%S")
        if result["start_time"]:
            result["duration_sec"] = round(
                (end - datetime.strptime(result["start_time"], "%Y-%m-%d %H:%M:%S")).total_seconds(), 3
            )
        result["error"] = str(exc)
        logger.error("ERROR in %s : %s", table, exc)

    return result

# ---------------------------------------------------------------------------
# Pipeline summary
# ---------------------------------------------------------------------------
def print_summary(results: list[dict], batch_duration: float) -> None:
    log_banner("PIPELINE SUMMARY")
    header = f"{'Table':<30} {'Status':<10} {'Rows':>8}  {'Duration(s)':>12}  Error"
    logger.info(header)
    logger.info("-" * 100)
    for r in results:
        rows = str(r["rows_loaded"]) if r["rows_loaded"] is not None else "N/A"
        dur  = f"{r['duration_sec']:.3f}" if r["duration_sec"] is not None else "N/A"
        err  = r["error"] or ""
        logger.info("%-30s %-10s %8s  %12s  %s",
                    r["table"], r["status"] or "UNKNOWN", rows, dur, err)

    total     = len(results)
    succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
    failed    = total - succeeded
    logger.info("-" * 100)
    logger.info("Total: %d  |  Succeeded: %d  |  Failed: %d  |  Total Duration: %.3f s",
                total, succeeded, failed, batch_duration)

    if failed:
        raise RuntimeError(f"{failed} table(s) failed to load. See log above for details.")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    batch_start = datetime.utcnow()
    log_banner(f"Loading Gold Layer  |  {batch_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("  Redshift : %s:%d / %s", REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB)
    logger.info("  S3 Source: s3://%s  (region: %s)", S3_BUCKET, S3_REGION)

    conn = get_connection()
    logger.info("  Connected to Redshift Serverless.")

    try:
        log_banner("Bootstrap Schema", char="-")
        bootstrap_gold_schema(conn)

        log_banner("Loading Tables", char="-")
        results = [load_table(conn, **t) for t in TABLES]

    finally:
        conn.close()
        logger.info("  Redshift connection closed.")

    batch_end      = datetime.utcnow()
    batch_duration = (batch_end - batch_start).total_seconds()

    log_banner("Loading Gold Layer is Completed")
    logger.info("  Total Load Duration: %.3f seconds", batch_duration)

    print_summary(results, batch_duration)


if __name__ == "__main__":
    main()
