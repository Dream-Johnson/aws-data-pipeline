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
REGION        = "ap-south-2"
SILVER_BUCKET = "de-project-silver-hyd-16"

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
s3 = boto3.client("s3", region_name=REGION)


def read_parquet(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def write_parquet(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=buf.getvalue())


def log_banner(msg: str, char: str = "=") -> None:
    line = char * 60
    logger.info(line)
    logger.info("  %s", msg)
    logger.info(line)


# ---------------------------------------------------------------------------
# Transformation functions  (one per SQL INSERT block)
# ---------------------------------------------------------------------------

def transform_crm_cust_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    silver.crm_cust_info
    - Drop rows where cst_id is NULL
    - Deduplicate: keep the most-recent record per cst_id  (ROW_NUMBER DESC)
    - TRIM firstname / lastname
    - Normalize marital status codes  (S→Single, M→Married, else n/a)
    - Normalize gender codes           (F→Female, M→Male,   else n/a)
    """
    df = df[df["cst_id"].notna()].copy()

    # Most-recent record per customer  (mirrors ROW_NUMBER … ORDER BY cst_create_date DESC)
    df = df.sort_values("cst_create_date", ascending=False)
    df = df.drop_duplicates(subset="cst_id", keep="first")

    df["cst_firstname"] = df["cst_firstname"].str.strip()
    df["cst_lastname"]  = df["cst_lastname"].str.strip()

    marital_map = {"S": "Single", "M": "Married"}
    df["cst_marital_status"] = (
        df["cst_marital_status"].str.strip().str.upper()
        .map(marital_map).fillna("n/a")
    )

    gender_map = {"F": "Female", "M": "Male"}
    df["cst_gndr"] = (
        df["cst_gndr"].str.strip().str.upper()
        .map(gender_map).fillna("n/a")
    )

    return df[["cst_id", "cst_key", "cst_firstname", "cst_lastname",
               "cst_marital_status", "cst_gndr", "cst_create_date"]]


def transform_crm_prd_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    silver.crm_prd_info
    - cat_id  : first 5 chars of original prd_key, '-' → '_'
    - prd_key : characters from position 7 onwards  (SQL SUBSTRING(prd_key,7,LEN))
    - prd_cost: NULL → 0
    - prd_line: code → descriptive label
    - prd_end_dt: LEAD(prd_start_dt) - 1 day, partitioned by prd_key
    """
    df = df.copy()

    # Derive cat_id and clean prd_key from the original column BEFORE overwriting it
    df["cat_id"]  = df["prd_key"].str[:5].str.replace("-", "_", regex=False)
    df["prd_key"] = df["prd_key"].str[6:]          # SQL position 7 = Python index 6

    df["prd_cost"] = df["prd_cost"].fillna(0)

    line_map = {"M": "Mountain", "R": "Road", "S": "Other Sales", "T": "Touring"}
    df["prd_line"] = (
        df["prd_line"].str.strip().str.upper()
        .map(line_map).fillna("n/a")
    )

    df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors="coerce")

    # prd_end_dt = LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 day
    df = df.sort_values(["prd_key", "prd_start_dt"])
    df["prd_end_dt"] = (
        df.groupby("prd_key")["prd_start_dt"]
        .shift(-1)                              # LEAD: next row's start date
        - pd.Timedelta(days=1)                  # minus 1 day
    )

    return df[["prd_id", "cat_id", "prd_key", "prd_nm", "prd_cost",
               "prd_line", "prd_start_dt", "prd_end_dt"]]


def _parse_date_int(series: pd.Series) -> pd.Series:
    """
    Convert integer dates (YYYYMMDD) to datetime.
    Returns NULL when value is 0, NaN, or doesn't have exactly 8 digits.
    Mirrors: CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                  ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    """
    def _convert(val):
        if pd.isna(val):
            return pd.NaT
        try:
            s = str(int(val))
        except (ValueError, TypeError):
            return pd.NaT
        if int(val) == 0 or len(s) != 8:
            return pd.NaT
        try:
            return pd.to_datetime(s, format="%Y%m%d")
        except Exception:
            return pd.NaT

    return series.apply(_convert)


def transform_crm_sales_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    silver.crm_sales_details
    - Date columns: integer YYYYMMDD → date, invalid/zero → NULL
    - sls_sales : recalculate as qty * ABS(price) when NULL, <=0, or inconsistent
    - sls_price : derive as sales / qty when NULL or <=0  (uses ORIGINAL sales)
    """
    df = df.copy()

    df["sls_order_dt"] = _parse_date_int(df["sls_order_dt"])
    df["sls_ship_dt"]  = _parse_date_int(df["sls_ship_dt"])
    df["sls_due_dt"]   = _parse_date_int(df["sls_due_dt"])

    # Capture original values before any recalculation  (SQL SELECT uses source values)
    orig_sales = df["sls_sales"].copy()
    orig_price = df["sls_price"].copy()
    orig_qty   = df["sls_quantity"].copy()

    recalc_sales = orig_qty * orig_price.abs()

    bad_sales = (
        orig_sales.isna()
        | (orig_sales <= 0)
        | (orig_sales != recalc_sales)
    )
    df["sls_sales"] = orig_sales.where(~bad_sales, recalc_sales)

    bad_price = orig_price.isna() | (orig_price <= 0)
    # Derived price uses original sls_sales / original sls_quantity (NULLIF → 0→NaN)
    derived_price = orig_sales / orig_qty.replace(0, pd.NA)
    df["sls_price"] = orig_price.where(~bad_price, derived_price)

    return df[["sls_ord_num", "sls_prd_key", "sls_cust_id",
               "sls_order_dt", "sls_ship_dt", "sls_due_dt",
               "sls_sales", "sls_quantity", "sls_price"]]


def transform_erp_cust_az12(df: pd.DataFrame) -> pd.DataFrame:
    """
    silver.erp_cust_az12
    - cid  : strip leading 'NAS' prefix when present
    - bdate: set to NULL if in the future
    - gen  : normalize  F/FEMALE → Female, M/MALE → Male, else n/a
    """
    df = df.copy()
    # CONTROLLED BUG: df.columns = df.columns.str.lower() intentionally removed.
    # CUST_AZ12.parquet has uppercase columns ['CID', 'BDATE', 'GEN'].
    # Without this line, df["cid"] below raises KeyError: 'cid' — column is 'CID'.
    # This failure triggers Step Functions Catch → InvokeDiagnosticAgent Lambda.

    df["cid"] = df["cid"].apply(
        lambda v: v[3:] if isinstance(v, str) and v.upper().startswith("NAS") else v
    )

    df["bdate"] = pd.to_datetime(df["bdate"], errors="coerce")
    future_mask = df["bdate"] > pd.Timestamp.now()
    df.loc[future_mask, "bdate"] = pd.NaT

    female_vals = {"F", "FEMALE"}
    male_vals   = {"M", "MALE"}

    def _normalize_gen(val):
        if pd.isna(val):
            return "n/a"
        u = str(val).strip().upper()
        if u in female_vals:
            return "Female"
        if u in male_vals:
            return "Male"
        return "n/a"

    df["gen"] = df["gen"].apply(_normalize_gen)

    return df[["cid", "bdate", "gen"]]


def transform_erp_loc_a101(df: pd.DataFrame) -> pd.DataFrame:
    """
    silver.erp_loc_a101
    - cid  : remove all '-' characters
    - cntry: DE→Germany, US/USA→United States, blank/NULL→n/a, else trim
    """
    df = df.copy()
    df.columns = df.columns.str.lower()

    df["cid"] = df["cid"].str.replace("-", "", regex=False)

    def _normalize_country(val):
        if pd.isna(val) or str(val).strip() == "":
            return "n/a"
        v = str(val).strip()
        if v == "DE":
            return "Germany"
        if v in ("US", "USA"):
            return "United States"
        return v

    df["cntry"] = df["cntry"].apply(_normalize_country)

    return df[["cid", "cntry"]]


def transform_erp_px_cat_g1v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    silver.erp_px_cat_g1v2
    No transformations — select columns as-is.
    """
    df = df.copy()
    df.columns = df.columns.str.lower()
    return df[["id", "cat", "subcat", "maintenance"]]


# ---------------------------------------------------------------------------
# Table registry: key, label, transform function
# ---------------------------------------------------------------------------
TABLES = [
    {
        "key":       "source_crm/cust_info.parquet",
        "label":     "silver.crm_cust_info",
        "transform": transform_crm_cust_info,
    },
    {
        "key":       "source_crm/prd_info.parquet",
        "label":     "silver.crm_prd_info",
        "transform": transform_crm_prd_info,
    },
    {
        "key":       "source_crm/sales_details.parquet",
        "label":     "silver.crm_sales_details",
        "transform": transform_crm_sales_details,
    },
    {
        "key":       "source_erp/CUST_AZ12.parquet",
        "label":     "silver.erp_cust_az12",
        "transform": transform_erp_cust_az12,
    },
    {
        "key":       "source_erp/LOC_A101.parquet",
        "label":     "silver.erp_loc_a101",
        "transform": transform_erp_loc_a101,
    },
    {
        "key":       "source_erp/PX_CAT_G1V2.parquet",
        "label":     "silver.erp_px_cat_g1v2",
        "transform": transform_erp_px_cat_g1v2,
    },
]

# ---------------------------------------------------------------------------
# Per-table load routine  (mirrors SQL timing + try/catch pattern)
# ---------------------------------------------------------------------------
def load_table(key: str, label: str, transform) -> dict:
    result = {
        "label":       label,
        "key":         key,
        "status":      None,
        "rows_in":     None,
        "rows_out":    None,
        "start_time":  None,
        "end_time":    None,
        "duration_sec": None,
        "error":       None,
    }

    logger.info("-" * 60)
    logger.info(">> Truncating / Loading : %s", label)

    try:
        start = datetime.utcnow()
        result["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")

        logger.info(">> Reading  : s3://%s/%s", SILVER_BUCKET, key)
        raw = read_parquet(key)
        result["rows_in"] = len(raw)
        logger.info(">> Rows in  : %d", len(raw))

        logger.info(">> Applying transformations …")
        transformed = transform(raw)
        result["rows_out"] = len(transformed)
        logger.info(">> Rows out : %d", len(transformed))

        logger.info(">> Writing  : s3://%s/%s", SILVER_BUCKET, key)
        write_parquet(transformed, key)

        end = datetime.utcnow()
        duration = (end - start).total_seconds()

        result["status"]       = "SUCCESS"
        result["end_time"]     = end.strftime("%Y-%m-%d %H:%M:%S")
        result["duration_sec"] = round(duration, 3)
        logger.info(">> Load Duration: %.3f seconds", duration)

    except Exception as exc:
        end = datetime.utcnow()
        result["status"]   = "FAILED"
        result["end_time"] = end.strftime("%Y-%m-%d %H:%M:%S")
        if result["start_time"]:
            result["duration_sec"] = round(
                (end - datetime.strptime(result["start_time"], "%Y-%m-%d %H:%M:%S")).total_seconds(), 3
            )
        result["error"] = str(exc)
        logger.error("ERROR in %s : %s", label, exc)

    return result


# ---------------------------------------------------------------------------
# Pipeline summary
# ---------------------------------------------------------------------------
def print_summary(results: list[dict], batch_duration: float) -> None:
    log_banner("PIPELINE SUMMARY")
    header = f"{'Table':<30} {'Status':<10} {'Rows In':>8} {'Rows Out':>9} {'Duration(s)':>12}  Error"
    logger.info(header)
    logger.info("-" * 100)
    for r in results:
        rows_in  = str(r["rows_in"])  if r["rows_in"]  is not None else "N/A"
        rows_out = str(r["rows_out"]) if r["rows_out"] is not None else "N/A"
        dur      = f"{r['duration_sec']:.3f}" if r["duration_sec"] is not None else "N/A"
        err      = r["error"] or ""
        logger.info("%-30s %-10s %8s %9s %12s  %s",
                    r["label"], r["status"] or "UNKNOWN", rows_in, rows_out, dur, err)

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
    log_banner(f"Loading Silver Layer  |  {batch_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    logger.info("=" * 60)
    logger.info("  Loading CRM Tables")
    logger.info("=" * 60)

    results = []
    for i, table in enumerate(TABLES):
        if i == 3:          # erp tables start at index 3
            logger.info("=" * 60)
            logger.info("  Loading ERP Tables")
            logger.info("=" * 60)
        results.append(load_table(**table))

    batch_end      = datetime.utcnow()
    batch_duration = (batch_end - batch_start).total_seconds()

    log_banner("Loading Silver Layer is Completed")
    logger.info("  Total Load Duration: %.3f seconds", batch_duration)

    print_summary(results, batch_duration)


if __name__ == "__main__":
    main()
