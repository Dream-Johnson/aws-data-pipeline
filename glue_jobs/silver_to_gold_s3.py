import boto3
import awswrangler as wr
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
GOLD_BUCKET   = "de-project-gold-hyd-16"
GLUE_DB       = "gold"

# Shared boto3 session — passed to every awswrangler call
session = boto3.Session(region_name=REGION)

# ---------------------------------------------------------------------------
# S3 read helper (boto3 + pandas — keeps Silver reads explicit)
# ---------------------------------------------------------------------------
_s3 = boto3.client("s3", region_name=REGION)


def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    obj = _s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def log_banner(msg: str, char: str = "=") -> None:
    line = char * 60
    logger.info(line)
    logger.info("  %s", msg)
    logger.info(line)


# ---------------------------------------------------------------------------
# Step 1 — Read all Silver source files
# ---------------------------------------------------------------------------
def read_silver() -> dict[str, pd.DataFrame]:
    sources = {
        "cust_info":     ("source_crm/cust_info.parquet",     SILVER_BUCKET),
        "prd_info":      ("source_crm/prd_info.parquet",      SILVER_BUCKET),
        "sales_details": ("source_crm/sales_details.parquet", SILVER_BUCKET),
        "cust_az12":     ("source_erp/CUST_AZ12.parquet",     SILVER_BUCKET),
        "loc_a101":      ("source_erp/LOC_A101.parquet",      SILVER_BUCKET),
        "px_cat_g1v2":   ("source_erp/PX_CAT_G1V2.parquet",   SILVER_BUCKET),
    }
    frames = {}
    for name, (key, bucket) in sources.items():
        logger.info("  Reading s3://%s/%s", bucket, key)
        df = read_parquet(bucket, key)
        df.columns = df.columns.str.lower()
        frames[name] = df
        logger.info("         → %d rows", len(df))
    return frames


# ---------------------------------------------------------------------------
# Step 2 — Transformation functions (SQL → pandas)
# ---------------------------------------------------------------------------

def build_dim_customers(
    cust_info: pd.DataFrame,
    cust_az12: pd.DataFrame,
    loc_a101:  pd.DataFrame,
) -> pd.DataFrame:
    """
    SQL equivalent:
        SELECT ROW_NUMBER() OVER (ORDER BY cst_id)          AS customer_key,
               ci.cst_id, ci.cst_key, ci.cst_firstname, ci.cst_lastname,
               la.cntry,
               ci.cst_marital_status,
               CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
                    ELSE COALESCE(ca.gen, 'n/a') END        AS gender,
               ca.bdate, ci.cst_create_date
        FROM   crm_cust_info ci
        LEFT JOIN erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN erp_loc_a101  la ON ci.cst_key = la.cid
    """
    # LEFT JOIN erp_cust_az12  (cst_key = cid)
    df = cust_info.merge(
        cust_az12[["cid", "bdate", "gen"]],
        left_on="cst_key", right_on="cid",
        how="left",
    ).drop(columns=["cid"])

    # LEFT JOIN erp_loc_a101  (cst_key = cid)
    df = df.merge(
        loc_a101[["cid", "cntry"]],
        left_on="cst_key", right_on="cid",
        how="left",
    ).drop(columns=["cid"])

    # CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr ELSE COALESCE(ca.gen, 'n/a')
    df["gender"] = df["cst_gndr"].where(
        df["cst_gndr"] != "n/a",
        df["gen"].fillna("n/a"),
    )

    # Surrogate key: ROW_NUMBER() OVER (ORDER BY cst_id)
    df = df.sort_values("cst_id").reset_index(drop=True)
    df.insert(0, "customer_key", range(1, len(df) + 1))

    return df[[
        "customer_key", "cst_id", "cst_key", "cst_firstname", "cst_lastname",
        "cntry", "cst_marital_status", "gender", "bdate", "cst_create_date",
    ]].rename(columns={
        "cst_id":             "customer_id",
        "cst_key":            "customer_number",
        "cst_firstname":      "first_name",
        "cst_lastname":       "last_name",
        "cntry":              "country",
        "cst_marital_status": "marital_status",
        "bdate":              "birthdate",
        "cst_create_date":    "create_date",
    })


def build_dim_products(
    prd_info:   pd.DataFrame,
    px_cat_g1v2: pd.DataFrame,
) -> pd.DataFrame:
    """
    SQL equivalent:
        SELECT ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
               pn.prd_id, pn.prd_key, pn.prd_nm,
               pn.cat_id, pc.cat, pc.subcat, pc.maintenance,
               pn.prd_cost, pn.prd_line, pn.prd_start_dt
        FROM   crm_prd_info      pn
        LEFT JOIN erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
        WHERE  pn.prd_end_dt IS NULL   -- current / active products only
    """
    # WHERE prd_end_dt IS NULL  — filter out historical rows
    df = prd_info[prd_info["prd_end_dt"].isna()].copy()

    # LEFT JOIN erp_px_cat_g1v2  (cat_id = id)
    df = df.merge(
        px_cat_g1v2[["id", "cat", "subcat", "maintenance"]],
        left_on="cat_id", right_on="id",
        how="left",
    ).drop(columns=["id"])

    # Surrogate key: ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key)
    df = df.sort_values(["prd_start_dt", "prd_key"]).reset_index(drop=True)
    df.insert(0, "product_key", range(1, len(df) + 1))

    return df[[
        "product_key", "prd_id", "prd_key", "prd_nm",
        "cat_id", "cat", "subcat", "maintenance",
        "prd_cost", "prd_line", "prd_start_dt",
    ]].rename(columns={
        "prd_id":      "product_id",
        "prd_key":     "product_number",
        "prd_nm":      "product_name",
        "cat_id":      "category_id",
        "cat":         "category",
        "subcat":      "subcategory",
        "prd_cost":    "cost",
        "prd_line":    "product_line",
        "prd_start_dt": "start_date",
    })


def build_fact_sales(
    sales:         pd.DataFrame,
    dim_products:  pd.DataFrame,
    dim_customers: pd.DataFrame,
) -> pd.DataFrame:
    """
    SQL equivalent:
        SELECT sd.sls_ord_num,
               pr.product_key, cu.customer_key,
               sd.sls_order_dt, sd.sls_ship_dt, sd.sls_due_dt,
               sd.sls_sales, sd.sls_quantity, sd.sls_price
        FROM   crm_sales_details sd
        LEFT JOIN dim_products  pr ON sd.sls_prd_key = pr.product_number
        LEFT JOIN dim_customers cu ON sd.sls_cust_id  = cu.customer_id
    """
    # LEFT JOIN dim_products  (sls_prd_key = product_number)
    df = sales.merge(
        dim_products[["product_key", "product_number"]],
        left_on="sls_prd_key", right_on="product_number",
        how="left",
    ).drop(columns=["product_number"])

    # LEFT JOIN dim_customers  (sls_cust_id = customer_id)
    df = df.merge(
        dim_customers[["customer_key", "customer_id"]],
        left_on="sls_cust_id", right_on="customer_id",
        how="left",
    ).drop(columns=["customer_id"])

    return df[[
        "sls_ord_num", "product_key", "customer_key",
        "sls_order_dt", "sls_ship_dt", "sls_due_dt",
        "sls_sales", "sls_quantity", "sls_price",
    ]].rename(columns={
        "sls_ord_num":  "order_number",
        "sls_order_dt": "order_date",
        "sls_ship_dt":  "shipping_date",
        "sls_due_dt":   "due_date",
        "sls_sales":    "sales_amount",
        "sls_quantity": "quantity",
        "sls_price":    "price",
    })


# ---------------------------------------------------------------------------
# Step 3 — Write to Gold S3 + register in Glue Data Catalog
# ---------------------------------------------------------------------------
def write_gold_table(df: pd.DataFrame, table_name: str) -> dict:
    """
    Writes df to s3://GOLD_BUCKET/<table_name>/ as Parquet and registers
    (or updates) the table in the Glue Data Catalog so Athena can query it.
    awswrangler handles both steps atomically via dataset=True.
    """
    result = {
        "table":        f"{GLUE_DB}.{table_name}",
        "s3_path":      f"s3://{GOLD_BUCKET}/{table_name}/",
        "status":       None,
        "rows_written": None,
        "start_time":   None,
        "end_time":     None,
        "duration_sec": None,
        "error":        None,
    }

    logger.info("-" * 60)
    logger.info(">> Writing   : %s.%s", GLUE_DB, table_name)
    logger.info(">> S3 path   : %s", result["s3_path"])

    try:
        start = datetime.utcnow()
        result["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")

        wr.s3.to_parquet(
            df=df,
            path=result["s3_path"],
            dataset=True,          # treat path as a dataset partition root
            database=GLUE_DB,      # Glue database to register the table in
            table=table_name,      # Glue table name (queryable from Athena)
            mode="overwrite",      # drop + rewrite — mirrors SQL TRUNCATE + INSERT
            boto3_session=session,
        )

        end = datetime.utcnow()
        duration = (end - start).total_seconds()

        result["status"]       = "SUCCESS"
        result["rows_written"] = len(df)
        result["end_time"]     = end.strftime("%Y-%m-%d %H:%M:%S")
        result["duration_sec"] = round(duration, 3)

        logger.info(">> Rows      : %d", len(df))
        logger.info(">> Catalog   : registered in Glue DB '%s'", GLUE_DB)
        logger.info(">> Duration  : %.3f seconds", duration)

    except Exception as exc:
        end = datetime.utcnow()
        result["status"]   = "FAILED"
        result["end_time"] = end.strftime("%Y-%m-%d %H:%M:%S")
        if result["start_time"]:
            result["duration_sec"] = round(
                (end - datetime.strptime(result["start_time"], "%Y-%m-%d %H:%M:%S")).total_seconds(), 3
            )
        result["error"] = str(exc)
        logger.error("ERROR writing %s.%s : %s", GLUE_DB, table_name, exc)

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
        rows = str(r["rows_written"]) if r["rows_written"] is not None else "N/A"
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
        raise RuntimeError(f"{failed} Gold table(s) failed. See log above for details.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    batch_start = datetime.utcnow()
    log_banner(f"Building Gold Layer  |  {batch_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("  Silver : s3://%s", SILVER_BUCKET)
    logger.info("  Gold   : s3://%s", GOLD_BUCKET)
    logger.info("  Catalog: Glue DB '%s'", GLUE_DB)

    # Ensure Glue database exists before writing any table
    wr.catalog.create_database(name=GLUE_DB, exist_ok=True, boto3_session=session)
    logger.info("  Glue database '%s' is ready.", GLUE_DB)

    # ── Read ──────────────────────────────────────────────────────────────
    log_banner("Reading Silver Layer", char="-")
    silver = read_silver()

    results = []

    # ── dim_customers ─────────────────────────────────────────────────────
    log_banner("Building dim_customers", char="-")
    dim_customers = build_dim_customers(
        silver["cust_info"],
        silver["cust_az12"],
        silver["loc_a101"],
    )
    results.append(write_gold_table(dim_customers, "dim_customers"))

    # ── dim_products ──────────────────────────────────────────────────────
    log_banner("Building dim_products", char="-")
    dim_products = build_dim_products(
        silver["prd_info"],
        silver["px_cat_g1v2"],
    )
    results.append(write_gold_table(dim_products, "dim_products"))

    # ── fact_sales  (depends on both dims for surrogate key lookup) ───────
    log_banner("Building fact_sales", char="-")
    fact_sales = build_fact_sales(
        silver["sales_details"],
        dim_products,
        dim_customers,
    )
    results.append(write_gold_table(fact_sales, "fact_sales"))

    # ── Summary ───────────────────────────────────────────────────────────
    batch_end      = datetime.utcnow()
    batch_duration = (batch_end - batch_start).total_seconds()

    log_banner("Gold Layer Build Completed")
    logger.info("  Total Duration: %.3f seconds", batch_duration)

    print_summary(results, batch_duration)


if __name__ == "__main__":
    main()
