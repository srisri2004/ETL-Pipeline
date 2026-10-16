"""Business logic for the sales ETL/ELT pipelines (no Airflow imports -> unit-testable)."""
import os
import random
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DB_PATH = os.environ.get("WAREHOUSE_DB", "/opt/airflow/data/warehouse.db")
INPUT_DIR = os.environ.get("INPUT_DIR", "/opt/airflow/data/input")
WORK_DIR = os.environ.get("WORK_DIR", "/opt/airflow/data/work")
SQL_DIR = os.environ.get("SQL_DIR", "/opt/airflow/sql")
REJECT_THRESHOLD = float(os.environ.get("REJECT_THRESHOLD", "0.30"))  # fail run if >30% rows bad

REQUIRED_COLS = ["order_id", "customer_id", "product", "quantity",
                 "unit_price", "status", "order_date", "updated_at"]
VALID_STATUS = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED"}
TS_FMT = "%Y-%m-%d %H:%M:%S"


class DataQualityError(Exception):
    """Raised when a batch breaches data-quality thresholds."""


class SchemaError(Exception):
    """Raised when the source file does not have the expected columns."""


# ----------------------------------------------------------------- DB helpers
@contextmanager
def get_conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema():
    with get_conn() as c:
        c.executescript((Path(SQL_DIR) / "schema.sql").read_text())


def now_utc():
    return datetime.now(timezone.utc).strftime(TS_FMT)


# ---------------------------------------------------------------- audit log
def audit(pipeline, run_id, task, status, rows_in=None, rows_out=None,
          rows_rejected=None, message=None):
    with get_conn() as c:
        c.execute(
            "INSERT INTO audit_log(logged_at,pipeline,run_id,task,status,"
            "rows_in,rows_out,rows_rejected,message) VALUES (?,?,?,?,?,?,?,?,?)",
            (now_utc(), pipeline, run_id, task, status, rows_in, rows_out,
             rows_rejected, (message or "")[:1000]))


def get_watermark(conn, pipeline):
    row = conn.execute("SELECT last_value FROM etl_watermark WHERE pipeline=?",
                       (pipeline,)).fetchone()
    return row[0] if row else "1970-01-01 00:00:00"


def set_watermark(conn, pipeline, value):
    conn.execute(
        "INSERT INTO etl_watermark(pipeline,last_value,updated_at) VALUES (?,?,?) "
        "ON CONFLICT(pipeline) DO UPDATE SET last_value=excluded.last_value,"
        "updated_at=excluded.updated_at", (pipeline, value, now_utc()))


# -------------------------------------------------------- source simulation
def generate_source_file(start, end, out_dir=None, seed=None):
    """Stand-in for an upstream system: writes orders changed in [start, end).
    ~7% of rows are deliberately bad so validation has something to catch."""
    out_dir = out_dir or INPUT_DIR
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    rnd = random.Random(seed if seed is not None else start.timestamp())
    span = max(int((end - start).total_seconds()), 1)
    rows = []
    for _ in range(120):
        upd = datetime.fromtimestamp(start.timestamp() + rnd.randrange(span))
        rows.append({
            "order_id": rnd.randint(1, 400),          # overlaps -> updates (upsert)
            "customer_id": f"C{rnd.randint(1, 60):03d}",
            "product": rnd.choice(["Laptop", "Phone", "Monitor", "Keyboard", "Mouse"]),
            "quantity": rnd.randint(1, 5),
            "unit_price": round(rnd.uniform(10, 1500), 2),
            "status": rnd.choice(sorted(VALID_STATUS)),
            "order_date": upd.strftime("%Y-%m-%d"),
            "updated_at": upd.strftime(TS_FMT)})
    for r in rnd.sample(rows, 9):                       # inject bad data
        kind = rnd.choice(["cust", "qty", "price", "date", "status", "id"])
        if kind == "cust": r["customer_id"] = ""
        elif kind == "qty": r["quantity"] = -2
        elif kind == "price": r["unit_price"] = "abc"
        elif kind == "date": r["order_date"] = "31-31-2026"
        elif kind == "status": r["status"] = "UNKNOWN"
        else: r["order_id"] = ""
    path = Path(out_dir) / f"sales_{start.strftime('%Y%m%dT%H%M%S')}.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return str(path)


# ------------------------------------------------------------- ETL: validate
def validate_file(path, work_dir=None):
    """Schema + row-level checks. Bad rows are quarantined (never silently dropped)."""
    work_dir = work_dir or WORK_DIR
    Path(work_dir).mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = set(REQUIRED_COLS) - set(df.columns)
    if missing:
        raise SchemaError(f"Missing columns: {sorted(missing)}")
    n_in = len(df)
    if n_in == 0:
        raise DataQualityError("Source file is empty")
    reasons = pd.Series("", index=df.index)

    def flag(mask, msg):
        reasons[mask] = reasons[mask] + msg + "; "

    flag(df.order_id.str.strip() == "", "order_id missing")
    flag(df.customer_id.str.strip() == "", "customer_id missing")
    qty = pd.to_numeric(df.quantity, errors="coerce")
    flag(qty.isna() | (qty <= 0) | (qty % 1 != 0), "invalid quantity")
    price = pd.to_numeric(df.unit_price, errors="coerce")
    flag(price.isna() | (price < 0), "invalid unit_price")
    odate = pd.to_datetime(df.order_date, format="%Y-%m-%d", errors="coerce")
    flag(odate.isna(), "invalid order_date")
    upd = pd.to_datetime(df.updated_at, errors="coerce")
    flag(upd.isna(), "invalid updated_at")
    flag(~df.status.str.upper().isin(VALID_STATUS), "invalid status")

    bad = reasons != ""
    stem = Path(path).stem
    rejected_path = str(Path(work_dir) / f"{stem}_rejected.csv")
    clean_path = str(Path(work_dir) / f"{stem}_clean.csv")
    df[bad].assign(reject_reason=reasons[bad]).to_csv(rejected_path, index=False)

    clean = df[~bad].copy()
    clean["order_id"] = clean.order_id.astype(int)
    clean["quantity"] = qty[~bad].astype(int)
    clean["unit_price"] = price[~bad].astype(float)
    clean["status"] = clean.status.str.upper()
    clean["updated_at"] = upd[~bad].dt.strftime(TS_FMT)
    before = len(clean)
    clean = (clean.sort_values("updated_at")
                  .drop_duplicates("order_id", keep="last"))  # latest version wins
    dups = before - len(clean)
    clean.to_csv(clean_path, index=False)

    ratio = bad.sum() / n_in
    stats = {"rows_in": n_in, "rows_clean": len(clean), "rows_rejected": int(bad.sum()),
             "duplicates_dropped": dups, "clean_path": clean_path,
             "rejected_path": rejected_path}
    if ratio > REJECT_THRESHOLD:
        raise DataQualityError(
            f"Reject ratio {ratio:.0%} exceeds threshold {REJECT_THRESHOLD:.0%}")
    return stats


# ---------------------------------------------------- ETL: incremental load
def incremental_load(clean_path, pipeline="sales_etl"):
    """Load only rows newer than the stored watermark, upsert, advance watermark
    in the SAME transaction (atomic + idempotent: re-running a task is safe)."""
    df = pd.read_csv(clean_path)
    with get_conn() as c:
        wm = get_watermark(c, pipeline)
        new = df[df.updated_at > wm]
        if new.empty:
            return {"rows_loaded": 0, "inserted": 0, "updated": 0,
                    "skipped": len(df), "watermark": wm}
        ids = [int(i) for i in new.order_id]
        existing = {r[0] for r in c.execute(
            f"SELECT order_id FROM sales_clean WHERE order_id IN "
            f"({','.join('?' * len(ids))})", ids)}
        c.executemany(
            "INSERT INTO sales_clean(order_id,customer_id,product,quantity,unit_price,"
            "status,order_date,updated_at,loaded_at) VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(order_id) DO UPDATE SET customer_id=excluded.customer_id,"
            "product=excluded.product,quantity=excluded.quantity,"
            "unit_price=excluded.unit_price,status=excluded.status,"
            "order_date=excluded.order_date,updated_at=excluded.updated_at,"
            "loaded_at=excluded.loaded_at WHERE excluded.updated_at > sales_clean.updated_at",
            [(int(r.order_id), r.customer_id, r.product, int(r.quantity),
              float(r.unit_price), r.status, r.order_date, r.updated_at, now_utc())
             for r in new.itertuples()])
        new_wm = new.updated_at.max()
        set_watermark(c, pipeline, new_wm)
    return {"rows_loaded": len(new), "inserted": len(new) - len(existing),
            "updated": len(existing), "skipped": len(df) - len(new), "watermark": new_wm}


# ------------------------------------------------------------------- ELT
def land_raw(path, batch_id):
    """E+L: load file as-is (all text) into staging; idempotent per file."""
    with get_conn() as c:
        if c.execute("SELECT 1 FROM stg_loaded_files WHERE file_name=?",
                     (Path(path).name,)).fetchone():
            return 0
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df["batch_id"] = batch_id
        df["source_file"] = Path(path).name
        df["loaded_at"] = now_utc()
        df.to_sql("stg_sales_raw", c, if_exists="append", index=False)
        c.execute("INSERT INTO stg_loaded_files VALUES (?,?,?)",
                  (Path(path).name, len(df), now_utc()))
        return len(df)


def run_sql_transform():
    """T: transformation executed inside the database (ELT)."""
    sql = (Path(SQL_DIR) / "transform_daily_summary.sql").read_text()
    with get_conn() as c:
        c.executescript(sql)
        return c.execute("SELECT COUNT(*) FROM sales_daily_summary").fetchone()[0]


def dq_checks():
    with get_conn() as c:
        n = c.execute("SELECT COUNT(*) FROM sales_daily_summary").fetchone()[0]
        neg = c.execute("SELECT COUNT(*) FROM sales_daily_summary "
                        "WHERE revenue < 0 OR orders <= 0").fetchone()[0]
    if n == 0:
        raise DataQualityError("sales_daily_summary is empty")
    if neg:
        raise DataQualityError(f"{neg} summary rows have invalid values")
    return n
