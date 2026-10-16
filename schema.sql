CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT, logged_at TEXT, pipeline TEXT, run_id TEXT,
  task TEXT, status TEXT, rows_in INTEGER, rows_out INTEGER, rows_rejected INTEGER, message TEXT);
CREATE TABLE IF NOT EXISTS etl_watermark (
  pipeline TEXT PRIMARY KEY, last_value TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS sales_clean (
  order_id INTEGER PRIMARY KEY, customer_id TEXT, product TEXT, quantity INTEGER,
  unit_price REAL, status TEXT, order_date TEXT, updated_at TEXT, loaded_at TEXT);
CREATE TABLE IF NOT EXISTS stg_sales_raw (
  order_id TEXT, customer_id TEXT, product TEXT, quantity TEXT, unit_price TEXT,
  status TEXT, order_date TEXT, updated_at TEXT, batch_id TEXT, source_file TEXT, loaded_at TEXT);
CREATE TABLE IF NOT EXISTS stg_loaded_files (
  file_name TEXT PRIMARY KEY, row_count INTEGER, loaded_at TEXT);
CREATE TABLE IF NOT EXISTS sales_daily_summary (
  order_date TEXT PRIMARY KEY, orders INTEGER, units INTEGER, revenue REAL);
