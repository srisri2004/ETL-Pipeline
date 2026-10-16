# Automated ETL & ELT Pipelines with Apache Airflow

Production-style data pipelines for sales orders, built with **Apache Airflow 2.9**, **Python/pandas** and **SQL**.

## Features
| Requirement | Implementation |
|---|---|
| Data validation | Schema check, row-level rules, quarantine of bad rows with reject reason, de-duplication, reject-ratio threshold (default 30%) |
| Incremental loading | Watermark table + `INSERT ... ON CONFLICT DO UPDATE`; watermark advances in the same transaction (idempotent re-runs) |
| Error handling | 3 retries with exponential back-off, execution timeouts, custom exceptions, transactional rollback, `on_failure_callback` |
| Audit logging | `audit_log` table (task, status, rows in/out/rejected, message) + run summary via `trigger_rule=ALL_DONE` |
| Orchestration | Scheduled ETL DAG triggers ELT DAG (`TriggerDagRunOperator`), TaskFlow API, `max_active_runs=1` |

## Architecture
```
etl_sales_pipeline (@daily)
  init_db > extract > validate > load (incremental) > trigger_elt
                                     \______________________/ > finalize (audit summary, always runs)

elt_sales_pipeline (triggered)
  land_raw (stg_sales_raw) > transform_in_db (SQL) > quality_checks
```

## Run locally (Docker Desktop required)
```bash
docker compose up
```
1. Open http://localhost:8080 — user `admin`, password is printed in the container log.
2. Un-pause `etl_sales_pipeline` and `elt_sales_pipeline`, then trigger `etl_sales_pipeline`.

## Inspect results
```bash
docker compose exec airflow python -c "import sqlite3;c=sqlite3.connect('/opt/airflow/data/warehouse.db');[print(r) for r in c.execute('select logged_at,task,status,rows_in,rows_out,rows_rejected,message from audit_log order by id')]"
```
Tables: `audit_log`, `etl_watermark`, `sales_clean`, `stg_sales_raw`, `sales_daily_summary`.
Rejected rows: `data/work/*_rejected.csv`.

## Tests
```bash
pip install -r requirements-dev.txt
pytest -q tests
```

## Project structure
```
dags/etl_sales_pipeline.py   ETL DAG
dags/elt_sales_pipeline.py   ELT DAG
dags/etl_lib.py              validation, incremental load, ELT helpers (Airflow-free, unit-tested)
sql/schema.sql               warehouse tables
sql/transform_daily_summary.sql   in-database transformation
tests/test_pipeline.py       unit tests
docker-compose.yml           local Airflow
```

## Notes
The source system is simulated (a CSV generator with deliberately dirty rows) and the warehouse is SQLite for zero-setup.
To use real systems, replace `generate_source_file` with an API/S3/DB extract and change the connection in `get_conn`.
