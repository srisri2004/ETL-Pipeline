"""ELT: land raw files in staging, transform with SQL inside the warehouse, run DQ checks."""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

import etl_lib as lib

PIPELINE = "elt_sales_pipeline"


def on_failure_callback(context):
    lib.init_schema()
    lib.audit(PIPELINE, context["run_id"], context["task_instance"].task_id, "FAILED",
              message=str(context.get("exception")))


@dag(dag_id=PIPELINE, start_date=datetime(2026, 1, 1), schedule=None, catchup=False,
     max_active_runs=1, tags=["elt", "sales"],
     default_args={"owner": "data-engineering", "retries": 2,
                   "retry_delay": timedelta(seconds=30),
                   "on_failure_callback": on_failure_callback})
def elt_sales_pipeline():

    @task
    def land_raw(run_id=None):
        lib.init_schema()
        total = 0
        for f in sorted(Path(lib.INPUT_DIR).glob("sales_*.csv")):
            total += lib.land_raw(str(f), run_id)       # skips files already landed
        lib.audit(PIPELINE, run_id, "land_raw", "SUCCESS", rows_out=total)
        return total

    @task
    def transform_in_db(_landed, run_id=None):
        n = lib.run_sql_transform()
        lib.audit(PIPELINE, run_id, "transform_in_db", "SUCCESS", rows_out=n)

    @task
    def quality_checks(_t, run_id=None):
        n = lib.dq_checks()
        lib.audit(PIPELINE, run_id, "quality_checks", "SUCCESS", rows_out=n)

    quality_checks(transform_in_db(land_raw()))


elt_sales_pipeline()
