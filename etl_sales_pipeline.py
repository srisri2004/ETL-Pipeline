"""ETL: extract -> validate -> incremental load -> audit, then trigger the ELT DAG."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

import etl_lib as lib

PIPELINE = "etl_sales_pipeline"


def on_failure_callback(context):
    """Runs after retries are exhausted: persist the failure to the audit log."""
    ti = context["task_instance"]
    lib.init_schema()
    lib.audit(PIPELINE, context["run_id"], ti.task_id, "FAILED",
              message=f"{type(context.get('exception')).__name__}: {context.get('exception')}")


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,           # 30s, 60s, 120s ...
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_callback,
}


@dag(dag_id=PIPELINE, description="Incremental sales ETL with validation and audit",
     start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False,
     max_active_runs=1, default_args=default_args,
     tags=["etl", "sales", "incremental"])
def etl_sales_pipeline():

    @task
    def init_db():
        lib.init_schema()

    @task
    def extract(data_interval_start=None, data_interval_end=None, run_id=None):
        path = lib.generate_source_file(data_interval_start.replace(tzinfo=None),
                                        data_interval_end.replace(tzinfo=None))
        lib.audit(PIPELINE, run_id, "extract", "SUCCESS", message=path)
        return path

    @task
    def validate(path, run_id=None):
        stats = lib.validate_file(path)
        lib.audit(PIPELINE, run_id, "validate", "SUCCESS", stats["rows_in"],
                  stats["rows_clean"], stats["rows_rejected"],
                  f"duplicates_dropped={stats['duplicates_dropped']}; "
                  f"quarantine={stats['rejected_path']}")
        return stats

    @task
    def load(stats, run_id=None):
        res = lib.incremental_load(stats["clean_path"])
        lib.audit(PIPELINE, run_id, "load", "SUCCESS", stats["rows_clean"],
                  res["rows_loaded"], None,
                  f"inserted={res['inserted']} updated={res['updated']} "
                  f"skipped_old={res['skipped']} watermark={res['watermark']}")
        return res

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize(run_id=None, **context):
        """Always runs: one run-level summary row (SUCCESS / FAILED) for the audit trail."""
        tis = context["dag_run"].get_task_instances()
        failed = [t.task_id for t in tis
                  if t.task_id != "finalize" and t.state in ("failed", "upstream_failed")]
        lib.audit(PIPELINE, run_id, "run_summary", "FAILED" if failed else "SUCCESS",
                  message=f"failed_tasks={failed}" if failed else "all tasks succeeded")

    trigger_elt = TriggerDagRunOperator(
        task_id="trigger_elt", trigger_dag_id="elt_sales_pipeline",
        wait_for_completion=False)

    db = init_db()
    src = extract()
    db >> src
    loaded = load(validate(src))
    loaded >> trigger_elt
    [loaded, trigger_elt] >> finalize()


etl_sales_pipeline()
