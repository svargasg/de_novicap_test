import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from cosmos import DbtTaskGroup, LoadMode, RenderConfig
from cosmos.constants import TestBehavior
# from include.common.params import GlobalParams
from include.utils.dbt import dbt_execution_config, dbt_project_config, sf_db

# params = GlobalParams()

# -------- dag definition ------------------------
DAG_NAME = os.path.basename(__file__).replace(".py", "")

default_args = {
    "owner": "DE",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
    # "on_failure_callback": get_slack_conf_failure(),
}


@dag(
    dag_id=DAG_NAME,
    description="dag that runs dbt pipeline",
    tags=["DataTransformation", "DE", "dbt"],
    start_date=datetime(2025, 1, 1, 1, 0),
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    # on_failure_callback=get_slack_conf_failure(),
)
def dbt_task_group() -> None:
    start = DummyOperator(task_id="start", priority_weight=5)

    end = DummyOperator(task_id="end", trigger_rule="all_success", priority_weight=5)

    task_group_transform = DbtTaskGroup(
        group_id="task_group_transform",
        project_config=dbt_project_config,
        profile_config=sf_db,
        execution_config=dbt_execution_config,
        render_config=RenderConfig(
            select=["stg_bank_loans"],
            load_method=LoadMode.CUSTOM,
            emit_datasets=False,
            test_behavior=TestBehavior.BUILD,
        ),
    )

    start >> task_group_transform >> end


dbt_task_group()
