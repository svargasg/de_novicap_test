import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from include.common.params import GlobalParams
from include.utils.aws import s3_read_csv, s3_upload_file
from include.utils.utils import generate_profile_report

def profile_data(s3_key):
    _params = GlobalParams()
    aws_conn_id = "aws_conn"
    df = s3_read_csv(aws_conn_id, _params.AWS_S3_BUCKET_RAW, s3_key, _params.region)
    tmp_path = generate_profile_report(df)
    s3_report_key = _params.AWS_S3_PROFILING_FOLDER + tmp_path.split("/")[-1]
    s3_upload_file(aws_conn_id, tmp_path, _params.AWS_S3_BUCKET_RAW, s3_report_key, _params.region, {'ContentType': 'text/html'})


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
    description="dag that generates and upload data profiling report to s3",
    tags=["DataProfiling", "DE", "S3"],
    start_date=datetime(2025, 1, 1, 1, 0),
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    # on_failure_callback=get_slack_conf_failure(),
)
def data_profiling_report() -> None:
    start = DummyOperator(task_id="start")

    end = DummyOperator(task_id="end", trigger_rule="all_success")

    profile_report = PythonOperator(
        task_id="data_profiling_report",
        python_callable=profile_data,
        provide_context=True,
        op_kwargs={
            "s3_key": '{{ dag_run.conf.get("s3_key", "") }}',
        },
    )

    start >> profile_report >> end


data_profiling_report()
