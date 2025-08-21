import os


class GlobalParams(object):
    def __init__(self):
        self.region = "us-east-1"
        self.ENV = os.getenv("AIRFLOW__DP__ENVIRONMENT", "undefined")
        self.HOST = os.getenv("AIRFLOW__DP__HOST", "localhost")

        self.TMP_PROFILING = "/tmp/data_profiling_report"

        self.AWS_S3_BUCKET_RAW = os.getenv("AIRFLOW__DP__AWS_S3_BUCKET_RAW", "")
        self.AWS_S3_PROFILING_FOLDER = os.getenv("AIRFLOW__DP__AWS_S3_PROFILING_FOLDER", "")

        self.DBT_CONN_ID = "sf_conn"
        self.DBT_DIR = "/usr/local/airflow/dags/dbt/"
        self.DBT_VENV = "/usr/local/airflow/dbt_venv/bin/dbt"
        self.DBT_PROJECT = "novicap"
        self.SCH_DBT = "dbt"
