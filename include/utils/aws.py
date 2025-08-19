import json
import logging
from io import StringIO

import boto3
import pandas as pd
from airflow.hooks.base import BaseHook

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def aws_session(connection):
    conn = BaseHook.get_connection(connection)
    my_session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=json.loads(conn.extra)["region_name"],
    )
    return my_session


def s3_read_csv(conn, bucket_name, key, aws_region):
    try:
        s3_client = aws_session(conn).client("s3", region_name=aws_region)

        logger.info(f"Reading file '{key}' from bucket '{bucket_name}'...")

        response = s3_client.get_object(Bucket=bucket_name, Key=key)

        csv_content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_content), sep=",", header=0, encoding="utf-8")

        logger.info("File was read successfully")
        return df
    except Exception as e:
        logger.error(f"s3 read failed: {e}")
        raise


def s3_upload_file(conn, file_name, bucket_name, key, aws_region, extra=None):
    try:
        s3_client = aws_session(conn).client("s3", region_name=aws_region)
        s3_client.upload_file(Filename=file_name, Bucket=bucket_name, Key=key, ExtraArgs=extra)
        logger.info(f"Upload object to s3://{bucket_name}/{key} was successfully")
    except Exception as e:
        logger.error(f"s3 upload failed: {e}")
        raise
