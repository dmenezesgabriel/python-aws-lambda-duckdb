import io
from typing import Any, Dict, Hashable, List

import boto3
import duckdb
import pandas as pd
from typing_extensions import TypedDict


class Datasets(TypedDict):
    bucket: str
    prefix: str


def s3_parquet_to_df(client: Any, bucket: str, key: str) -> pd.DataFrame:
    try:
        object = client.get_object(Bucket=bucket, Key=key)
        data = object["Body"].read()
        file_like_object = io.BytesIO(data)
        df = pd.read_parquet(file_like_object)
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


def s3_partitioned_parquet_to_df(
    client: Any, bucket: str, prefix: str
) -> pd.DataFrame:
    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        dfs = []
        for page in pages:
            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith(".parquet"):
                    df = s3_parquet_to_df(client, bucket, key)
                    dfs.append(df)
        return pd.concat(dfs)
    except Exception as e:
        print(e)
        return pd.DataFrame()


def run_query(
    datasets: List[Datasets], query: str
) -> List[Dict[Hashable, Any]]:
    try:
        client = boto3.client("s3")
        conn = duckdb.connect(":memory:")

        # account = boto3.client("sts").get_caller_identity()["Account"]
        # region = boto3.session.Session().region_name

        for dataset in datasets:
            bucket = dataset["bucket"]
            prefix = dataset["prefix"]
            df = s3_partitioned_parquet_to_df(client, bucket, prefix)
            conn.register(prefix, df)

        result = conn.sql(query).df()
        return result.to_dict(orient="records")
    except Exception as e:
        print(e)
        return []
