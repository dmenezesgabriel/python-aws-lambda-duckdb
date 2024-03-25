from unittest.mock import MagicMock

import boto3
import moto
import pandas as pd
from lambda_function import lambda_handler
from pytest import TempPathFactory


class TestLambdaHandler:

    @moto.mock_aws
    def test_lambda_handler_integration(
        self, context: MagicMock, tmp_path: TempPathFactory
    ) -> None:
        parquet_file_path = tmp_path / "test.parquet"  # type: ignore
        _df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 35]})
        _df.to_parquet(parquet_file_path)

        test_bucket = "test_bucket"
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=test_bucket)

        client.upload_file(
            str(parquet_file_path), test_bucket, "test_prefix/test.parquet"
        )

        event = {
            "datasets": [{"bucket": "test_bucket", "prefix": "test_prefix"}],
            "query": "SELECT * FROM test_prefix;",
        }

        response = lambda_handler(event, context)

        expected_response = {
            "result": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 35},
            ]
        }

        assert response == expected_response
