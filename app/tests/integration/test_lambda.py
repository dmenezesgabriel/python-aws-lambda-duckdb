import io

import boto3
import moto
import pandas as pd
from lambda_function import lambda_handler

payload = {
    "datasets": '[{"bucket": "test-bucket", "prefix": "test-prefix"}]',
    "query": "SELECT * FROM test;",
}


class TestLambdaHandler:

    @moto.mock_aws
    def test_lambda_handler_integration(self, context, tmp_path):
        parquet_file_path = tmp_path / "test.parquet"
        _df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 35]})
        _df.to_parquet(parquet_file_path)

        test_bucket = "test_bucket"
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=test_bucket)

        client.upload_file(
            str(parquet_file_path), test_bucket, "test_prefix/test.parquet"
        )

        obj = client.get_object(
            Bucket=test_bucket, Key="test_prefix/test.parquet"
        )

        data = obj["Body"].read()
        file_like_object = io.BytesIO(data)
        df = pd.read_parquet(file_like_object)
        print(df)

        event = {
            "datasets": '[{"bucket": "test_bucket", "prefix": "test_prefix"}]',
            "query": "SELECT * FROM test_prefix;",
        }

        response = lambda_handler(event, context)

        expected_response = {
            "result": '{"name": ["Alice", "Bob"], "age": [30, 35]}'
        }

        assert response == expected_response
