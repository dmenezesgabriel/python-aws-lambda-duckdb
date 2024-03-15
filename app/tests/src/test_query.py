from typing import List
from unittest.mock import MagicMock, patch

import pandas as pd
from src.query import (
    Datasets,
    run_query,
    s3_parquet_to_df,
    s3_partitioned_parquet_to_df,
)


class TestQueryS3ParquetToDf:
    @patch("boto3.client")
    @patch("io.BytesIO")
    @patch("pandas.read_parquet")
    def test_should_call_client_get_object(
        self,
        mock_read_parquet: MagicMock,
        mock_io_BytesIO: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        key = "test-key"
        mock_read_parquet.return_value = MagicMock()

        # Act
        s3_parquet_to_df(mock_boto3_client, bucket, key)

        # Assert
        mock_boto3_client.get_object.assert_called_once_with(
            Bucket=bucket, Key=key
        )

    @patch("boto3.client")
    @patch("io.BytesIO")
    @patch("pandas.read_parquet")
    def test_should_call_io_bytes(
        self,
        mock_read_parquet: MagicMock,
        mock_io_BytesIO: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        key = "test-key"
        mock_read_parquet.return_value = MagicMock()

        # Act
        s3_parquet_to_df(mock_boto3_client, bucket, key)

        # Assert
        assert mock_io_BytesIO.call_count == 1

    @patch("boto3.client")
    @patch("io.BytesIO")
    @patch("pandas.read_parquet")
    def test_should_call_read_parquet(
        self,
        mock_read_parquet: MagicMock,
        mock_io_BytesIO: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        key = "test-key"
        mock_read_parquet.return_value = MagicMock()

        # Act
        s3_parquet_to_df(mock_boto3_client, bucket, key)

        # Assert
        assert mock_read_parquet.call_count == 1

    @patch("boto3.client")
    @patch("io.BytesIO")
    @patch("pandas.read_parquet")
    def test_should_return_dataframe(
        self,
        mock_read_parquet: MagicMock,
        mock_io_BytesIO: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        key = "test-key"
        mock_read_parquet.return_value = MagicMock(spec=pd.DataFrame)

        # Act
        result_df = s3_parquet_to_df(mock_boto3_client, bucket, key)

        # Assert
        assert isinstance(result_df, pd.DataFrame)

    @patch("boto3.client")
    @patch("io.BytesIO")
    @patch("pandas.read_parquet")
    def test_should_return_empty_dataframe(
        self,
        mock_read_parquet: MagicMock,
        mock_io_BytesIO: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        key = "test-key"
        mock_boto3_client.return_value.get_object.side_effect = Exception(
            "Test exception"
        )

        # Act
        result_df = s3_parquet_to_df(mock_boto3_client, bucket, key)

        # Assert
        assert result_df.empty


class TestQueryS3PartitionedParquetToDf:

    @patch("boto3.client")
    @patch("pandas.concat")
    @patch("src.query.s3_parquet_to_df")
    def test_should_call_client_get_paginator(
        self,
        mock_s3_parquet_to_df: MagicMock,
        mock_pandas_concat: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        prefix = "test-prefix"
        mock_s3_parquet_to_df.return_value = MagicMock(spec=pd.DataFrame)

        # Act
        s3_partitioned_parquet_to_df(mock_boto3_client, bucket, prefix)

        # Assert
        mock_boto3_client.get_paginator.assert_called_once_with(
            "list_objects_v2"
        )

    @patch("boto3.client")
    @patch("pandas.concat")
    @patch("src.query.s3_parquet_to_df")
    def test_should_call_client_get_paginator_paginate(
        self,
        mock_s3_parquet_to_df: MagicMock,
        mock_pandas_concat: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        prefix = "test-prefix"
        mock_s3_parquet_to_df.return_value = MagicMock(spec=pd.DataFrame)
        paginator = mock_boto3_client.get_paginator.return_value

        # Act
        s3_partitioned_parquet_to_df(mock_boto3_client, bucket, prefix)

        # Assert
        paginator.paginate.assert_called_once_with(
            Bucket=bucket, Prefix=prefix
        )

    @patch("boto3.client")
    @patch("pandas.concat")
    @patch("src.query.s3_parquet_to_df")
    def test_should_call_s3_parquet_to_df(
        self,
        mock_s3_parquet_to_df: MagicMock,
        mock_pandas_concat: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        prefix = "test-prefix"
        mock_s3_parquet_to_df.return_value = MagicMock(spec=pd.DataFrame)
        paginator = mock_boto3_client.get_paginator.return_value
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "test-key.parquet"}]}
        ]

        # Act
        s3_partitioned_parquet_to_df(mock_boto3_client, bucket, prefix)

        # Assert
        mock_s3_parquet_to_df.assert_called_once_with(
            mock_boto3_client, bucket, "test-key.parquet"
        )

    @patch("boto3.client")
    @patch("pandas.concat")
    @patch("src.query.s3_parquet_to_df")
    def test_should_call_pandas_concat(
        self,
        mock_s3_parquet_to_df: MagicMock,
        mock_pandas_concat: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        bucket = "test-bucket"
        prefix = "test-prefix"
        mock_s3_parquet_to_df.return_value = MagicMock(spec=pd.DataFrame)
        paginator = mock_boto3_client.get_paginator.return_value
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "test-key.parquet"}]}
        ]

        # Act
        s3_partitioned_parquet_to_df(mock_boto3_client, bucket, prefix)

        # Assert
        mock_s3_parquet_to_df.assert_called_once_with(
            mock_boto3_client, bucket, "test-key.parquet"
        )


class TestQueryS3RunQuery:

    @patch("boto3.client")
    @patch("duckdb.connect")
    @patch("src.query.s3_partitioned_parquet_to_df")
    def test_should_call_s3_partitioned_parquet_to_df(
        self,
        mock_s3_partitioned_parquet_to_df: MagicMock,
        mock_duckdb_connect: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        datasets: List[Datasets] = [
            {"bucket": "test-bucket", "prefix": "test-prefix"}
        ]
        query = "SELECT * FROM test;"
        mock_s3_partitioned_parquet_to_df.return_value = MagicMock(
            spec=pd.DataFrame
        )

        # Act
        run_query(datasets, query)

        # Assert
        mock_s3_partitioned_parquet_to_df.call_count == 1
        mock_s3_partitioned_parquet_to_df.call_args == [
            (mock_boto3_client, "test-bucket", "test-prefix")
        ]

    @patch("boto3.client")
    @patch("duckdb.connect")
    @patch("src.query.s3_partitioned_parquet_to_df")
    def test_should_call_connect_register(
        self,
        mock_s3_partitioned_parquet_to_df: MagicMock,
        mock_duckdb_connect: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        datasets: List[Datasets] = [
            {"bucket": "test-bucket", "prefix": "test-prefix"}
        ]
        query = "SELECT * FROM test;"
        mock_s3_partitioned_parquet_to_df.return_value = MagicMock(
            spec=pd.DataFrame
        )

        # Act
        run_query(datasets, query)

        # Assert
        mock_duckdb_connect.return_value.register.assert_called_once_with(
            "test-prefix", mock_s3_partitioned_parquet_to_df.return_value
        )

    @patch("boto3.client")
    @patch("duckdb.connect")
    @patch("src.query.s3_partitioned_parquet_to_df")
    def test_should_call_connect_sql(
        self,
        mock_s3_partitioned_parquet_to_df: MagicMock,
        mock_duckdb_connect: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        datasets: List[Datasets] = [
            {"bucket": "test-bucket", "prefix": "test-prefix"}
        ]
        query = "SELECT * FROM test;"
        mock_s3_partitioned_parquet_to_df.return_value = MagicMock(
            spec=pd.DataFrame
        )

        # Act
        run_query(datasets, query)

        # Assert
        mock_duckdb_connect.return_value.sql.assert_called_once_with(query)

    @patch("boto3.client")
    @patch("duckdb.connect")
    @patch("src.query.s3_partitioned_parquet_to_df")
    def test_should_return_dict(
        self,
        mock_s3_partitioned_parquet_to_df: MagicMock,
        mock_duckdb_connect: MagicMock,
        mock_boto3_client: MagicMock,
    ) -> None:
        # Arrange
        datasets: List[Datasets] = [
            {"bucket": "test-bucket", "prefix": "test-prefix"}
        ]
        query = "SELECT * FROM test;"
        mock_s3_partitioned_parquet_to_df.return_value = MagicMock(
            spec=pd.DataFrame
        )
        mock_duckdb_connect.return_value.sql.return_value.df.return_value = (
            pd.DataFrame([{"test": "test"}])
        )

        # Act
        result = run_query(datasets, query)

        # Assert
        assert isinstance(result, list)
        assert isinstance(result[0], dict)
