from unittest.mock import MagicMock, patch

from lambda_function import lambda_handler


class TestLambdaFunction:

    @patch("lambda_function.run_query")
    def test_lambda_handler_should_call_run_query(
        self,
        mock_run_query: MagicMock,
        context: MagicMock,
    ) -> None:

        # Arrange
        mock_run_query.return_value = {"result": "test result"}
        event = {
            "datasets": '[{"bucket": "test-bucket", "prefix": "test-prefix"}]',
            "query": "SELECT * FROM test;",
        }

        # Act
        lambda_handler(event, context)

        # Assert
        assert mock_run_query.call_count == 1

    @patch("lambda_function.run_query")
    def test_lambda_handler_should_return_json(
        self,
        mock_run_query: MagicMock,
        context: MagicMock,
    ) -> None:

        # Arrange
        mock_run_query.return_value = {"result": "test result"}
        event = {
            "datasets": [{"bucket": "test-bucket", "prefix": "test-prefix"}],
            "query": "SELECT * FROM test;",
        }

        # Act
        response = lambda_handler(event, context)

        # Assert
        assert response == {"result": {"result": "test result"}}

    @patch("lambda_function.run_query")
    def test_lambda_handler_missing_datasets_json_error(
        self,
        mock_run_query: MagicMock,
        context: MagicMock,
    ) -> None:

        # Arrange
        mock_run_query.return_value = {"result": "test result"}
        event = {"query": "SELECT * FROM test;"}

        # Act
        response = lambda_handler(event, context)

        # Assert
        assert "error" in response
        assert mock_run_query.call_count == 0
