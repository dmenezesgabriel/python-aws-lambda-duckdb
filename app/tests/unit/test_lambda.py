from unittest.mock import MagicMock, patch

from lambda_function import lambda_handler


class TestLambdaFunction:

    @patch("lambda_function.run_query")
    @patch("lambda_function.json.loads")
    def test_lambda_handler_should_call_json_loads(
        self,
        mock_json_loads: MagicMock,
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
        assert mock_json_loads.call_count == 1

    @patch("lambda_function.run_query")
    @patch("lambda_function.json.dumps")
    def test_lambda_handler_should_call_json_dumps(
        self,
        mock_json_dumps: MagicMock,
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
        assert mock_json_dumps.call_count == 1

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
            "datasets": '[{"bucket": "test-bucket", "prefix": "test-prefix"}]',
            "query": "SELECT * FROM test;",
        }

        # Act
        response = lambda_handler(event, context)

        # Assert
        assert response == {"result": '{"result": "test result"}'}

    @patch("lambda_function.run_query")
    def test_lambda_handler_invalid_datasets_json(
        self,
        mock_run_query: MagicMock,
        context: MagicMock,
    ) -> None:

        # Arrange
        mock_run_query.return_value = {"result": "test result"}
        event = {
            "datasets": '[{"bucket": "test-bucket", "prefix": "test-prefix"}',
            "query": "SELECT * FROM test;",
        }

        # Act
        response = lambda_handler(event, context)

        # Assert
        assert "error" in response
        assert mock_run_query.call_count == 0

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
