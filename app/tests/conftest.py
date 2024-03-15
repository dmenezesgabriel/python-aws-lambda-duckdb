import os
from unittest.mock import MagicMock

import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext


@pytest.fixture(scope="function")
def aws_credentials() -> None:
    os.environ["AWS_ACCESS_KEY"] = "mock-access-key"
    os.environ["AWS_SECRET_KEY"] = "mock-secret-key"
    os.environ["AWS_SESSION_TOKEN"] = "mock-session-token"
    os.environ["AWS_SECURITY_TOKEN"] = "mock-security-token"
    os.environ["AWS_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def context() -> MagicMock:
    return MagicMock(spec=LambdaContext)
