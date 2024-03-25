import json
from typing import Any, Dict, List

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from src.query import Datasets, run_query

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
@logger.inject_lambda_context
def lambda_handler(
    event: Dict[str, Any], context: LambdaContext
) -> Dict[str, Any]:
    try:
        if "datasets" not in event or "query" not in event:
            logger.error("Invalid input: 'datasets' and 'query' are required")
            return {
                "error": "Invalid input: 'datasets' and 'query' are required"
            }

        datasets: List[Datasets] = event["datasets"]
        query: str = event["query"]

        result = run_query(datasets, query)

        msg = {"result": result}
        return msg

    except json.JSONDecodeError as e:
        # logger.error(f"Invalid input: {e}")
        return {"error": f"Invalid JSON Format: {e}"}

    except Exception as e:
        # logger.error(f"Error: {e}")
        return {"error": f"Error: {e}"}
