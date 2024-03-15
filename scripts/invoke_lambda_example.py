import json

import boto3

if __name__ == "__main__":
    boto3.setup_default_session(region_name="us-east-1")
    client = boto3.client("lambda")

    payload = {}

    response = client.invoke(
        FunctionName="test_lambda",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )

    response_str = response["Payload"].read().decode("utf-8")
    response_dict = json.loads(response_str)
    print(response_dict)
