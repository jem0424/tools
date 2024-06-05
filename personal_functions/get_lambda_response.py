from boto3 import client
import logging
from simplejson import load


def get_lambda_response(function_name: str, payload=b'{}') -> dict:
    """
    Get the response from a Lambda function.

    Args:
        function_name (str): The name of the Lambda function.
        payload (bytes, optional): The payload to pass to the Lambda function. Defaults to b'{}'.

    Returns:
        dict: The response from the Lambda function.
    """
    lambda_client = client('lambda')
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=payload
    )
    lambda_response = load(response["Payload"])
    return lambda_response

