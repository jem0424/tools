from boto3 import client
import logging
from simplejson import load

logger = logging.getLogger(__name__)


def get_lambda_response(function_name: str, payload=b'{}'):
    lambda_client = client('lambda')
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=payload
    )
    lambda_response = load(response["Payload"])
    return lambda_response

