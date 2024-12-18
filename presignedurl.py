

import logging
import boto3
import json
import base64
from botocore.config import Config
import os

log_level   = os.getenv('log_level')

def presigned_url(event, context):
    """Generate a presigned URL to share an S3 object
    :param bucket_name: string
    :param object_name: string
    :param content_type: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    :JSON Structure to invoke the API 
    {
        "bucket_name": "intents-artifacts-364685145795",
        "object_name": "response.txt",
        "expiration": "3600",
        "content_type": "application/xml",
        "request_type": "<operation*>"
    }

    <operation*> = put_object/get_object
    """
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logging.info(event)
    
    request_body = ''
    
    if (event['body']) and (event['body'] is not None):
        is_encoded = event['isBase64Encoded']
        if (is_encoded):
            encoded_body = event['body']
            request_body = json.loads(base64.b64decode(encoded_body))
        else:
            request_body = json.loads(event['body'])
    else:
        return None

    logging.info(request_body)
    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
    
    params={
                'Bucket': request_body['bucket_name'],
                'Key': request_body['object_name']
           }
    
    # Content Type is needed only for Put 
    if(request_body['request_type'] == 'put_object'):
        if 'content_type' in request_body:
            params['ContentType'] = request_body['content_type']

    logging.info('Parameters Passed to Presgined URL: ' + json.dumps(params))

    try:
        # request_body['request_type'] valid values = put_object or get_object
        response = s3_client.generate_presigned_url(request_body['request_type'],
                                                    Params = params,
                                                    ExpiresIn=request_body['expiration'])
        response_body = {
            "statusCode": "200",
            "expiration": request_body['expiration'],
            "s3_url"    : response
        }

        logging.info('Presigned URL: ' + str(response))

        response_body = base64.b64encode(json.dumps(response_body).encode())
        response = {
            "isBase64Encoded" : "true", 
            "statusCode": "200", 
            "headers": {
                "content-type":"application/json"
            },
            "body": response_body.decode()
        }
    except Exception as e:
        logging.error(e)
        body = {
            "statusCode": "500",
            "error": repr(e)
        }
        response = {
            "isBase64Encoded" : "false", 
            "statusCode": "500", 
            "headers": {
                "content-type":"application/json"
            },
            "body": json.dumps(body)
        }
        
    logging.info(json.dumps(response))
    # The response contains the presigned URL
    return response
    
