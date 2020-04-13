
import boto3
import datetime
import json
import logging
import os

TABLE_NAME = os.getenv('TABLE_NAME')
QUEUE_URL = os.getenv('QUEUE_URL')

logger = logging.getLogger()
logger.setLevel(os.getenv('LOGLEVEL') or 'INFO')


def get_metadata_from_catalog(source, interval_start, interval_end):
    ddb = boto3.client('dynamodb')
    metadata = ddb.query(
        TableName=TABLE_NAME,
        KeyConditions={
            'Source': {
                'AttributeValueList': [
                    {
                        'S': source
                    }
                ],
                'ComparisonOperator': 'EQ'
            },
            'Timestamp': {
                'AttributeValueList': [
                    {
                        'S': interval_start
                    },
                    {
                        'S': interval_end
                    }
                ],
                'ComparisonOperator': 'BETWEEN'
            }
        }
    )
    return metadata['Items']


def send_messages(objects_metadata):
    sqs = boto3.client('sqs')
    for object_metada in objects_metadata:
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(
                {
                    'Replay': object_metada
                }
            )
        )


def lambda_handler(event, context):
    logger.info("Received event: {}".format(event))
    replay = json.loads(event['body'])
    objects_metadata = get_metadata_from_catalog(
        replay['Source'], replay['IntervalStart'], replay['IntervalEnd'])
    send_messages(objects_metadata)
    return {
        "headers": {
            "Content-Type": "application/json"
        },
        "statusCode": 202
    }
