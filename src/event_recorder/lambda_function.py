import boto3
import gzip
import io
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(os.getenv('LOGLEVEL') or 'INFO')

TOPIC_SSM_PREFIX = os.getenv('TOPIC_SSM_PREFIX')
TABLE_NAME = os.getenv('TABLE_NAME')
BUCKET_NAME = os.getenv('BUCKET_NAME')


def create_catalog_entry(source, key, timestamp):
    return {
        'PutRequest': {
            'Item': {
                'Source': {
                    'S': source
                },
                'Timestamp': {
                    'S': timestamp
                },
                'Key': {
                    'S': key
                }
            }
        }
    }


def get_content(s3, obj_key):
    obj = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=obj_key
    )
    binary_content = io.BytesIO(obj['Body'].read())
    gzip_content = gzip.GzipFile(fileobj=binary_content)
    raw_content = gzip_content.read().decode('utf-8')
    content = "[{}]".format(raw_content.replace("}{", "},{"))
    return json.loads(content)


def write_catalog_entries(catalog_entries):
    ddb = boto3.client('dynamodb')
    logger.debug(
        "Writing batch to table {}: {}".format(TABLE_NAME, catalog_entries))
    ddb.batch_write_item(
        RequestItems=catalog_entries
    )


def publish_content(content):
    sns = boto3.client('sns')
    ssm = boto3.client('ssm')
    for source in content:
        topic_arn = ssm.get_parameter(Name="{}{}".format(
            TOPIC_SSM_PREFIX, source))['Parameter']['Value']
        for content in content[source]:
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps(content)
            )


def lambda_handler(event, context):
    logger.info("Received event: {}".format(event))
    s3 = boto3.client('s3')
    catalog_entries = {
        TABLE_NAME: []
    }
    content = {}
    for sqs_record in event['Records']:
        sqs_message = json.loads(sqs_record['body'])
        timestamp = sqs_record['attributes']['SentTimestamp']
        logger.debug("Processing SQS message: {}".format(sqs_message))
        if 'Records' in sqs_message:
            for s3_event in sqs_message['Records']:
                key = s3_event['s3']['object']['key']
                source = key.split('/')[0]
                catalog_entries[TABLE_NAME].append(
                    create_catalog_entry(source, key, timestamp)
                )
                if source in content:
                    content[source].extend(
                        get_content(s3, key))
                else:
                    content[source] = get_content(s3, key)
            write_catalog_entries(catalog_entries)
            publish_content(content)
            logger.info("Event processed successfully")
        elif 'Replay' in sqs_message:
            source = sqs_message['Replay']['Source']['S']
            content[source] = get_content(
                s3, sqs_message['Replay']['Key']['S'])
            publish_content(content)
            logger.info("Event processed successfully")
        else:
            logger.info("Ignoring invalid event")
