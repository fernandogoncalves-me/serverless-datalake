
import logging

logger = logging.getLogger()
logger.setLevel('INFO')


def lambda_handler(event, context):
    logger.info("Received event: {}".format(event))
