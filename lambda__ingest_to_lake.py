"""
Move file from ingest s3 bucket to ITS DataHub sandbox s3.

"""

from __future__ import print_function

import logging
import os
import traceback


from s3_file_mover import CvPilotFileMover


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

SLACK_WEBHOOK_URL = os.environ['SLACK_WEBHOOK_URL']
TARGET_BUCKET = os.environ['TARGET_BUCKET']
SOURCE_BUCKET_PREFIX = 'usdot-its-datahub-'
SOURCE_KEY_PREFIX = os.environ['SOURCE_KEY_PREFIX'] or ""
VALIDATION_QUEUE_NAME = os.environ['VALIDATION_QUEUE_NAME'] or None
if VALIDATION_QUEUE_NAME:
    VALIDATION_QUEUE_NAME = [i.strip() for i in VALIDATION_QUEUE_NAME.split(',')]


def lambda_handler(event, context):
    """AWS Lambda handler. """

    mover = CvPilotFileMover(target_bucket=TARGET_BUCKET,
                             source_bucket_prefix=SOURCE_BUCKET_PREFIX,
                             source_key_prefix=SOURCE_KEY_PREFIX,
                             validation_queue_names=VALIDATION_QUEUE_NAME)

    for bucket, key in mover.get_fps_from_event(event):
        try:
            mover.move_file(bucket, key)
        except Exception as e:
            # send_to_slack(traceback.format_exc())
            logger.error("Error while processing event record: {}".format(event))
            logger.error(traceback.format_exc())
            raise e

    logger.info('Processed events')
