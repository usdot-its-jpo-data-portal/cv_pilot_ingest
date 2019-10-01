"""
Flatten records from ITS Sandbox S3 bucket and upsert to Socrata.

"""

from __future__ import print_function

from datetime import datetime, timedelta
import logging
import os
import traceback

from s3_file_mover import CvPilotFileMover
from socrata_util import SocrataDataset
from flattener import load_flattener


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

SOCRATA_USERNAME = os.environ.get('SOCRATA_USERNAME')
SOCRATA_PASSWORD = os.environ.get('SOCRATA_PASSWORD')
SOCRATA_API_KEY = os.environ.get('SOCRATA_API_KEY')
SOCRATA_DOMAIN = os.environ.get('SOCRATA_DOMAIN', 'data.transportation.gov')
SOCRATA_DATASET_ID = os.environ.get('SOCRATA_DATASET_ID')

# the environment variables below are only required for lambdas that are
# triggered by CloudWatch Events, not by S3 object uploads.
S3_SOURCE_BUCKET = os.environ.get('S3_SOURCE_BUCKET', '')
S3_SOURCE_PREFIX = os.environ.get('S3_SOURCE_PREFIX', '')
NUM_HOURS_BACKTRACK = int(os.environ.get('NUM_HOURS_BACKTRACK', 48))


socrata_params = dict(
username = SOCRATA_USERNAME,
password = SOCRATA_PASSWORD,
app_token = SOCRATA_API_KEY,
domain = SOCRATA_DOMAIN
)

skip_time_ms = 60*1000


def lambda_handler(event, context):
    '''
    AWS Lambda handler.


    '''
    mover = CvPilotFileMover()
    so_ingestor = SocrataDataset(
        dataset_id=SOCRATA_DATASET_ID,
        socrata_params=socrata_params,
        float_fields=['randomNum', 'metadata_generatedAt_timeOfDay'])

    if event.get('source') == 'aws.events':
        overwrite = True
        workingId = so_ingestor.create_new_draft()

        source_ymdh = datetime.today() - timedelta(hours=NUM_HOURS_BACKTRACK)
        y,m,d = source_ymdh.strftime('%Y-%m-%d').split('-')
        formatted_source_prefix = S3_SOURCE_PREFIX.format(y,m,d)
        bucket_key_tuples = mover.get_fps_from_prefix(bucket=S3_SOURCE_BUCKET, prefix=formatted_source_prefix, limit=10000)
        logger.info('Lambda triggered by scheduled event. Retrieved {} file paths from s3://{}/{}'.format(len(bucket_key_tuples), S3_SOURCE_BUCKET, formatted_source_prefix))
    else:
        # s3 triggered
        overwrite = False
        workingId = SOCRATA_DATASET_ID
        bucket_key_tuples = mover.get_fps_from_event(event)
        logger.info('Lambda triggered by uploaded s3 object. Retrieved {} file paths from event'.format(len(bucket_key_tuples)))

    count = 0
    for bucket, key in bucket_key_tuples:
        flattenerMod = load_flattener(key)
        flattener = flattenerMod()

        recs = []
        err_recs = []
        stream = mover.get_data_stream(bucket, key)
        for r in mover.newline_json_rec_generator(stream):
            try:
                recs += flattener.process_and_split(r)
            except:
                logger.error("Error while transforming record: {}".format(event))
                logger.error(traceback.format_exc())
                err_recs.append(r)

            if context.get_remaining_time_in_millis() < skip_time_ms:
                break

        response = so_ingestor.clean_and_upsert(recs, workingId)
        count += len(recs)
        logger.info(response)
        if context.get_remaining_time_in_millis() < skip_time_ms:
            logger.info('Not able to finish ingesting all files within lambda time limit. Skipping to publishing.')
            break

    # publish draft if this is an overwrite
    if overwrite is True:
        if count > 0:
            so_ingestor.publish_draft(workingId)
        else:
            so_ingestor.delete_draft(workingId)

    logger.info('Processed events')
