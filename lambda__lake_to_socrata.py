"""
Move file from ingest s3 bucket to ITS DataHub sandbox s3.

"""

from __future__ import print_function

import logging
import os
import traceback


from s3_file_mover import CvPilotFileMover
from socrata_util import SocrataDataset


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

SOCRATA_USERNAME = os.environ.get('SOCRATA_USERNAME')
SOCRATA_PASSWORD = os.environ.get('SOCRATA_PASSWORD')
SOCRATA_API_KEY = os.environ.get('SOCRATA_API_KEY')
SOCRATA_DOMAIN = os.environ.get('SOCRATA_DOMAIN') or 'data.transportation.gov'
SOCRATA_DATASET_ID = os.environ.get('SOCRATA_DATASET_ID')

socrata_params = dict(
username = SOCRATA_USERNAME,
password = SOCRATA_PASSWORD,
app_token = SOCRATA_API_KEY,
domain = SOCRATA_DOMAIN
)


def load_flattener(key):
    pilot, message_type = key.split('/')[:2]
    try:
        mod = __import__('flattener_{}'.format(pilot))
        flattener = getattr(mod, '{}{}Flattener'.format(pilot.title(), message_type))
    except:
        print('Module not found. Load generic CVP flattener.')
        mod = __import__('flattener')
        flattener = getattr(mod, 'CvDataFlattener')
    return flattener


def lambda_handler(event, context):
    """AWS Lambda handler. """

    so_ingestor = SocrataDataset(dataset_id=SOCRATA_DATASET_ID,
    socrata_client=None, socrata_params=socrata_params)

    mover = CvPilotFileMover(target_bucket=TARGET_BUCKET)

    for bucket, key in mover.get_fps_from_event(event):
        flattener = load_flattener(key)

        recs = []
        err_recs = []
        stream = prodMover.get_data_stream(bucket, key)
        for r in prodMover.newline_json_rec_generator(stream):
            try:
                recs += flattener.process_and_split(r)
            except:
                logger.error("Error while transforming record: {}".format(event))
                logger.error(traceback.format_exc())
                err_recs.append(r)

        response = clean_and_upsert(recs)

    logger.info('Processed events')
