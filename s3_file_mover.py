"""
Move file from ingest s3 bucket to ITS DataHub sandbox s3.

"""

from __future__ import print_function

import logging
import boto3
from datetime import datetime
from gzip import GzipFile
from io import TextIOWrapper
import json
import os
import re
import requests
import traceback
import uuid


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging


class S3FileMover(object):

    def __init__(self, target_bucket=None, log=True, s3_client=None):
        self.target_bucket = target_bucket
        self.s3_client = s3_client or boto3.client('s3')
        self.print_func = print
        if log:
            self.print_func = logger.info
        self.err_lines = []

    def get_fps_from_event(self, event):
        bucket_key_tuples = [(e['s3']['bucket']['name'], e['s3']['object']['key']) for e in event['Records']]
        bucket_key_dict = {os.path.join(bucket, key): (bucket, key) for bucket, key in bucket_key_tuples}
        bucket_key_tuples_deduped = list(bucket_key_dict.values())
        return bucket_key_tuples_deduped

    def get_fps_from_prefix(self, bucket, prefix, limit=0):
        s3_source_kwargs = dict(Bucket=bucket, Prefix=prefix)

        bucket_key_tuples = []
        while True:
            resp = self.s3_client.list_objects_v2(**s3_source_kwargs)
            if not resp.get('Contents'):
                return []
            bucket_key_tuples += [(bucket, i['Key']) for i in resp['Contents']]
            if not resp.get('NextContinuationToken'):
                break
            s3_source_kwargs['ContinuationToken'] = resp['NextContinuationToken']
            if limit > 0 and len(bucket_key_tuples) > limit:
                break
        return bucket_key_tuples

    def get_data_stream(self, bucket, key):
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        if key[-3:] == '.gz':
            gzipped = GzipFile(None, 'rb', fileobj=obj['Body'])
            data = TextIOWrapper(gzipped)
        else:
            data = obj['Body']._raw_stream
        return data

    def newline_json_rec_generator(self, data_stream):
        line = data_stream.readline()
        while line:
            if type(line) == bytes:
                line_stripped = line.strip(b'\n')
            else:
                line_stripped = line.strip('\n')

            try:
                if line_stripped:
                    yield json.loads(line_stripped)
            except:
                self.print_func(traceback.format_exc())
                self.print_func('Invalid json line. Skipping: {}'.format(line))
                self.err_lines.append(line)
            line = data_stream.readline()

    def write_recs(self, recs, bucket, key):
        outbytes = "\n".join([json.dumps(i) for i in recs if i]).encode('utf-8')
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=outbytes)

    def delete_file(self, bucket, key):
        self.s3_client.delete_object(Bucket=bucket, Key=key)

    def move_file(self, source_bucket, source_key):
        source_path = os.path.join(source_bucket, source_key)
        self.print_func('Triggered by file: {}'.format(source_path))

        data_stream = self.get_data_stream(source_bucket, source_key)
        recs = []
        for rec in self.newline_json_rec_generator(data_stream):
            recs.append(rec)

        if recs:
            target_key = source_key
            target_path = os.path.join(self.target_bucket, target_key)
            self.print_func('Writing {} records from {} -> {}'.format(len(recs), source_path, target_path))
            self.write_recs(recs, self.target_bucket, target_key)
        else:
            self.print_func('File is empty: {}'.format(source_path))

        self.print_func('Delete file: {}'.format(source_path))
        self.delete_file(source_bucket, source_key)


class CvPilotFileMover(S3FileMover):

    def __init__(self, source_bucket_prefix='usdot-its-datahub-', source_key_prefix=None, validation_queue_names=[], *args, **kwargs):
        super(CvPilotFileMover, self).__init__(*args, **kwargs)
        self.source_bucket_prefix = source_bucket_prefix
        self.source_key_prefix = source_key_prefix or ''
        self.queues = []
        self.pilot_name = None
        self.message_type = None

        if validation_queue_names:
            for validation_queue_name in validation_queue_names:
                sqs = boto3.resource('sqs')
                queue = sqs.get_queue_by_name(QueueName=validation_queue_name)
                self.queues.append(queue)

    def generate_outfp(self, ymdh_data_dict, source_bucket, source_key):
        if not ymdh_data_dict:
            self.print_func('File is empty: s3://{}/{}'.format(source_bucket, source_key))
            return None

        original_ymdh = "-".join(source_key.split('/')[-5:-1])
        no_change = "".join(ymdh_data_dict.keys()) == original_ymdh

        filename_prefix = self.target_bucket.replace('-public-data', '')
        regex_str = r'(?:test-)?{}(.*)-ingest'.format(self.source_bucket_prefix)
        regex_finds = re.findall(regex_str, source_bucket)
        if len(regex_finds) == 0:
            # if source bucket is sandbox
            pilot_name = source_key.split('/')[0]
            message_type = source_key.split('/')[1]
            stream_version = '0'
            if no_change and source_bucket == self.target_bucket:
                self.print_func('No need to reorder data at s3://{}/{}'.format(source_bucket, source_key))
                return None
        else:
            pilot_name = regex_finds[0].lower()
            message_type = source_key.strip(self.source_key_prefix).split('/')[0]

            # get stream version
            regex_str2 = filename_prefix+r'-(?:.*)-public-(\d)-(?:.*)'
            stream_version_res = re.findall(regex_str2, source_key)
            if not stream_version_res:
                stream_version = '0'
            else:
                stream_version = stream_version_res[0]

        def outfp_func(ymdh):
            y,m,d,h = ymdh.split('-')
            ymdhms = '{}-00-00'.format(ymdh)
            uuid4 = str(uuid.uuid4())

            target_filename = '-'.join([filename_prefix, message_type.lower(), 'public', str(stream_version), ymdhms, uuid4])
            target_prefix = os.path.join(pilot_name, message_type, y, m, d, h)
            target_key = os.path.join(target_prefix, target_filename)
            return target_key

        self.pilot_name  = pilot_name
        self.message_type = message_type

        return outfp_func

    def get_ymdh(self, rec):
        recordGeneratedAt = rec['metadata'].get('recordGeneratedAt')
        if not recordGeneratedAt:
            recordGeneratedAt = rec['payload']['data']['timeStamp']
        try:
            dt = datetime.strptime(recordGeneratedAt[:14].replace('T', ' '), '%Y-%m-%d %H:')
        except:
            self.print_func(traceback.format_exc())
            recordReceivedAt = rec['metadata'].get('odeReceivedAt')
            dt = datetime.strptime(recordReceivedAt[:14].replace('T', ' '), '%Y-%m-%d %H:')
            self.print_func('Unable to parse {} timestamp. Using odeReceivedAt timestamp of {}'.format(recordGeneratedAt, recordReceivedAt))
        recordGeneratedAt_ymdh = datetime.strftime(dt, '%Y-%m-%d-%H')
        return recordGeneratedAt_ymdh


    def move_file(self, source_bucket, source_key):
        # read triggering file
        source_path = os.path.join(source_bucket, source_key)
        self.print_func('Triggered by file: {}'.format(source_path))

        # sort all files by generatedAt timestamp ymdh
        ymdh_data_dict = {}
        data_stream = self.get_data_stream(source_bucket, source_key)
        for rec in self.newline_json_rec_generator(data_stream):
            recordGeneratedAt_ymdh = self.get_ymdh(rec)
            if recordGeneratedAt_ymdh not in ymdh_data_dict:
                ymdh_data_dict[recordGeneratedAt_ymdh] = []
            ymdh_data_dict[recordGeneratedAt_ymdh].append(rec)

        # generate output path
        outfp_func = self.generate_outfp(ymdh_data_dict, source_bucket, source_key)
        if outfp_func is None:
            return

        for ymdh, recs in ymdh_data_dict.items():
            target_key = outfp_func(ymdh)
            target_path = os.path.join(self.target_bucket, target_key)

            # copy data
            self.print_func('Writing {} records from \n{} -> \n{}'.format(len(recs), source_path, target_path))
            self.write_recs(recs, self.target_bucket, target_key)
            self.print_func('File written')
            if self.queues:
                for queue in self.queues:
                    msg = {
                    'bucket': self.target_bucket,
                    'key': target_key,
                    'pilot_name': self.pilot_name,
                    'message_type': self.message_type.lower()
                    }
                    queue.send_message(MessageBody=json.dumps(msg))

        if len(self.err_lines) > 0:
            self.print_func('{} lines not read in file. Keep file at: {}'.format(len(self.err_lines), source_path))
        else:
            self.print_func('Delete file: {}'.format(source_path))
            self.delete_file(source_bucket, source_key)
        return
