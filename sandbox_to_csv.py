"""
Folder Restructure utility script
"""
from argparse import ArgumentParser
import boto3
from copy import copy
import dateutil.parser
from datetime import datetime, timedelta
import json
import pandas as pd
import threading
import time
import traceback


from flattener import load_flattener
from s3_file_mover import CvPilotFileMover


# If credentials are held in env variables, set s3_credentials as an empty dictionary (default)
# If credentials are not held in env variables, comment out the second line that sets s3_credentials variable to
# a empty dictionary and fill out the s3_credentials object with your own credentials.

# s3_credentials = {
#     'aws_access_key_id': 'localkey',
#     'aws_secret_access_key': 'localsecret',
#     'aws_session_token': 'localtoken',
#     'region_name': 'us-east-1'
# }

s3_credentials = {}


class SandboxExporter(object):

    def __init__(self, bucket='usdot-its-cvpilot-public-data', pilot='wydot', message_type='bsm', sdate=None, edate=None, csv=True):
        # set up
        self.bucket = bucket
        self.pilot=pilot
        self.message_type=message_type
        self.sdate = None
        self.edate = None
        self.csv = csv

        if sdate:
            self.sdate = dateutil.parser.parse(sdate)
        if edate:
            self.edate = dateutil.parser.parse(edate)
        else:
            self.edate = self.sdate + timedelta(hours=24)

        s3botoclient = boto3.client('s3', **s3_credentials)
        self.mover = CvPilotFileMover(target_bucket=bucket,
                                 source_bucket_prefix="",
                                 source_key_prefix="",
                                 validation_queue_name=None,
                                 log=False,
                                 s3_client=s3botoclient)

        flattenerMod = load_flattener('{}/{}'.format(pilot, message_type.upper()))
        self.flattener = flattenerMod()
        self.current_recs = []
        self.file_names = []


    def get_folder_prefix(self, dt):
        y,m,d,h = dt.strftime('%Y-%m-%d-%H').split('-')
        folder = '{}/{}/{}/{}/{}/{}'.format(self.pilot, self.message_type.upper(), y, m, d, h)
        return folder

    def write_json_newline(self, recs, fp):
        with open(fp, 'w') as outfile:
            for r in recs:
                outfile.write(json.dumps(r))
                outfile.write('\n')
        self.file_names.append(fp)

    def write_csv(self, recs, fp):
        flat_recs = []
        for r in recs:
            flat_recs += self.flattener.process_and_split(r)
        df = pd.DataFrame(flat_recs)
        df.to_csv(fp, index=False, encoding='utf-8')
        self.file_names.append(fp)

    def write(self, recs, fp):
        if self.csv:
            ext = '.csv'
            self.write_csv(recs, fp+ext)
        else:
            ext = '.txt'
            self.write_json_newline(recs, fp+ext)
        print('Wrote {} recs to {}'.format(len(recs), fp+ext ))

    def process(self, key):
        s3botoclient = boto3.client('s3', **s3_credentials)
        mover = CvPilotFileMover(target_bucket=self.bucket,
                                 source_bucket_prefix="",
                                 source_key_prefix="",
                                 validation_queue_name=None,
                                 log=False,
                                 s3_client=s3botoclient)
        sb,sk = key
        stream = mover.get_data_stream(sb, sk)
        recs = []
        for r in mover.newline_json_rec_generator(stream):
            if self.csv:
                recs += self.flattener.process_and_split(r)
            else:
                recs.append(r)
        self.current_recs += recs
        print('.', end='', flush=True)
        return

    def run(self):
        print('===========START===========')
        print('Exporting {} {} data between {} and {}'.format(self.pilot, self.message_type, self.sdate.date, self.edate.date))
        t0 = time.time()
        fp = lambda filenum: '{}_{}_{}_{}_{}'.format(self.pilot, self.message_type.lower(), self.sdate.strftime('%Y%m%d%H'), self.edate.strftime('%Y%m%d%H'), filenum)
        sfolder = self.get_folder_prefix(self.sdate)
        efolder = self.get_folder_prefix(self.edate)

        numkeys = 0
        filenum = 0
        numrecs = 0
        curr_folder = sfolder
        curr_dt = copy(self.sdate)
        while curr_folder < efolder:
            threads = []
            keys = self.mover.get_fps_from_prefix(self.bucket, curr_folder)
            if len(keys) > 0:
                print('Processing {} keys from {}'.format(len(keys), curr_folder))
            for key in keys:
                self.process(key)
                # t = threading.Thread(target = self.process, args=(key,))
                # threads.append(t)
                # t.start()
            # if threads:
                # print('Waiting on {} threads'.format(len(threads)))
                # results = [t.join() for t in threads]
            if len(keys) > 0:
                print('{} recs processed from {}'.format(len(self.current_recs), curr_folder))

            numkeys += len(keys)
            curr_dt += timedelta(hours=1)
            curr_folder = self.get_folder_prefix(curr_dt)

            if len(self.current_recs) > 10000:
                self.write(self.current_recs, fp(filenum))
                numrecs += len(self.current_recs)
                self.current_recs = []
                filenum += 1

        if self.current_recs:
            self.write(self.current_recs, fp(filenum))
            numrecs += len(self.current_recs)
            filenum += 1
        t1 = time.time()
        print('===========================')
        print('{} keys retrieved between s3://{}/{} and s3://{}/{}'.format(numkeys, self.bucket, sfolder, self.bucket, efolder ))
        print('{} records from read and written to {} files in {} min'.format(numrecs, filenum, (t1-t0)/60))
        if self.file_names:
            print('Output files:\n{}'.format('\n'.join(self.file_names)))
        print('============END============')
        return


if __name__ == '__main__':
    """
    Sample Usage
    python -u restructure_folder.py --bucket usdot-its-cvpilot-public-data --bucket_prefix usdot-its-datahub- --folder wydot/BSM/2019/
    """

    parser = ArgumentParser(description="Script for exporting ITS sandbox data from specified date range to merged CSV files")
    parser.add_argument('--bucket', default="test-usdot-its-cvpilot-public-data", help="Name of the s3 bucket. Default: usdot-its-cvpilot-public-data")
    parser.add_argument('--pilot', default="wydot", help="Pilot name (options: wydot, thea).")
    parser.add_argument('--message_type', default=None, help="Message type (options: bsm, tim, spat).")
    parser.add_argument('--sdate', default=None, required=True, help="Starting generatedAt date of your data, in the format of YYYY-MM-DD.")
    parser.add_argument('--edate', default=None, help="Ending generatedAt date of your data, in the format of YYYY-MM-DD. Will be set to 24 hours from the start date if not supplied.")
    parser.add_argument('--json', default=False, action='store_true', help="Supply flag if file is to be exported as newline json instead of CSV file.")
    args = parser.parse_args()

    exporter = SandboxExporter(
        bucket=args.bucket,
        pilot=args.pilot,
        message_type=args.message_type,
        sdate=args.sdate,
        edate=args.edate,
        csv=bool(not args.json))
    exporter.run()
