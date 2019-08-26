"""
Folder Restructure utility script
"""
from argparse import ArgumentParser
import boto3
import traceback
import time

from s3_file_mover import CvPilotFileMover


# If credentials are not held in env variables, commend out the second line that sets s3_credentials variable to 
# a empty dictionary and fill out the s3_credentials object with your own credentials.
# If credentials are held in env varialbes, uncomment the line below to set s3_credentials as an empty dictionary

s3_credentials = {
    'aws_access_key_id': 'localkey',
    'aws_secret_access_key': 'localsecret',
    'aws_session_token': 'localtoken',
    'region_name': 'us-east-1'
}
s3_credentials = {}


class FolderRestructurer(object):

    def __init__(self, bucket, folder='wydot/BSM', startKey='', source_bucket_prefix="usdot-its-datahub-", outfp=None, infp=None):
        # set up
        s3botoclient = boto3.client('s3', **s3_credentials)
        self.mover = CvPilotFileMover(target_bucket=bucket,
                                 source_bucket_prefix="",
                                 source_key_prefix="",
                                 validation_queue_name=None,
                                 log=False,
                                 s3_client=s3botoclient)
        self.bucket = bucket
        self.folder = folder
        self.startKey = startKey
        self.infp = infp
        self.outfp = outfp

    def filter_by_startKey(self, keys):
        if self.startKey:
            keys = [i for i in keys if i[1] >= self.startKey]
            print('Processing {} keys after start key of {}'.format(len(keys), self.startKey))
        return keys

    def get_keys_from_s3(self):
        keys = self.mover.get_fps_from_prefix(self.bucket, self.folder)
        print('{} keys retrieved from s3://{}/{}'.format(len(keys), self.bucket, self.folder))
        keysFiltered = self.filter_by_startKey(keys)

        if len(keysFiltered) == 0:
            print('All set.')

        elif self.outfp:
            with open(self.outfp, 'w') as outfile:
                for k in keysFiltered:
                    outfile.write('{},{}'.format(k[0], k[1]))
                    outfile.write('\n')
            print('{} filepaths from s3://{}/{} written to {}'.format(len(keysFiltered), self.bucket, self.folder, self.outfp))
        return keysFiltered

    def get_keys_from_fp(self, fp):
        with open(fp, 'r') as infile:
            data = infile.read()
        data = data.splitlines()
        keys = [i.strip('\n').split(',') for i in data]
        print('{} keys retrieved from {}'.format(len(keys), fp))
        keysFiltered = self.filter_by_startKey(keys)
        return keysFiltered

    def run(self):
        # get fp
        if self.infp:
            keysFiltered = self.get_keys_from_fp(self.infp)
        else:
            keysFiltered = self.get_keys_from_s3()

        # move files
        t0 = time.time()
        count = 0
        for idx, tup in enumerate(keysFiltered):
            try:
                sb,sk = tup
                self.mover.move_file(sb, sk)
                count += 1
            except:
                print(traceback.format_exc())
                print('Process stopped at idx: {}'.format(idx))
                print('======================================')
                print('To continue running this script on the rest of the folder, specifiy the followng --startKey for your next run:\n{}\n'.format(sk))
                print('======================================')
                break

        t1 = time.time()
        print('{} fps re-orged in {} hr'.format(count, (t1-t0)/3600))


if __name__ == '__main__':
    """
    Sample Usage
    python -u restructure_folder.py --bucket usdot-its-cvpilot-public-data --bucket_prefix usdot-its-datahub- --folder wydot/BSM/2019/
    """

    parser = ArgumentParser(description="Script for reorganizing a folder based on generatedAt timestamp field")
    parser.add_argument('--bucket', default="test-usdot-its-cvpilot-public-data", help="Name of the s3 bucket.")
    parser.add_argument('--bucket_prefix', default="test-usdot-its-datahub-", help="Prefix of the s3 bucket name (e.g. 'usdot-its-datahub-' or 'test-usdot-its-datahub')")
    parser.add_argument('--folder', default=None, help="S3 folder you'd like to reorganize. All files with this prefix will be reorganized as needed, in ascending order of the key.")
    parser.add_argument('--startKey', default=None, help="Start Key - provide this only if you'd like to organize only keys after the specified key.")
    parser.add_argument('--infp', default=None, help="Supply fp if you'd like to read keys to process from the file path.")
    parser.add_argument('--outfp', default=None, help="Supply fp if you'd like to write the keys to process to a file.")
    args = parser.parse_args()
    
    folderRestructurer = FolderRestructurer(args.bucket, folder=args.folder, startKey=args.startKey, source_bucket_prefix=args.bucket_prefix, infp=args.infp, outfp=args.outfp)
    folderRestructurer.run()




