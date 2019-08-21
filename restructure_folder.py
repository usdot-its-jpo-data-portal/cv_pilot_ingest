from argparse import ArgumentParser
import boto3
import traceback
import time

from s3_file_mover import CvPilotFileMover


# fill out the s3_credentials object with your own credential if credentials are not held in env variables
# if credentials are held in env varialbes, uncomment the line below to set s3_credentials as an empty object
s3_credentials = {
    'aws_access_key_id': 'localkey',
    'aws_secret_access_key': 'localsecret',
    'aws_session_token': 'localtoken',
    'region_name': 'us-east-1'
}
# s3_credentials = {}

def restructure_folder(bucket, folder='wydot/BSM', startKey=None, source_bucket_prefix="usdot-its-datahub-"):

    # set up
    s3botoclient = boto3.client('s3', **s3_credentials)
    mover = CvPilotFileMover(target_bucket=bucket,
                             source_bucket_prefix="",
                             source_key_prefix="",
                             validation_queue_name=None,
                             log=False,
                             s3_client=s3botoclient)

    # get list of files to process
    keys = mover.get_fps_from_prefix(bucket, folder)
    print('{} keys retrieved from s3://{}/{}'.format(len(keys), bucket, folder))
    if startKey:
        keysFiltered = [i for i in keys if i[1] >= startKey]
        print('Processing {} keys after start key of {}'.format(len(keysFiltered), startKey))
    else:
        keysFiltered = keys

    if len(keysFiltered) == 0:
        print('All set.')

    # move files
    t0 = time.time()
    count = 0
    for idx, tup in enumerate(keysFiltered):
        try:
            sb,sk = tup
            mover.move_file(sb, sk)
            count += 1
        except:
            print(traceback.format_exc())
            print('Process stopped at idx: {}'.format(idx))
            print('======================================')
            print('To continue running this script on the rest of the folder, specifiy the followng --startKey for your next run:\n{}'.format(sk))
            print('======================================')
            break

    t1 = time.time()
    print('{} fps re-orged in {} hr'.format(count, (t1-t0)/3600))


if __name__ == '__main__':
    """
    python -u restructure_folder.py --bucket usdot-its-cvpilot-public-data --bucket_prefix usdot-its-datahub- --folder wydot/BSM/2019/
    """

    parser = ArgumentParser(description="Script for reorganizing a folder based on generatedAt timestamp field")
    parser.add_argument('--bucket', default="test-usdot-its-cvpilot-public-data", help="Name of the s3 bucket.")
    parser.add_argument('--bucket_prefix', default="test-usdot-its-datahub-", help="Prefix of the s3 bucket name (e.g. 'usdot-its-datahub-' or 'test-usdot-its-datahub')")
    parser.add_argument('--folder', default=None, help="S3 folder you'd like to reorganize. All files with this prefix will be reorganized as needed, in ascending order of the key.")
    parser.add_argument('--startKey', default=None, help="Start Key - provide this only if you'd like to organize only keys after the specified key.")

    args = parser.parse_args()
    restructure_folder(args.bucket, folder=args.folder, startKey=args.startKey, source_bucket_prefix=args.bucket_prefix)
