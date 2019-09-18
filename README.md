# cv_pilot_ingest
Code for CV pilot data ingestion pipeline (ingestion into ITS Sandbox and from ITS Sandbox to Socrata) and utilities to work with ITS Sandbox data. For more information on ITS Sandbox data, please refer to README at the [ITS Sandbox README page](https://github.com/usdot-its-jpo-data-portal/sandbox/tree/split-repo#exporting-data-to-csv-with-sandbox-exporter).

This repository currently includes several utility scripts: Sandbox Exporter, S3 Folder Restructurer, Data Flattener, and Socrata Connector.

## Usage example

### Sandbox Exporter

This utility can be used to download data generated between a specified date range into larger merged CSV or JSON file(s) by using our Sandbox Exporter script.

#### Prerequisites for using Sandbox Exporter

1) Have your own Free Amazon Web Services account.

	- Create one at http://aws.amazon.com

2) Obtain Access Keys:

	- On your Amazon account, go to your profile (at the top right)

	- My Security Credentials > Access Keys > Create New Access Key

	- Record the Access Key ID and Secret Access Key ID (you will need them in step 4)

3) Have access to Python 3.6+. You can check your python version by entering `python --version` and `python3 --version` in command line.

#### Exporting Data to CSV with Sandbox Exporter

1. Download the script by cloning the git repository at https://github.com/usdot-its-jpo-data-portal/cv_pilot_ingest. You can do so by running the following in command line.
`git clone https://github.com/usdot-its-jpo-data-portal/cv_pilot_ingest.git`. If unfamiliar with how to clone a repository, follow the guide at https://help.github.com/en/articles/cloning-a-repository.
2. Navigate into the repository folder by entering `cd cv_pilot_ingest` in command line.
3. Install the required packages by running `pip install -r requirements.txt`.
4. Modify the s3 credentials listed at the head of `sandbox_to_csv.py` to use your AWS s3 credentials.
5. Run the script by entering `python -u sandbox_to_csv.py`. You may get more details on each parameters of the script by entering `python -u sandbox_to_csv.py --help`
```
--bucket BUCKET       Name of the s3 bucket. Default: usdot-its-cvpilot-
											public-data
--pilot PILOT         Pilot name (options: wydot, thea).
--message_type MESSAGE_TYPE
											Message type (options: bsm, tim, spat).
--sdate SDATE         Starting generatedAt date of your data, in the format
											of YYYY-MM-DD.
--edate EDATE         Ending generatedAt date of your data, in the format of
											YYYY-MM-DD. Will be set to 24 hours from the start
											date if not supplied.
--json                Supply flag if file is to be exported as newline json
											instead of CSV file.
```
Example :
- Retrieve all WYDOT TIM data from 2019-09-16:
`python -u sandbox_to_csv.py --pilot wydot --message_type tim --sdate 2019-09-16`
- Retrieve all WYDOT TIM data between 2019-09-16 to 2019-09-18:
`python -u sandbox_to_csv.py --pilot thea --message_type tim --sdate 2019-09-16 --edate 2019-09-18`
- Retrieve all WYDOT TIM data between 2019-09-16 to 2019-09-18 in json newline format (instead of flattened CSV):
`python -u sandbox_to_csv.py --pilot thea --message_type tim --sdate 2019-09-16 --edate 2019-09-18 --json`

### S3 Folder Restructurer

This utility can be used to reorganizing folder based on generatedAt timestamp.

Sample command line prompt:
```
python -u restructure_folder.py
	--bucket usdot-its-cvpilot-public-data
	--bucket_prefix usdot-its-datahub-
	--folder wydot/BSM/2018
	--outfp wydotBSM2018fps.txt
	--startKey wydot/BSM/2018/11/29/17/usdot-its-cvpilot-bsm-public-4-2018-11-29-17-54-20-2b9afefa-ff32-4b8d-b458-bed83857dd46

```

Run `python restructure_folder.py --help` for more info on each parameter.
