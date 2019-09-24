# cv_pilot_ingest
Utilities to work with ITS Sandbox and code for CV pilot data ingestion pipeline (ingestion into ITS Sandbox and from ITS Sandbox to Socrata). For more information on ITS Sandbox data, please refer to the [ITS Sandbox README page](https://github.com/usdot-its-jpo-data-portal/sandbox/tree/split-repo#exporting-data-to-csv-with-sandbox-exporter).

This repository currently includes several utility scripts: Sandbox Exporter, S3 Folder Restructurer, Data Flattener, and Socrata Connector. These utilities uses Python 3.x as the primary programming language and should be able to be executed across operative systems.

**Table of Contents**

* [Utilities](#utilities)
  * [Sandbox Exporter](#sandbox-exporter)
  * [S3 Folder Restructurer](#S3-Folder-Restructurer)

## Utilities
### Sandbox Exporter

This utility can be used to download data generated between a specified date range into larger merged CSV or JSON file(s) by using our Sandbox Exporter script.

#### Usage

`python -u sandbox_to_csv.py [-h] [--bucket BUCKET] [--pilot PILOT]
                         [--message_type MESSAGE_TYPE] --sdate SDATE
                         [--edate EDATE]
                         [--output_convention OUTPUT_CONVENTION] [--json]
                         [--aws_profile AWS_PROFILE]`

```
optional arguments:
  -h, --help            show this help message and exit
  --bucket BUCKET       Name of the s3 bucket. Default: usdot-its-cvpilot-
                        public-data
  --pilot PILOT         Pilot name (options: wydot, thea). Default: wydot
  --message_type MESSAGE_TYPE
                        Message type (options: bsm, tim, spat). Default: tim
  --sdate SDATE         Starting generatedAt date of your data, in the format
                        of YYYY-MM-DD.
  --edate EDATE         Ending generatedAt date of your data, in the format of
                        YYYY-MM-DD. If not supplied, this will be set to 24
                        hours from the start date.
  --output_convention OUTPUT_CONVENTION
                        Supply string for naming convention of output file.
                        Variables available for use in this string include:
                        pilot, messate_type, sdate, edate. Note that a file
                        number will always be appended to the output file
                        name. Default: {pilot}_{message_type}_{sdate}_{edate}
  --json                Supply flag if file is to be exported as newline json
                        instead of CSV file. Default: False
  --aws_profile AWS_PROFILE
                        Supply name of AWS profile if not using default
                        profile. AWS profile must be configured in
                        ~/.aws/credentials on your machine. See https://boto3.
                        amazonaws.com/v1/documentation/api/latest/guide/config
                        uration.html#shared-credentials-file for more
                        information.
```

Examples:
- Retrieve all WYDOT TIM data from 2019-09-16:
`python -u sandbox_to_csv.py --pilot wydot --message_type tim --sdate 2019-09-16`
- Retrieve all WYDOT TIM data between 2019-09-16 to 2019-09-18:
`python -u sandbox_to_csv.py --pilot thea --message_type tim --sdate 2019-09-16 --edate 2019-09-18`
- Retrieve all WYDOT TIM data between 2019-09-16 to 2019-09-18 in json newline format (instead of flattened CSV):
`python -u sandbox_to_csv.py --pilot thea --message_type tim --sdate 2019-09-16 --edate 2019-09-18 --json`

#### Configuration
The applications requires [Python 3.x](https://www.python.org/download/releases/3.0/) and the packages listed in the requirements.txt file.

##### Prerequisites for using Sandbox Exporter

1) Have your own Free Amazon Web Services account.

	- Create one at http://aws.amazon.com

2) Obtain Access Keys:

	- On your Amazon account, go to your profile (at the top right)

	- My Security Credentials > Access Keys > Create New Access Key

	- Record the Access Key ID and Secret Access Key ID (you will need them in step 4)

3) Have access to Python 3.6+. You can check your python version by entering `python --version` and `python3 --version` in command line.

4) Save your AWS credentials in your local machine, using one of the following method:
	- shared credentials file: instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file.
	- environmental variables: instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#environment-variables
	- (not preferred) hardcoding credentials in the script: instructions in lines 52-61 of `sandbox_to_csv.py`

#### Installation

1. Download the script by cloning the git repository at https://github.com/usdot-its-jpo-data-portal/cv_pilot_ingest. You can do so by running the following in command line.
`git clone https://github.com/usdot-its-jpo-data-portal/cv_pilot_ingest.git`. If unfamiliar with how to clone a repository, follow the guide at https://help.github.com/en/articles/cloning-a-repository.
2. Navigate into the repository folder by entering `cd cv_pilot_ingest` in command line.
3. Install the required packages by running `pip install -r requirements.txt`.

#### File Manifest
- Python 3.x : https://www.python.org/download/releases/3.0
- requests : https://pypi.org/project/requests/
- pandas : https://pypi.org/project/pandas/

#### Development setup

1. Download the script by cloning the git repository at https://github.com/usdot-its-jpo-data-portal/cv_pilot_ingest. You can do so by running the following in command line.
`git clone https://github.com/usdot-its-jpo-data-portal/cv_pilot_ingest.git`. If unfamiliar with how to clone a repository, follow the guide at https://help.github.com/en/articles/cloning-a-repository.
2. Run the application `python -u sandbox_to_csv.py --sdate 2019-09-16`

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

## Release History
* 0.1.0
  * Initial version


## Contact information
TBD


## Contributing
1. Fork it (https://github.com/usdot-its-jpo-data-portal/cv_pilot_ingest/fork)
2. Create your feature branch (git checkout -b feature/fooBar)
3. Commit your changes (git commit -am 'Add some fooBar')
4. Push to the branch (git push origin feature/fooBar)
5. Create a new Pull Request

## Known Bugs
*

## Credits and Acknowledgment
Thank you to the Department of Transportation for funding to develop this project.

## CODE.GOV Registration Info
* __Agency:__ DOT
* __Short Description:__ Utilities to work with ITS Sandbox and code for CV pilot data ingestion pipeline.
* __Status:__ Beta
* __Tags:__ transportation, connected vehicles, intelligent transportation systems, python, ITS Sandbox, Socrata
* __Labor Hours:__
* __Contact Name:__
* __Contact Phone:__
