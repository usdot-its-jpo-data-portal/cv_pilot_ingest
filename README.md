# cv_pilot_ingest
Code for CV pilot data ingestion pipeline (ingestion into ITS Sandbox and from ITS Sandbox to Socrata) and utilities to work with ITS Sandbox data.

## Usage example

### Reorganizing folder based on generatedAt timestamp

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