#!/bin/bash
echo "Remove current package cv_pilot_ingest.zip"
rm -rf cv_pilot_ingest.zip
pip install -r requirements.txt --upgrade --target package/
cp lambda_function.py s3FileMover.py package/
cd package && zip -r ../cv_pilot_ingest.zip * && cd ..
rm -rf package
echo "Created package in cv_pilot_ingest.zip"
