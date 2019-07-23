#!/bin/bash
echo "Remove current package ingest_to_lake.zip"
rm -rf ingest_to_lake.zip
pip install -r requirements.txt --upgrade --target package/
cp lambda__ingest_to_lake.py s3_file_mover.py package/
mv package/lambda__ingest_to_lake.py package/lambda_function.py
cd package && zip -r ../ingest_to_lake.zip * && cd ..
rm -rf package
echo "Created package in ingest_to_lake.zip"

echo "Remove current package lake_to_socrata.zip"
rm -rf lake_to_socrata.zip
pip install -r requirements.txt --upgrade --target package/
cp lambda__lake_to_socrata.py s3_file_mover.py socrata_util.py flattener* package/
mv package/lambda__lake_to_socrata.py package/lambda_function.py
cd package && zip -r ../lake_to_socrata.zip * && cd ..
rm -rf package
echo "Created package in lake_to_socrata.zip"
