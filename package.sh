#!/bin/bash
echo "Remove current package ingest_to_lake.zip"
rm -rf ingest_to_lake.zip
pip install -r requirements.txt --upgrade --target package/
cp lambda_function__ingest_to_lake.py s3_file_mover.py package/
mv package/lambda_function__ingest_to_lake.py package/lambda_function.py
cd package && zip -r ../ingest_to_lake.zip * && cd ..
rm -rf package
echo "Created package in ingest_to_lake.zip"
