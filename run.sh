#!/bin/bash
set -euo pipefail

python3 -m venv .env
source .env/bin/activate
pip install -r requirements.txt

# Prepare lambda packages
cd lambda

pip install --target ./load-ddb-data crhelper
cd ./load-ddb-data
zip -r ../load-ddb-data.zip .
cd ..
pip install -r es-requirements.txt --target ./load-es-index/
cd ./load-es-index
zip -r ../load-es-index.zip .
cd ..
pip install -r es-requirements.txt --target ./load-kibana-dashboards/
cd ./load-kibana-dashboards
zip -r ../load-kibana-dashboards.zip .

cd ../..

cdk bootstrap
cdk deploy iot-kinesis-es -c cognito_user_email=$USER_EMAIL