from requests_aws4auth import AWS4Auth
from crhelper import CfnResource

import requests
import boto3
import json
import os

helper = CfnResource()

@helper.create
@helper.update
def load_dashboards(event, _):
    host = os.environ['ES_HOST']
    region = os.environ['ES_REGION']

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service,
                       session_token=credentials.token)

    with open('kibana-objects.json') as data_file:
        data = json.load(data_file)

    headers = {'kbn-xsrf': 'true'}

    print("Importing {} objects to Kibana".format(len(data)))

    for saved_object in data:
        url = 'https://{}/{}/{}/{}'.format(host, '_plugin/kibana/api/saved_objects', saved_object['_type'], saved_object['_id'])
        requests.post(url, auth=awsauth, json={'attributes': saved_object['_source']}, headers=headers)
@helper.delete
def no_op(_, __):
    pass


def handler(event, context):
    helper(event, context)
