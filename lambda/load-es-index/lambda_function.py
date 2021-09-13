from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from crhelper import CfnResource

import boto3
import os

helper = CfnResource()

@helper.create
@helper.update
def load_es_index(event, _):
    host = os.environ['ES_HOST']
    region = os.environ['ES_REGION']

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service,
                       session_token=credentials.token)

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    mappings = {
        "properties": {
            "location": {"type": "geo_point"},
            "oilTemperature": {"type": "double"},
            "oilLevel": {"type": "double"},
            "temperature": {"type": "double"},
            "humidity": {"type": "double"},
            "noiseLevel": {"type": "double"},
            "gasEmissionLevel": {"type": "double"},
            "moistureLevel": {"type": "double"},
            "monthsOfUsage": {"type": "long"},
            "timestamp": {"type": "date"},
            "name": {"type": "keyword"}
        }
    }

    es.indices.create(index='power-transformer', body={'mappings': mappings}, include_type_name=False)

@helper.delete
def no_op(_, __):
    pass


def handler(event, context):
    helper(event, context)
