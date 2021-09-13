#!/usr/bin/env python3

from aws_cdk import core

from analytics.iot_kinesis_elasticsearch_stack import IotKinesisElasticsearchStack

app = core.App()

project_name = 'power-transformers-telemetry'

iot_analytics = IotKinesisElasticsearchStack(app, "iot-kinesis-es", {'projectName': project_name})

app.synth()
