import base64
import boto3
import json
import os


print('Loading function')

client = boto3.client('dynamodb')

def handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data']).decode('utf-8')
        
        telemetry_item = json.loads(payload)
        
        response = client.get_item(TableName=os.environ['TABLE_NAME'], Key={'name': {'S': telemetry_item['id']}})
        item = response['Item']
        
        telemetry_item['monthsOfUsage'] = int(item['monthsOfUsage']['N'])
        
        telemetry_item['location'] = {'lat': float(item['lat']['N']), 'lon': float(item['lon']['N']) }
        
        telemetry_item['name'] = telemetry_item.pop('id')
        
        print(telemetry_item)

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(telemetry_item).encode('utf-8'))
        }
        
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}