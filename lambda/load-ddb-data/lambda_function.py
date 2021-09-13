from crhelper import CfnResource
import boto3
import os

helper = CfnResource()

@helper.create
@helper.update
def load_ddb_data(event, _):
    data = {
        'PEQUETITA': [-23.592281, -46.685100, 80],
        'SHOPPINGJK': [-23.591521, -46.689667, 240],
        'ROCIO': [-23.593718, -46.684730, 300],
        'EATALY': [-23.591588, -46.683382, 60],
        'SHOPPINGVILAOLIMPIA': [-23.595452, -46.686497, 120],
        'FERREIRALOBO': [-23.593791, -46.683232, 200],
        'ESTANPLAZAFUNCHAL': [-23.593363, -46.688941, 136],
        'FUNCHAL10': [-23.595432, -46.690471, 180],
        'ESTACAOVILAOLIMPIA': [-23.593680, -46.692023, 77],
        'TEATROSANTANDER': [-23.592243, -46.691609, 34],
        'NACOESUNIDAS05': [-23.597272, -46.692596, 170],
        'FARIALIMA28': [-23.591122, -46.680859, 290],
        'FARIALIMA14': [-23.591250, -46.681481, 400],
        'FARIALIMA100': [-23.592027, -46.681202, 12],
        'FARIALIMA42': [-23.592027, -46.681202, 190]
    }

    client = boto3.client('dynamodb')

    for key in data.keys():
        info = data[key]

        client.put_item(
            TableName=os.environ['TABLE_NAME'],
            Item={
                'name': {'S': key},
                'lat': {'N': str(info[0])},
                'lon': {'N': str(info[1])},
                'monthsOfUsage': {'N': str(info[2])}
            }
        )
@helper.delete
def no_op(_, __):
    pass

def handler(event, context):
    helper(event, context)