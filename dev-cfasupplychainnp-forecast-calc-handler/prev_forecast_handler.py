import pytz

from datetime import timedelta
from helper import *


REGION_NAME             = 'us-east-1'

ssm                     = GetSsmParameters(region_name=REGION_NAME)
dynamodb                = boto3.resource('dynamodb')
sqs                     = boto3.client('sqs')

eligbile_table          = ssm.get_single('dev_ycf_dynamodb')
sqs_queue_url           = ssm.get_single('dev_forecast_queue_url')


def process(record):
    sqs.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=json.dumps(record)
    )
    
    print ('PUSHED {}'.format(record))


def lambda_handler(event, context):
    yesterday = str(datetime.now(pytz.timezone('EST')) - timedelta(days=1)).strip()
    print (yesterday)
    
    rs = dynamodb.Table(eligbile_table).scan(
        Select='ALL_ATTRIBUTES',
        FilterExpression='#fs = :fs AND #s = :s',
        ExpressionAttributeNames={
            '#fs': 'forecast_status',
            '#s': 'status',
        },
        ExpressionAttributeValues={
            ':fs': 'Pending', 
            ':s': 'COMPLETED',
        },
        ConsistentRead=True
    )
    # 'this is a small change i want to test'
    print (rs['Items'])
    print ('Number of records: {}'.format(len(rs['Items'])))
    for record in rs['Items']:
        calc_date = record['calc_date'][:10]
        if calc_date != yesterday[:10]:
            print ('Skipped item with calc_date {}'.format(calc_date))
            continue

        process(record)
        time.sleep(0.1)