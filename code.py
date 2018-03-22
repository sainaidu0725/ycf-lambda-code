import time
import json
from YCFHelper import *


REGION_NAME     = 'us-east-1'

ssm = GetSsmParameters(region_name=REGION_NAME)
sqs = boto3.client('sqs')

sqs_queue_url   = ssm.get_single('dev_queue_url')


# Event data provides the DynamoDB event which triggered the lambda
def lambda_handler(event, context):
    print ('Number of records: {}'.format(len(event['Records'])))
    for r in event['Records']:
        record = r['dynamodb']

        inventory_id = record['Keys']['inventory_id']['S']
        issue_date_time = record['Keys']['issue_date_time']['S']
        status = record['NewImage']['status']['S']

        # Only process this incoming record if the DynamoDB record status is Pending
        if status != 'Pending':
            print ('[IGNORED] {} {} {}'.format(inventory_id, issue_date_time, status))
            continue

        sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps({
                'inventory_id': inventory_id,
                'issue_date_time': issue_date_time,
                'location_num': record['NewImage']['location_num']['S'],
                'month':record['NewImage']['month']['S'],
                'year':record['NewImage']['year']['S'],
                'calc_date': record['NewImage']['calc_date']['S'],
                'status': status,
                'last_inventoryperiod_enddate': record['NewImage']['last_inventoryperiod_enddate']['S'],
                'current_inventoryperiod_enddate': record['NewImage']['current_inventoryperiod_enddate']['S'],
                'inventoryperiod_description': record['NewImage']['inventoryperiod_description']['S'],
                'forecast_status': record['NewImage']['forecast_status']['S']
            })
        )

        print ('[PUSHED] {} {}'.format(inventory_id, issue_date_time))
        time.sleep(0.2)