import pytz
import threading

from datetime import timedelta
from helper import *

REGION_NAME     = 'us-east-1'
MAX_THREADS     = 10

ssm             = GetSsmParameters(region_name=REGION_NAME)
dynamodb        = boto3.resource('dynamodb')
sqs             = boto3.client('sqs')

eligbile_table  = ssm.get_single('dev_ycf_dynamodb')
sqs_queue_url   = ssm.get_single('dev_forecast_queue_url')


def push(thread_name, record):
    sqs.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=json.dumps(record)
    )

    print ('{} PUSHED {}'.format(thread_name, record))


def process(yesterday, segment):
    thread_name = threading.current_thread().getName()
    print ('%10s starting..' % thread_name)

    records = []

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
        TotalSegments=MAX_THREADS,
        Segment=segment,
        ConsistentRead=True
    )
    records.extend(rs.get('Items', []))

    while 'LastEvaluatedKey' in rs:
        print ('{} scanning .. {}'.format(thread_name, len(records)))

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
            TotalSegments=10,
            Segment=segment,
            ExclusiveStartKey=rs['LastEvaluatedKey'],
            ConsistentRead=True
        )
        records.extend(rs.get('Items', []))

    print ('{} number of records: {}'.format(thread_name, len(records)))
    for record in records:
        calc_date = record['calc_date'][:10]
        if calc_date != yesterday[:10]:
            # print ('Skipped item with calc_date {}'.format(calc_date))
            continue

        push(
            thread_name=threading,
            record=record
        )

        time.sleep(0.1)


def lambda_handler(event, context):
    yesterday = str(datetime.now(pytz.timezone('EST')) - timedelta(days=11)).strip()
    print ('Yesterday: {}'.format(yesterday[:10]))

    threads = []
    # Creating threads with i as the segment id
    for i in range(MAX_THREADS):
        threads.append(threading.Thread(target=process, args=(yesterday, i)))

    # Start threads
    for thread in threads:
        thread.start()

    # Block main thread until all threads are finished
    for thread in threads:
        thread.join()

    print ('Completed.')