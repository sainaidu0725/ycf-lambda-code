import boto3
import json
import os
import time

from aws_xray_sdk.core import xray_recorder
from datetime import datetime


class GetSsmParameters(object):
    def __init__(self, region_name):
        self.region_name = region_name
        self.client = boto3.client('ssm', region_name=self.region_name)

    def get_single(self, parameter_name):
        parameter = self.client.get_parameter(
                            Name=parameter_name
                            # WithDecryption=True|False
                        )

        return parameter['Parameter']['Value']


def get_boto_session(region_name, role_arn, session_name):
    client = boto3.client('sts')
    response = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    credentials = response['Credentials']

    print('STS Token expire on: {0}. Current time: {1}'.format(credentials['Expiration'], datetime.now()))

    return boto3.session.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=region_name
    )


def build_data_set(rows):
    labels = rows[0]
    data_set = []

    for row in rows[1:]:
        record = {}
        for k in labels:
            record[k] = row[labels.index(k)]

        data_set.append(record)

    return data_set


def upload_file(data, new_file, bucket_name):
    temp = '/tmp/tmp-{}.json'.format(datetime.now())

    with open(temp, 'a') as outfile:
        json.dump(data, outfile)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    bucket.delete_objects(
        Delete={
            'Objects': [
                {'Key': new_file},
            ]
        }
    )

    bucket.upload_file(temp, new_file)
    bucket.Object(new_file).Acl().put(ACL='authenticated-read')

    os.remove(temp)

    print ('Uploaded %s/%s' % (bucket_name, new_file))


def download_file(file_name, bucket_name):
    tmp_file = '/tmp/tmp-{}.json'.format(datetime.now())

    s3 = boto3.resource('s3')
    s3.Bucket(bucket_name).download_file(file_name, tmp_file)
    print ('Fetched file: ', file_name)

    with open(tmp_file) as f:
        data = json.load(f)

    os.remove(tmp_file)
    return data


def execute_athena_query(region_name, role_arn, query_string, max_results, athena_db, athena_output_bucket):
    @xray_recorder.capture('athena_polling')
    def get_athena_results(query_id):
        rows = []
        func_start = time.time()
        end_time = func_start + 300

        while time.time() < end_time:
            query_status = athena_client.get_query_execution(
                QueryExecutionId=query_id
            )

            state = query_status['QueryExecution']['Status']['State']
            print('Query state: {}'.format(state))

            if state == 'SUCCEEDED':
                results = athena_client.get_query_results(
                    QueryExecutionId=query_id,
                    MaxResults=max_results
                )

                for row in results['ResultSet']['Rows']:
                    rows.append([v['VarCharValue'] for v in row['Data']])

                print ('Fetched: {}'.format(len(rows) - 1))

                next_token = results.get('NextToken', None)
                while next_token:
                    results = athena_client.get_query_results(
                        QueryExecutionId=query_id,
                        NextToken=next_token,
                        MaxResults=max_results
                    )

                    for row in results['ResultSet']['Rows']:
                        rows.append([v['VarCharValue'] for v in row['Data']])

                    print ('Fetched: {}'.format(len(rows) - 1))

                    next_token = results.get('NextToken', None)

                print('Athena polling took {} seconds'.format(time.time() - func_start))
                break

            time.sleep(2)

        return rows

    athena_session = get_boto_session(
        role_arn=role_arn,
        session_name='athena-lambda-{}'.format(str(time.time())),
        region_name=region_name
    )

    athena_client = athena_session.client('athena')

    # Start the query
    print('Executing query: {}'.format(query_string))

    query = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': athena_db
        },
        ResultConfiguration={
            'OutputLocation': athena_output_bucket
        }
    )

    rows = get_athena_results(query_id=query['QueryExecutionId'])
    print ('Query results: {}'.format(len(rows) - 1))

    return build_data_set(rows)
