import sys
import boto3
import argparse
from botocore.exceptions import ClientError


client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')


def main():
    parser = argparse.ArgumentParser(description=
                                     'update evidence bucket names in environment specified.')
    parser.add_argument('-e', '--env', help='Required environment', required=True)
    args = vars(parser.parse_args())

    update_bucket_names(args["env"].lower())


def update_bucket_names(env):
    table_name = f'cid-{env}-pyi-verification-evidence-table'
    table = dynamodb.Table(table_name)

    try:
        client.describe_table(TableName=table_name)

    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ResourceNotFoundException':
            print("Table " + table_name + " does not exist.")
        else:
            print("Unknown exception occurred while querying for the " + table_name + " table. Printing full error:")
            pprint.pprint(ce.response)

    response = table.scan(
        ProjectionExpression='#k,#s',
        ExpressionAttributeNames={
            '#k': 'object_key',  # partition key
            '#s': 'created_date_utc'  # sort key
        }
    )
    print(len(response['Items']), "records to be processed")
    process_batch(table, response, env)

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression='#k,#s',
            ExpressionAttributeNames={
                '#k': 'object_key',  # partition key
                '#s': 'created_date_utc'  # sort key
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        print(len(response['Items']), "records to be processed")
        process_batch(table, response, env)


def process_batch(table, response, env):
    items = response['Items']

    for item in items:
        table.update_item(
            Key=item,
            UpdateExpression='SET #b = :bucketName',
            ExpressionAttributeNames={
                '#b': 'bucket_name'
            },
            ExpressionAttributeValues={
                ':bucketName': f'cid-{env}-pyi-verification-evidence-bucket'
            }
        )
    print(len(response['Items']), "records processed.")

if __name__ == '__main__':
    sys.exit(main())