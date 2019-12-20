import sys
import boto3
import argparse
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')

def main():
    parser = argparse.ArgumentParser(description=
                                     'update evidence states in environment specified.')
    parser.add_argument('-e', '--env', help='Required environment', required=True)
    args = vars(parser.parse_args())

    update_evidence_state(args["env"].lower())


def update_evidence_state(env):
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

    response = table.query(
        IndexName="state-created_date-index",
        KeyConditionExpression=Key('evidence_state').eq('Archived')
    )
    print(len(response['Items']), "records to be processed")
    process_batch(table, response)

    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName="state-created_date-index",
            KeyConditionExpression=Key('evidence_state').eq('Archived'),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        print(len(response['Items']), "records to be processed")
        process_batch(table, response)


def process_batch(table, response):
    items = response['Items']

    for item in items:
        table.update_item(
            Key={"object_key": item.get("object_key"), "created_date_utc": item.get("created_date_utc")},
            UpdateExpression='SET #s = :evidenceState',
            ExpressionAttributeNames={
                '#s': 'evidence_state'
            },
            ExpressionAttributeValues={
                ':evidenceState': 'Acted'
            }
        )
    print(len(response['Items']), "records processed.")

if __name__ == '__main__':
    sys.exit(main())