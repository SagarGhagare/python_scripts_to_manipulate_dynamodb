import sys
import boto3
import argparse
import os
import csv
from botocore.exceptions import ClientError


client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')

def main():
    parser = argparse.ArgumentParser(description=
                                     'delete evidence in environment specified.')
    parser.add_argument('-e', '--env', help='Required environment', required=True)
    parser.add_argument('-c', '--csv', help='Required csv file', required=True)
    args = vars(parser.parse_args())

    delete_evidences(args["env"].lower(), args["csv"].lower())


def delete_evidences(env, evidence_file):
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

    with open(f'{evidence_file}') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                print(f'\tobject_key {row[0]}, and created_date_utc {row[1]}.')
                table.delete_item(
                    Key={
                        'object_key': row[0],
                        'created_date_utc': row[1]
                    }
                )
                line_count += 1
        print(f'Processed {line_count} lines.')


if __name__ == '__main__':
    sys.exit(main())

