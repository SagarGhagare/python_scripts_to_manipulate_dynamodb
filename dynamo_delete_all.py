import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('cid-dev9-pyi-verification-evidence-table')

scan = table.scan(
        ProjectionExpression='#k,#s',
        ExpressionAttributeNames={
            '#k': 'object_key',  # partition key
            '#s': 'created_date_utc'  # sort key
        }
    )
print(len(scan['Items']), "records to be processed")

with table.batch_writer() as batch:
    for each in scan['Items']:
        batch.delete_item(Key=each)
print(len(scan['Items']), "records processed.")

while 'LastEvaluatedKey' in scan:
    scan = table.scan(
            ProjectionExpression='#k,#s',
            ExpressionAttributeNames={
                 '#k': 'object_key',  # partition key
                 '#s': 'created_date_utc'  # sort key
            },
            ExclusiveStartKey=scan['LastEvaluatedKey']
    )
    print(len(scan['Items']), "records to be processed")
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key=each)
    print(len(scan['Items']), "records processed.")