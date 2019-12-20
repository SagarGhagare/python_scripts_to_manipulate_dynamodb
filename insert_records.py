import uuid
import boto3
from datetime import datetime


def setup_data():
    dynamodb = boto3.resource('dynamodb', region_name="eu-west-2")
    table = dynamodb.Table('cid-dev9-pyi-verification-evidence-table')

    for x in range(15000):
        unique_id = str(uuid.uuid4())
        unique_id_1 = str(uuid.uuid4())
        unique_id_2 = str(uuid.uuid4())
        print(unique_id)
        dt_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        object_key_value = "submitted"+"/"+unique_id+"/"+unique_id+"_"+unique_id_1+"_"+unique_id_2+"_video"
        print(object_key_value)
        table.put_item(
            Item={
                  "account_id": unique_id,
                  "bucket_name": "dev9-pyi-verification-evidence-bucket",
                  "created_date_utc": dt_string,
                  "evidence_state": "Archived",
                  "instance_id": unique_id_1,
                  "last_modified_date_utc": dt_string,
                  "object_key": object_key_value
                }
        )


if __name__ == '__main__':
    setup_data()