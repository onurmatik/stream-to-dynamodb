from time import time
import json
from settings import *
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=DYNAMODB_ENDPOINT,
)

table = dynamodb.Table(DYNAMODB_TABLE)

#response = table.query(
#    KeyConditionExpression=Key('user_id').eq('270483276')
#)

#response = table.scan(
#    FilterExpression=Attr('lang').eq('tr')
#)

table.update_item(
    Key={
        'user_id': '123',
        'tweet_id': '321',
    },
    UpdateExpression='SET deleted = :timestamp',
    ExpressionAttributeValues={
        ':timestamp': int(time())
    }
)


"""
response = table.get_item(
    Key={
        'timestamp': 1479768591,
    }
)
item = response['Item']
print(item)
"""
