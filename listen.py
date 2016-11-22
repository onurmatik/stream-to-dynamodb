import json
from decimal import Decimal
from time import time
from delorean import parse
from settings import *
from birdy.twitter import UserClient, StreamClient
import boto3

rest_client = UserClient(**TWITTER_APP)
stream_client = StreamClient(**TWITTER_APP)

user_ids = []
for twitter_list in LISTS:
    print 'Fetching %s / %s' % twitter_list
    owner_screen_name, slug = twitter_list
    response = rest_client.api.lists.members.get(
        owner_screen_name=owner_screen_name,
        slug=slug,
        count=5000,
        include_entities=False,
        skip_status=True,
    )
    users = [user['id_str'] for user in response.data['users']]
    print 'Fetched %s members' % len(users)
    user_ids += users

user_ids = list(set(user_ids))
print 'Fetched %s unique users' % len(user_ids)

dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=DYNAMODB_ENDPOINT,
)
table = dynamodb.Table(DYNAMODB_TABLE)

resource = stream_client.stream.statuses.filter.post(
    follow=','.join(user_ids),
    stall_warnings=True,
)

for item in resource.stream():
    if 'text' in item:
        data = {
            'user_id': item['user']['id_str'],
            'tweet_id': item['id_str'],
            'timestamp': int(parse(item['created_at']).epoch),
            'text': item['text'],
            'lang': item['lang'],
            'user': {
                'name': item['user']['name'],
                'profile_image_url': item['user']['profile_image_url'],
                'screen_name': item['user']['screen_name'],
            }
        }
        if 'entities' in item:
            if 'hashtags' in item['entities'] and item['entities']['hashtags']:
                data['hashtags'] = [h['text'] for h in item['entities']['hashtags']]
            if 'symbols' in item['entities'] and item['entities']['symbols']:
                data['symbols'] = [s['text'] for s in item['entities']['symbols']]
            if 'urls' in item['entities'] and item['entities']['urls']:
                data['urls'] = [u['expanded_url'] for u in item['entities']['urls']]
            if 'user_mentions' in item['entities'] and item['entities']['user_mentions']:
                data['mentions'] = [m['screen_name'] for m in item['entities']['user_mentions']]
            if 'media' in item['entities'] and item['entities']['media']:
                videos, photos = [], []
                for media in item['entities']['media']:
                    if media['type'] == 'photo':
                        photos.append(media['expanded_url'])
                    elif media['type'] == 'video':
                        videos.append(media['expanded_url'])
                if photos:
                    data['photos'] = photos
                if videos:
                    data['videos'] = videos

        if 'retweeted_status' in item:
            data['retweeted'] = {
                'id': item['retweeted_status']['id_str'],
                'text': item['retweeted_status']['text'],
                'user': {
                    'id': item['retweeted_status']['user']['id_str'],
                    'screen_name': item['retweeted_status']['user']['screen_name'],
                }
            }
            # update original tweets rt / fav counts
            table.update_item(
                Key={
                    'user_id': item['retweeted_status']['user']['id_str'],
                    'tweet_id': item['retweeted_status']['id_str'],
                },
                UpdateExpression='SET rt_count = :retweet_count, fav_count = :favorite_count',
                ExpressionAttributeValues={
                    ':retweet_count': item['retweeted_status']['retweet_count'],
                    ':favorite_count': item['retweeted_status']['favorite_count'],
                }
            )
        if 'coordinates' in item and item['coordinates'] and item['coordinates']['coordinates']:
            data['coordinates'] = [
                Decimal(str(item['coordinates']['coordinates'][0])),
                Decimal(str(item['coordinates']['coordinates'][1])),
            ]
        try:
            table.put_item(
                Item=data,
                #ConditionExpression='attribute_not_exists',
            )
        except Exception, e:
            print item
            raise e
    elif 'delete' in item:
        table.update_item(
            Key={
                'user_id': item['delete']['status']['user_id_str'],
                'tweet_id': item['delete']['status']['id_str'],
            },
            UpdateExpression='SET deleted = :timestamp',
            ExpressionAttributeValues={
                ':timestamp': int(time())
            }
        )
    elif 'warning' in item:
        print item
    elif 'disconnect' in item:
        print item
    elif 'event' in item and item['event'] == 'user_update':
        print item['event']['source']
    elif 'status_withheld' in item:
        print item
    elif 'user_withheld' in item:
        print item
    elif 'limit' in item:
        print item
    elif 'scrub_geo' in item:
        print item
