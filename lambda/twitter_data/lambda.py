import json
from datetime import datetime

import boto3
from config import get_oltp_creds, get_twitter_creds
from db_interface import DBConnection, execute_json_upsert
from twitter_interface import TwitterConnection


def lambda_handler(event, context):

    query = "uranium"
    count = 1000

    twitter = TwitterConnection(get_twitter_creds())
    tweets = twitter.get_tweets(query=query, count=count)
    processed_tweets = tweets[0]
    raw_tweets = tweets[1]

    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        execute_json_upsert(
            json_data=processed_tweets,
            table_name="tweet",
            constraint_key="twitter_id",
            curr=curr,
        )

    s3_bucket = "stock-twitter-s3"
    s3_key = datetime.now().strftime("%Y%m%d") + "_raw_tweets.json"
    data = json.dumps(raw_tweets)
    upload_to_S3(s3_bucket, s3_key, data)

    return {"Status": "Success!"}


def upload_to_S3(bucket: str, key: str, data: json) -> None:
    s3_client = boto3.client("s3")
    s3_client.put_object(Body=data, Bucket=bucket, Key=key)


# sam local invoke -e ./twitter_data/lambda_event.json TwitterDataFunction
