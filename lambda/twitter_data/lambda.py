import json
from datetime import datetime

import boto3
import config
import db_interface
import twitter_interface


def lambda_handler(event, context):
    """Connect to Twitter API and fetch tweets for given query/keyword string. Processed tweets written to batch DB. Raw tweets stored in S3."""

    query = "uranium"
    count = 1000

    # Utilize TwitterConnection to connect to Twitter and fetch tweets for query keyword.
    twitter = twitter_interface.TwitterConnection(config.get_twitter_creds())
    tweets = twitter.get_tweets(query=query, count=count)
    processed_tweets = tweets[0]
    raw_tweets = tweets[1]

    # Utilize DBConnection to connect to batch DB and upsert processed tweets into the tweet table.
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        db_interface.execute_json_upsert(
            json_data=processed_tweets,
            table_name="tweet",
            constraint_key="twitter_id",
            curr=curr,
        )

    # Connect to S3 and store raw tweets.
    s3_bucket = "stock-twitter-s3"
    s3_key = datetime.now().strftime("%Y%m%d") + "_raw_tweets.json"
    data = json.dumps(raw_tweets)
    upload_to_S3(s3_bucket, s3_key, data)

    return {"Status": "Success!"}


def upload_to_S3(bucket: str, key: str, data: json) -> None:
    """Upload data to S3."""

    s3_client = boto3.client("s3")
    s3_client.put_object(Body=data, Bucket=bucket, Key=key)


# sam local invoke -e ./twitter_data/lambda_event.json TwitterDataFunction
