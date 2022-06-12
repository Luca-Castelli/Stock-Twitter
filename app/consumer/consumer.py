import json
from datetime import datetime

from kafka import KafkaConsumer

from utils import config, db_interface

TOPIC_NAME = "TWEET_STREAM"

CONSUMER = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=["kafka:9093"],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=128,
    max_poll_records=100,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def stream_tweets_consume():
    for message in CONSUMER:

        tweet = json.loads(json.dumps(message.value))

        single_tweet_data = [
            {
                "twitter_id": tweet["id"],
                "username": tweet["user"]["screen_name"],
                "text": tweet["text"],
                "created_at": tweet["created_at"],
            }
        ]
        print(single_tweet_data)
        with db_interface.DBConnection(
            config.get_batch_creds()
        ).managed_cursor() as curr:
            db_interface.execute_json_upsert(
                single_tweet_data, "twitter_id", "tweet_stream", curr
            )


if __name__ == "__main__":
    stream_tweets_consume()
