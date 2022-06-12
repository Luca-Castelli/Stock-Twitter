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


def stream_tweets_consume() -> None:
    """Consumer for the TWEET_STREAM Kafka topic. Loads consumed tweets into the stream DB."""

    for message in CONSUMER:

        tweet = json.loads(json.dumps(message.value))

        if tweet["retweeted"] or "RT @" in tweet["text"]:
            continue

        if tweet["truncated"]:
            text = tweet["extended_tweet"]["full_text"]
        else:
            text = tweet["text"]

        single_tweet_data = [
            {
                "twitter_id": tweet["id"],
                "username": tweet["user"]["screen_name"],
                "text": text,
                "created_at": tweet["created_at"],
                "verified_user": tweet["user"]["verified"],
                "followers": tweet["user"]["followers_count"],
            }
        ]

        print(f"CONSUMER: {single_tweet_data}\n")

        with db_interface.DBConnection(
            config.get_stream_creds()
        ).managed_cursor() as curr:
            db_interface.execute_json_upsert(
                json_data=single_tweet_data,
                constraint_key="twitter_id",
                table_name="tweet_stream",
                curr=curr,
            )


if __name__ == "__main__":
    stream_tweets_consume()
