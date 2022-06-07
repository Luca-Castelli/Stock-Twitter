from json import dumps
from time import sleep

import tweepy
from kafka import KafkaProducer

from utils import config, twitter_interface

PRODUCER = KafkaProducer(bootstrap_servers="kafka:9093")

TOPIC_NAME = "TWEET_STREAM"


def stream_tweets_produce():
    while True:
        # listener = TweetStreamListener()
        # auth = twitter_interface.TwitterConnection(config.get_twitter_creds()).auth
        creds = config.get_twitter_creds()
        stream = TweetStreamListener(
            creds.twitter_api_key,
            creds.twitter_api_secret,
            creds.twitter_access_token,
            creds.twitter_access_secret,
        )
        stream.filter(track=["uranium"], stall_warnings=True, languages=["en"])


class TweetStreamListener(tweepy.Stream):
    def on_data(self, raw_data):
        PRODUCER.send(TOPIC_NAME, raw_data)
        return True


if __name__ == "__main__":
    stream_tweets_produce()
