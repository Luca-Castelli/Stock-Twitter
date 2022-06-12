import tweepy
from kafka import KafkaProducer

from utils import config

PRODUCER = KafkaProducer(bootstrap_servers="kafka:9093")

TOPIC_NAME = "TWEET_STREAM"

QUERY = "uranium"


def stream_tweets_produce() -> None:
    """Producer for the TWEET_STREAM Kafka topic. Listens to TweetStreamListener."""

    while True:
        twitter_creds = config.get_twitter_creds()
        stream = TweetStreamListener(
            twitter_creds.twitter_api_key,
            twitter_creds.twitter_api_secret,
            twitter_creds.twitter_access_token,
            twitter_creds.twitter_access_secret,
        )
        stream.filter(track=[QUERY], stall_warnings=True, languages=["en"])


class TweetStreamListener(tweepy.Stream):
    """Twitter API to get and filter realtime tweets."""

    def on_data(self, data):
        """This is called when raw data is received from the stream. Sends data to the Kafka topic."""

        print(f"PRODUCER: {data}\n")

        PRODUCER.send(TOPIC_NAME, data)
        return True


if __name__ == "__main__":
    stream_tweets_produce()
