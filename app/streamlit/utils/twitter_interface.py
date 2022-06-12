import re
from datetime import datetime

import tweepy as tw
from textblob import TextBlob

import utils.config as config


class TwitterConnection(object):
    """To facilitate Twitter connection"""

    def __init__(self, twitter_params: config.TwitterParams):
        API_KEY = twitter_params.twitter_api_key
        API_SECRET = twitter_params.twitter_api_secret
        TOKEN = twitter_params.twitter_access_token
        TOKEN_SECRET = twitter_params.twitter_access_secret

        try:
            self.auth = tw.OAuthHandler(API_KEY, API_SECRET)
            self.auth.set_access_token(TOKEN, TOKEN_SECRET)
            self.api = tw.API(self.auth, wait_on_rate_limit=True)
        except:
            print("Error: Auth failed")

    def clean_tweet(self, tweet):
        """Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        """
        return " ".join(
            re.sub(
                "(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet
            ).split()
        )

    def get_tweet_sentiment(self, tweet):
        """Utility function to classify sentiment of passed tweet
        using textblob's sentiment method.
        """
        analysis = TextBlob(self.clean_tweet(tweet))
        if analysis.sentiment.polarity > 0:
            return "positive"
        elif analysis.sentiment.polarity == 0:
            return "neutral"
        else:
            return "negative"

    def get_tweets(self, query: str, count: int = 1000, until: str = None):
        """Main function to fetch tweets and process them. Returns processed teweets and raw tweets."""
        tweets_data = []
        raw_tweets = []

        try:
            if until:
                fetched_tweets = tw.Cursor(
                    self.api.search_tweets, q=query, lang="en", until=until
                ).items(count)
            else:
                fetched_tweets = tw.Cursor(
                    self.api.search_tweets, q=query, lang="en"
                ).items(count)

            for tweet in fetched_tweets:
                single_tweet_data = {
                    "twitter_id": tweet.id,
                    "username": tweet.user.name,
                    "text": tweet.text,
                    "created_at": datetime.strftime(tweet.created_at, "%Y%m%d"),
                    "sentiment": self.get_tweet_sentiment(tweet.text),
                }

                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once.
                    if single_tweet_data not in tweets_data:
                        tweets_data.append(single_tweet_data)
                else:
                    tweets_data.append(single_tweet_data)

                raw_tweets.append(tweet._json)

            return (tweets_data, raw_tweets)

        except tw.TweepError as e:
            print("Error : " + str(e))
