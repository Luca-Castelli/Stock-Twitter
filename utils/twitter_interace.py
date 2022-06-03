import re
from datetime import datetime

import tweepy as tw
from textblob import TextBlob

from config import TwitterParams


class TwitterConnection(object):
    def __init__(self, twitter_params: TwitterParams):
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
        """
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        """
        return " ".join(
            re.sub(
                "(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet
            ).split()
        )

    def get_tweet_sentiment(self, tweet):
        """
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        """
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return "positive"
        elif analysis.sentiment.polarity == 0:
            return "neutral"
        else:
            return "negative"

    def get_tweets(self, query: str, count: int = 1000, until: str = None):
        """
        Main function to fetch tweets and parse them.
        """
        # empty list to store parsed tweets
        tweets_data = []

        try:
            # call twitter api to fetch tweets
            if until:
                fetched_tweets = tw.Cursor(
                    self.api.search_tweets, q=query, lang="en", until=until
                ).items(count)
            else:
                fetched_tweets = tw.Cursor(
                    self.api.search_tweets, q=query, lang="en"
                ).items(count)

            # parsing tweets one by one
            for tweet in fetched_tweets:
                single_tweet_data = {
                    "twitter_id": tweet.id,
                    "username": tweet.user.name,
                    "text": tweet.text,
                    "created_at": datetime.strftime(tweet.created_at, "%Y%m%d"),
                    "sentiment": self.get_tweet_sentiment(tweet.text),
                }

                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if single_tweet_data not in tweets_data:
                        tweets_data.append(single_tweet_data)
                else:
                    tweets_data.append(single_tweet_data)

            # return parsed tweets
            return tweets_data

        except tw.TweepError as e:
            # print error (if any)
            print("Error : " + str(e))
