import json
from datetime import datetime, timedelta

import boto3
import pandas as pd
import yfinance as yf

from utils import config, db_interface, twitter_interface


def backfill_stock_data(ticker: str, name: str) -> None:
    """Backfill stock and stock_price tables in the batch DB going back 1y."""

    query_insert = f"INSERT INTO stock(ticker, name) VALUES('{ticker}', '{name}') ON CONFLICT(ticker) DO NOTHING;"
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        curr.execute(query_insert)

    stock_price_data = get_stock_prices(ticker)
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        db_interface.execute_df_upsert(
            df=stock_price_data,
            table_name="stock_price",
            constraint_key="ticker, timestamp",
            curr=curr,
        )


def get_stock_prices(ticker: str) -> pd.DataFrame:
    """Fetch stock prices from Yahoo Finance for a period of 1y and interval of 1d."""

    raw_data = yf.download(tickers=ticker, period="1y", interval="1d")
    data = pd.DataFrame(columns=["ticker", "timestamp", "price"])
    data["timestamp"] = raw_data.index
    data["price"] = raw_data["Close"].values
    data["ticker"] = ticker

    return data


def backfill_twitter_data(query: str, count: int) -> None:
    """Backfill tweet table in the batch DB and raw tweets into S3 going back 7 days (Twitter API limit)."""

    twitter = twitter_interface.TwitterConnection(config.get_twitter_creds())

    processed_tweets = []
    # Free version of the Twitter search API can only go back 7 days.
    for i in range(7, -1, -1):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        tweets = twitter.get_tweets(query=query, count=count, until=date)
        processed_tweets += tweets[0]

        # Insert raw tweets into data lake (hosted using AWS S3).
        s3_bucket = "stock-twitter-s3"
        s3_key = date + "_raw_tweets.json"
        data = json.dumps(tweets[1])
        upload_to_S3(s3_bucket, s3_key, data)

    # Insert processed tweets into the batch DB.
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        db_interface.execute_json_upsert(
            json_data=processed_tweets,
            table_name="tweet",
            constraint_key="twitter_id",
            curr=curr,
        )


def upload_to_S3(bucket: str, key: str, data: json) -> None:
    """Upload data to S3."""

    s3_client = boto3.client("s3")
    s3_client.put_object(Body=data, Bucket=bucket, Key=key)


if __name__ == "__main__":
    backfill_stock_data(ticker="URA", name="Uranium ETF")
    backfill_twitter_data(query="uranium", count=100)
