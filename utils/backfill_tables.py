import pandas as pd
import psycopg2
import psycopg2.extras as extras
import yfinance as yf

from db_interface import execute_df_upsert, execute_query


def backfill_stock_data(ticker: str, name: str) -> None:

    query_insert = f"INSERT INTO stock(ticker, name) VALUES('{ticker}', '{name}') ON CONFLICT(ticker) DO NOTHING;"
    execute_query(query_insert)

    stock_price_data = get_stock_prices(ticker)
    execute_df_upsert(stock_price_data, "ticker, timestamp", "stock_price")


def get_stock_prices(ticker: str) -> pd.DataFrame:
    raw_data = yf.download(tickers=ticker, period="1y", interval="1d")
    data = pd.DataFrame(columns=["ticker", "timestamp", "price"])
    data["timestamp"] = raw_data.index
    data["price"] = raw_data["Close"].values
    data["ticker"] = ticker
    return data


def backfill_twitter_data():
    pass


if __name__ == "__main__":
    backfill_stock_data("URA", "Uranium ETF")
