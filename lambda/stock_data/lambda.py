import pandas as pd
import yfinance as yf
from config import get_oltp_creds
from db_interface import DBConnection, execute_df_upsert


def lambda_handler(event, context):

    ticker = "URA"
    name = "Uranium ETF"

    query_insert = f"INSERT INTO stock(ticker, name) VALUES('{ticker}', '{name}') ON CONFLICT(ticker) DO NOTHING;"
    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        curr.execute(query_insert)

    stock_price_data = get_stock_prices(ticker)
    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        execute_df_upsert(
            df=stock_price_data,
            table_name="stock_price",
            constraint_key="ticker, timestamp",
            curr=curr,
        )

    return {"Status": "Success!"}


def get_stock_prices(ticker: str) -> pd.DataFrame:

    raw_data = yf.download(tickers=ticker, period="10d", interval="1d")
    data = pd.DataFrame(columns=["ticker", "timestamp", "price"])
    data["timestamp"] = raw_data.index
    data["price"] = raw_data["Close"].values
    data["ticker"] = ticker
    return data


# sam local invoke -e ./stock_data/lambda_event.json StockDataFunction
