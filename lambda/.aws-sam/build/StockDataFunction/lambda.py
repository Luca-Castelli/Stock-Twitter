import config
import db_interface
import pandas as pd
import yfinance as yf


def lambda_handler(event, context):
    """Connect to Yahoo API and fetch stock prices for a given ticker. Stock price written to batch DB."""

    ticker = "URA"
    name = "Uranium ETF"

    # Utilize DBConnection to connect to batch DB and insert into the stock table.
    query_insert = f"INSERT INTO stock(ticker, name) VALUES('{ticker}', '{name}') ON CONFLICT(ticker) DO NOTHING;"
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        curr.execute(query_insert)

    stock_price_data = get_stock_prices(ticker)
    # Utilize DBConnection to connect to batch DB and insert into the stock_price table.
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        db_interface.execute_df_upsert(
            df=stock_price_data,
            table_name="stock_price",
            constraint_key="ticker, timestamp",
            curr=curr,
        )

    return {"Status": "Success!"}


def get_stock_prices(ticker: str) -> pd.DataFrame:
    """Fetch stock prices from Yahoo finance for a period of 10d and interval of 1d."""

    raw_data = yf.download(tickers=ticker, period="10d", interval="1d")
    data = pd.DataFrame(columns=["ticker", "timestamp", "price"])
    data["timestamp"] = raw_data.index
    data["price"] = raw_data["Close"].values
    data["ticker"] = ticker

    return data


# sam local invoke -e ./stock_data/lambda_event.json StockDataFunction
