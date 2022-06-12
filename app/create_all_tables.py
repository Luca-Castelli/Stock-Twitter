from utils import config, db_interface


def init_stock_data_table() -> None:
    """Initialize stock and stock_price tables in the batch DB."""

    query_delete_stock_stock_price = """
        DROP TABLE IF EXISTS stock, stock_price;
        """
    query_create_stock = """
        CREATE TABLE stock(
            ticker TEXT,
            name TEXT,
            PRIMARY KEY(ticker)
        );
        """
    query_create_stock_price = """
        CREATE TABLE stock_price(
            ticker TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            price NUMERIC(5,2),
            PRIMARY KEY(ticker, timestamp),
            CONSTRAINT fk_stock
                FOREIGN KEY(ticker)
                REFERENCES stock(ticker)

            );
        """
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        curr.execute(query_delete_stock_stock_price)
        curr.execute(query_create_stock)
        curr.execute(query_create_stock_price)


def init_twitter_data_table() -> None:
    """Initialize tweet table in the batch DB."""

    query_delete_tweet = """
        DROP TABLE IF EXISTS tweet;
        """
    query_create_tweet = """
        CREATE TABLE tweet(
            id SERIAL,
            twitter_id BIGSERIAL UNIQUE,
            username TEXT,
            text TEXT,
            created_at TIMESTAMP,
            sentiment TEXT,
            PRIMARY KEY(id)
            );
        """
    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        curr.execute(query_delete_tweet)
        curr.execute(query_create_tweet)


def init_twitter_streaming_table() -> None:
    """Initialize tweet table in the stream DB."""

    query_delete_tweet_stream = """
        DROP TABLE IF EXISTS tweet_stream;
        """
    query_create_tweet_stream = """
        CREATE TABLE tweet_stream(
            id SERIAL,
            twitter_id BIGSERIAL UNIQUE,
            username TEXT,
            text TEXT,
            created_at TIMESTAMP,
            verified_user BOOLEAN,
            followers INTEGER,
            sentiment TEXT,
            PRIMARY KEY(id)
            );
        """
    with db_interface.DBConnection(config.get_stream_creds()).managed_cursor() as curr:
        curr.execute(query_delete_tweet_stream)
        curr.execute(query_create_tweet_stream)


if __name__ == "__main__":
    # init_stock_data_table()
    # init_twitter_data_table()
    init_twitter_streaming_table()
