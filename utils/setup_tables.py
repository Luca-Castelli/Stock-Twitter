from db_interface import execute_query


def init_stock_data_table():
    """
    temp
    """
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
    execute_query(query_delete_stock_stock_price)
    execute_query(query_create_stock)
    execute_query(query_create_stock_price)


def init_twitter_data_table():
    """
    temp
    """
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
    execute_query(query_delete_tweet)
    execute_query(query_create_tweet)


if __name__ == "__main__":
    init_stock_data_table()
    init_twitter_data_table()
