import pandas as pd
import psycopg2
import psycopg2.extras as extras

from db_config import get_db_creds
from db_connection import DBConnection


def execute_query(query):
    with DBConnection(get_db_creds()).managed_cursor() as curr:
        curr.execute(query)
    print("execute_query() done")


def execute_df_upsert(df, key, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ",".join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT(%s) DO NOTHING" % (
        table,
        cols,
        key,
    )
    with DBConnection(get_db_creds()).managed_cursor() as curr:
        extras.execute_values(curr, query, tuples)
        print("execute_df_upsert() done")
