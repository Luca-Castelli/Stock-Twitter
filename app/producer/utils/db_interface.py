from contextlib import contextmanager
from typing import Any, Dict, List

import pandas as pd
import psycopg2
import psycopg2.extras as extras

from utils.config import DbParams


class DBConnection:
    def __init__(self, db_params: DbParams):

        self.conn_url = (
            f"postgresql://{db_params.user}:{db_params.password}@"
            f"{db_params.host}:{db_params.port}/{db_params.db}"
        )

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()


def execute_df_upsert(
    df: pd.DataFrame,
    constraint_key: str,
    table_name: str,
    curr: Any,
) -> None:
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ",".join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT(%s) DO NOTHING" % (
        table_name,
        cols,
        constraint_key,
    )
    extras.execute_values(curr, query, tuples)


def execute_json_upsert(
    json_data: List[Dict],
    constraint_key: str,
    table_name: str,
    curr: Any,
) -> None:

    # Create a list of tupples from the json object
    tuples = [tuple(x.values()) for x in json_data]
    # Comma-separated json object keys
    cols = ",".join(json_data[0].keys())
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT(%s) DO NOTHING" % (
        table_name,
        cols,
        constraint_key,
    )
    extras.execute_values(curr, query, tuples)
