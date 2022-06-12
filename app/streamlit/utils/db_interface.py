from contextlib import contextmanager
from typing import Any, Dict, List

import pandas as pd
import psycopg2
import psycopg2.extras as extras

import utils.config as config


class DBConnection:
    """Object to handle a connection to a DB. Must pass in a DbParams object when initializing."""

    def __init__(self, db_params: config.DbParams):
        self.conn_url = (
            f"postgresql://{db_params.user}:{db_params.password}@"
            f"{db_params.host}:{db_params.port}/{db_params.db}"
        )

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        """Method returns DB cursor."""
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()


def execute_df_upsert(
    df: pd.DataFrame, constraint_key: str, table_name: str, curr: Any
) -> None:
    """Upserts a dataframe into table_name belonging to curr's DB. If constraint_key already exists, do nothing."""

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))

    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT(%s) DO NOTHING" % (
        table_name,
        cols,
        constraint_key,
    )

    extras.execute_values(curr, query, tuples)


def execute_json_upsert(
    json_data: List[Dict], constraint_key: str, table_name: str, curr: Any
) -> None:
    """Upsert a JSON object into table_name belonging to curr's DB. If constraint_key already exists, do nothing."""

    tuples = [tuple(x.values()) for x in json_data]
    cols = ",".join(json_data[0].keys())

    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT(%s) DO NOTHING" % (
        table_name,
        cols,
        constraint_key,
    )

    extras.execute_values(curr, query, tuples)
