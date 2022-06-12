from contextlib import contextmanager
from typing import Any, Dict, List

import pandas as pd
import psycopg2
import psycopg2.extras as extras

import utils.config as config


class DBConnection:
    """To faciliate DB connection."""

    def __init__(self, db_params: config.DbParams):
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
    """Upsert dataframe into table_name within database. If constraint_key already exists, do nothing."""

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))
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
    """Upsert JSON object into table_name within database. If constraint_key already exists, do nothing."""

    tuples = [tuple(x.values()) for x in json_data]
    cols = ",".join(json_data[0].keys())
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT(%s) DO NOTHING" % (
        table_name,
        cols,
        constraint_key,
    )
    extras.execute_values(curr, query, tuples)
