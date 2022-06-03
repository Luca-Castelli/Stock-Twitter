from contextlib import contextmanager
from dataclasses import dataclass

import psycopg2


@dataclass
class DBParams:
    db: str
    user: str
    password: str
    host: str
    port: int = 5432


class DBConnection:
    def __init__(self, db_params: DBParams):
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
