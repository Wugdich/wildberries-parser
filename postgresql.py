from typing import Union

import psycopg2
from psycopg2.extras import execute_values
import config


class Database:

    def __init__(self):
        self._database = config.PG_DATABASE
        self._user = config.PG_USER
        self._pass = config.PG_PASS
        self._host = config.PG_HOST
        self._port = config.PG_PORT
        self._conn = None
        self._cur = None

    def connect(self):
        # Connect to an existing database
        self._conn = psycopg2.connect(dbname=self._database,
                                      user=self._user,
                                      password=self._pass,
                                      host=self._host,
                                      port=self._port)

        # Open a cursor to perform database operations
        self._cur = self._conn.cursor()

        # Create tables.
        self._create_table_products()

    def _create_table_products(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS products (
        id BIGSERIAL PRIMARY KEY,
        ctgr VARCHAR(40),
        ctgr_id INTEGER,
        subject VARCHAR(40),
        subject_id INTEGER,
        brand_name VARCHAR(40),
        brand_id INTEGER,
        article_number INTEGER,
        product_name VARCHAR(40),
        base_price INTEGER,
        sale_price INTEGER,
        average_price INTEGER,
        sales_amount INTEGER,
        rating SMALLINT,
        feedback_count INTEGER,
        date DATE,
        CONSTRAINT unique_product_data UNIQUE (article_number, date)
        )
        """
        self.execute(query)

    def execute(self, query: str, values: Union[list, tuple]=None) -> None:
        if values and len(values) > 1 and query.count('%s') == 1:
            execute_values(self._cur, query, values)
        else:
            self._cur.execute(query, values)

    def close(self) -> None:
        # Make the changes to the database persistent.
        self._conn.commit()

        # Close communication with the database.
        self._cur.close()
        self._conn.close()

