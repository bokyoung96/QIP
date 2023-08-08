# 64bit env required

import pandas.io.sql as psql

from enum import Enum, unique
from db_login import *


@unique
class SQLQuery(Enum):
    SELECT = "SELECT * FROM"
    INSERT = "INSERT INTO"


class DBGetPrice(DBLogin):
    def __init__(self):
        super().__init__()
        self.table_name = "stock_raw"
        self.conn, self.cur = self.db_connect()
        self.query_select = SQLQuery.SELECT.value + \
            " {}".format(self.table_name)

    def db_get_raw_price(self):
        self.cur.execute(self.query_select)
        data = self.cur.fetchall()
        return data

    def db_get_df_price(self):
        df = psql.read_sql(self.query_select, self.conn)
        return df
