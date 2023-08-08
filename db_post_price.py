# 32bit env required

from request_price import *
from db_login import *


class DBPostPrice(RequestSplit, DBLogin):
    def __init__(self,
                 start: str,
                 end: str = "TODAY",
                 mkt: str = "KOSPI",
                 login_info: str = "login.json"):
        super().__init__(start, end, mkt, login_info)
        self.conn, self.cur = self.db_connect()

    def db_post_price(self, table_name: str = 'stock_raw'):
        request_split = self.get_split_request()

        self.cur.executemany('INSERT INTO {}(ticker, stocksection, date, close_adj) \
            VALUES (%s, %s, %s, %s)'.format(table_name), request_split)

        self.conn.commit()
        print("========== DB UPLOAD FINISHED ==========")
        self.db_disconnect()


if __name__ == "__main__":
    start = '20000101'
    post_price = DBPostPrice(start=start)
    post_price.db_post_price(table_name='stock_raw')
