import psycopg2

from request_price import *


class PostPrice(RequestSplit):
    def __init__(self,
                 start: str,
                 end: str = "TODAY",
                 mkt: str = "KOSPI",
                 login_info: str = "login.json"):
        super().__init__(start, end, mkt, login_info)
        self.login_db_info = "login_DB.json"

    def db_upload(self, login_db_info: str, table_name: str = 'stock_raw'):
        request_split = self.get_split_request()

        with open(login_db_info, 'r') as info:
            login = json.load(info)

        host, dbname, \
            user, pwd = login["host"], login["dbname"], login["user"], login["pwd"]

        conn = psycopg2.connect(
            'host={host} dbname={dbname} user={user} password={pwd}'.format(host=host,
                                                                            dbname=dbname,
                                                                            user=user,
                                                                            pwd=pwd))
        cur = conn.cursor()
        cur.executemany('INSERT INTO {}(ticker, stocksection, date, close_adj) \
            VALUES (%s, %s, %s, %s)'.format(table_name), request_split)

        conn.commit()
        print("========== DB UPLOAD FINISHED ==========")


if __name__ == "__main__":
    start = '20230101'
    post_price = PostPrice(start=start)
    post_price.db_upload(login_db_info='login_DB.json',
                         table_name='stock_raw')
