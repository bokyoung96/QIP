# env not specified

import json
import psycopg2


class DBLogin:
    def __init__(self):
        self.login_db_info = "login_DB.json"

    def db_connect(self):
        """
        Connect PostgreSQL DB.
        """
        with open(self.login_db_info, 'r') as info:
            login = json.load(info)

        host, dbname, \
            user, pwd = login["host"], login["dbname"], login["user"], login["pwd"]

        conn = psycopg2.connect(
            'host={host} dbname={dbname} user={user} password={pwd}'.format(host=host,
                                                                            dbname=dbname,
                                                                            user=user,
                                                                            pwd=pwd))
        cur = conn.cursor()
        return conn, cur

    def db_disconnect(self):
        """
        Disconnect PostgreSQL DB.
        """
        conn = self.db_connect()[0]
        if conn.closed == 0:
            print("EXECUTE DISCONNECTION")
            conn.close()
