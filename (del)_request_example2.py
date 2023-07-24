import time
import datetime as dt
import pandas as pd

from ticker import *

DATA = ['Ticker', 'StockSection', 'Date',
        'Open', 'High', 'Low', 'Close', 'Vol']
ROW = list(range(len(DATA)))
ROWS = []


class RequestDataAll(Ticker):
    def __init__(self,
                 start: str,
                 end: str = "TODAY",
                 mkt: str = "KOSPI",
                 login_info: str = "login.json"):
        super().__init__(mkt, login_info)
        if not self.config_connect:
            self.process_connect(login_info=login_info)

        self.start = start
        if end == 'TODAY':
            self.end = (dt.datetime.today() -
                        dt.timedelta(days=1)).strftime("%Y%m%d")
        else:
            self.end = end
        self.obj = self.objStockChart
        self.ticker = self.get_ticker_info[:3]  # NOTE: For test (3)

    @property
    def config_request(self):
        self.obj.BlockRequest()

        rqStatus = self.obj.GetDibStatus()
        rqRet = self.obj.GetDibMsg1()
        print("Request connection: ", rqStatus, rqRet)

        if rqStatus != 0:
            print("ERROR: REQUEST CONNECTION.")
            exit()

    @property
    def set_input_value(self):
        self.obj.SetInputValue(1, ord('1'))
        self.obj.SetInputValue(2, self.end)
        self.obj.SetInputValue(3, self.start)
        self.obj.SetInputValue(5, (0, 2, 3, 4, 5, 8))
        self.obj.SetInputValue(6, ord('D'))
        self.obj.SetInputValue(9, ord('1'))

    def get_reqeust(self):
        # SET INPUT VALUES
        self.set_input_value

        # BLOCK_REQUEST FOR REQUEST
        for idx, item in self.ticker.iterrows():
            self.obj.SetInputValue(0, item['Ticker'])
            self.config_request

            # GET HEADER VALUES: TOTAL DATES REQUESTED
            numDate = self.obj.GetHeaderValue(3)

            for date in range(numDate):
                ROW[0] = item['Ticker']
                ROW[1] = item['StockSection']
                ROW[2] = self.obj.GetDataValue(0, date)
                ROW[3] = self.obj.GetDataValue(1, date)
                ROW[4] = self.obj.GetDataValue(2, date)
                ROW[5] = self.obj.GetDataValue(3, date)
                ROW[6] = self.obj.GetDataValue(4, date)
                ROW[7] = self.obj.GetDataValue(5, date)
                ROWS.append(list(ROW))
        return ROWS


if __name__ == "__main__":
    request_data_all = RequestDataAll(start='20230701')
    res = request_data_all.get_reqeust()
    print(res)
