import time
import datetime as dt
import numpy as np
import pandas as pd

from itertools import chain

from preprocess import *
from ticker import *

DATA = ['Ticker', 'StockSection', 'Date', 'Close']


class RequestPrice(Ticker):
    def __init__(self,
                 start: str,
                 end: str = "TODAY",
                 mkt: str = "KOSPI",
                 login_info: str = "login.json"):
        super().__init__(mkt, login_info)

        self.start = start
        if end == 'TODAY':
            self.end = (dt.datetime.today() -
                        dt.timedelta(days=1)).strftime("%Y%m%d")
        else:
            self.end = end
        self.preprocess = PreProcess(self.start, self.end)

        self.obj = self.objStockChart

        self.ticker = self.get_ticker_info
        self.ROW = list(range(len(DATA)))
        self.res = None

    @property
    def config_request(self):
        """
        Configure request API server.
        """
        self.obj.BlockRequest()

        rqStatus = self.obj.GetDibStatus()
        if rqStatus != 0:
            print("ERROR: REQUEST CONNECTION.")
            exit()

    @property
    def get_input_value(self):
        """
        Set input values in order to request datas from API.
        """
        self.obj.SetInputValue(1, ord('1'))
        self.obj.SetInputValue(2, self.end)
        self.obj.SetInputValue(3, self.start)
        self.obj.SetInputValue(5, (0, 5))
        self.obj.SetInputValue(6, ord('D'))
        self.obj.SetInputValue(9, ord('1'))

    def get_request_count(self, item: pd.DataFrame):
        """
        Count limits(60 in 15sec) for requesting.
        """
        remain_request_count = self.obj_CpUtil_CpCybos.GetLimitRemainCount(1)
        print(item['Ticker'], item['Name'],
              'Remaining Requests: ', remain_request_count)

        if remain_request_count == 0:
            print('Remaining requests are empty.')
            while True:
                time.sleep(1)
                remain_request_count = self.obj_CpUtil_CpCybos.GetLimitRemainCount(
                    1)
                if remain_request_count > 0:
                    print('Requests re-filled. (Left: {})'.format(remain_request_count))
                    break
                print('Waiting...')

    @property
    def get_request(self) -> list:
        """
        Request data from API in order.
        """
        ROWS = []
        self.get_input_value
        for idx, item in self.ticker.iterrows():
            self.get_request_count(item=item)
            self.obj.SetInputValue(0, item['Ticker'])
            self.config_request

            numDate = self.obj.GetHeaderValue(3)
            for num in range(numDate):
                self.ROW[0] = item['Ticker']
                self.ROW[1] = item['StockSection']
                self.ROW[2] = self.obj.GetDataValue(0, num)
                self.ROW[3] = self.obj.GetDataValue(1, num)
                ROWS.append(list(self.ROW))
        return ROWS

    @classmethod
    def pp_dt_storage(cls, start, end):
        """
        Get datetime for class <RequestSplit> get_split_price.
        """
        return cls(start, end)

    def get_price(self) -> pd.DataFrame:
        """
        Get time-series price data.
        """
        df = pd.DataFrame(self.get_request, columns=DATA)
        dates = self.preprocess.pp_extract_dates(df)
        temp = self.preprocess.pp_append_nans(df)
        self.res = pd.DataFrame(temp['Close'].tolist(),
                                index=temp['Ticker'],
                                columns=dates).T[::-1]
        return self.res


class RequestSplit(RequestPrice):
    def __init__(self,
                 start: str,
                 end: str = "TODAY",
                 mkt: str = "KOSPI",
                 login_info: str = "login.json"):
        super().__init__(start, end, mkt, login_info)
        self.time_diff = self.preprocess.pp_time_diff()

    def get_split_request(self) -> list:
        """
        Get request data for SQL.
        """
        temp = []
        for start, end in self.time_diff:
            request_price = RequestPrice.pp_dt_storage(start, end)
            temp.append(request_price.get_request)
        res = list(chain(*temp))
        return res

    def get_split_price(self) -> pd.DataFrame:
        """
        Get time-series price data.
        """
        temp = []
        for start, end in self.time_diff:
            request_price = RequestPrice.pp_dt_storage(start, end)
            temp.append(request_price.get_price())

        res = pd.concat(temp)
        res.index = pd.to_datetime(res.index, format="%Y%m%d")
        return res


# TODO: start date에 존재하지 않는 기업 배제: 추가 방안?
# TODO: StockSectionKind 기반 Ticker 전송
# TODO: task kill issue
