import time
import datetime as dt
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from ticker import *

DATA = ['Ticker', 'StockSection', 'Date', 'Close']


class RequestPrice(Ticker):
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

        self.ticker = self.get_ticker_info
        self.ROW = list(range(len(DATA)))
        self.ROWS = []
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
                self.ROWS.append(list(self.ROW))
        return self.ROWS

    @staticmethod
    def pp_extract_dates(df: pd.DataFrame) -> list:
        """
        Extract dates for time-series data columns.
        """
        dates = df['Date'].tolist()
        dates_sliced = []
        current = [dates[0]]

        for i in range(1, len(dates)):
            if dates[i] > dates[i-1]:
                dates_sliced.append(current)
                current = [dates[i]]
            else:
                current.append(dates[i])

        dates_sliced.append(current)
        res = max(dates_sliced, key=len)
        return res

    @staticmethod
    def pp_append_nans(df: pd.DataFrame) -> pd.DataFrame:
        """
        Append NaN values for companies listed after start date.
        (i.e. Listed between start date and end date.)
        """
        res = df.groupby('Ticker')['Close'].agg(list).reset_index()
        max_date = res['Close'].apply(len).max()
        res['Close'] = res['Close'].apply(
            lambda x: x + [np.nan] * (max_date - len(x)))
        return res

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
        dates = self.pp_extract_dates(df)
        temp = self.pp_append_nans(df)
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
        self.time_interval = timedelta(days=252 * 3)

    def pp_time_diff(self) -> list:
        """
        Limit time difference to 3-year business days.
        Slice time interval.
        """
        start_dt = datetime.strptime(self.start, "%Y%m%d")
        end_dt = datetime.strptime(self.end, "%Y%m%d")

        res_dt = []
        while start_dt < end_dt:
            next_dt = start_dt + self.time_interval
            res_dt.append([datetime.strftime(start_dt, "%Y%m%d"),
                           datetime.strftime(min(next_dt, end_dt), "%Y%m%d")])
            start_dt = next_dt + timedelta(days=1)
        return res_dt

    def get_split_price(self) -> pd.DataFrame:
        """
        Get time-series price data.
        """
        res_dt = self.pp_time_diff()

        temp = []
        for start, end in res_dt:
            request_price = RequestPrice.pp_dt_storage(start, end)
            temp.append(request_price.get_price())

        res = pd.concat(temp)
        res.index = pd.to_datetime(res.index, format="%Y%m%d")
        return res


# TODO: start date에 존재하지 않는 기업 배제: 추가 방안?
# TODO: task kill issue