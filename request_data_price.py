import time
import datetime as dt
import numpy as np
import pandas as pd

from ticker import *

DATA = ['Ticker', 'StockSection', 'Date', 'Close']
ROW = list(range(len(DATA)))
ROWS = []


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
    def get_reqeust(self) -> list:
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
                ROW[0] = item['Ticker']
                ROW[1] = item['StockSection']
                ROW[2] = self.obj.GetDataValue(0, num)
                ROW[3] = self.obj.GetDataValue(1, num)
                ROWS.append(list(ROW))
        return ROWS

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

    def get_price(self) -> pd.DataFrame:
        """
        Get time-series price data.
        """
        df = pd.DataFrame(self.get_reqeust, columns=DATA)
        dates = self.pp_extract_dates(df)
        temp = self.pp_append_nans(df)
        res = pd.DataFrame(temp['Close'].tolist(),
                           index=temp['Ticker'],
                           columns=dates).T[::-1]
        return res


# TODO: start date에 존재하지 않는 기업 배제: 추가 방안?
# TODO: 주가 나눠서 받고, 합치기 (최대 한도 존재)


if __name__ == "__main__":
    start_time = time.time()

    request_price = RequestPrice(start='20210120')
    res = request_price.get_price()
    print(res)

    end_time = time.time()
    print('Time elapsed: {:.5f}sec'.format(end_time - start_time))
