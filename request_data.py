import pandas as pd

from auto_login import *
from ticker import *


class RequestData(Ticker):
    def __init__(self, login_info='login.json'):
        super().__init__()
        if not self.config_connect:
            self.process_connect(login_info=login_info)

        self.obj = self.objStockWeek

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
    def price_request(self):
        self.config_request
        count = self.obj.GetHeaderValue(1)

        info = []
        for order in range(count):
            date = self.obj.GetDataValue(0, order)
            open = self.obj.GetDataValue(1, order)
            high = self.obj.GetDataValue(2, order)
            low = self.obj.GetDataValue(3, order)
            close = self.obj.GetDataValue(4, order)
            diff = self.obj.GetDataValue(5, order)
            vol = self.obj.GetDataValue(6, order)
            info.append([date, open, high, low, close, diff, vol])
        return info

    def get_request(self, stock_code: str):
        self.obj.SetInputValue(0, stock_code)
        self.config_request

        res = pd.DataFrame()
        nextcount = 1
        while self.obj.Continue:
            nextcount += 1
            if (nextcount > 5):
                print("REQUEST FINISHED.")
                break

            temp = pd.DataFrame(self.price_request,
                                columns=['Date', 'Open', 'High', 'Low', 'Close', 'Diff', 'Vol'])
            res = pd.concat([res, temp], axis=0)

        res = res[::-1].reset_index(drop=True)
        res.Date = pd.to_datetime(res.Date, format='%Y%m%d')
        return res


if __name__ == "__main__":
    request_data = RequestData()
    SSE_price = request_data.get_request('A005930')
    print(SSE_price)

# TODO: 데이터가 존재하지 않을 경우, NaN 처리
# TODO: 2023/05/23까지만 로드되는 데이터 (가장 최근은?)
# TODO: 수정주가 로드하는 방법
