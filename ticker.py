import pandas as pd

from enum import Enum, unique
from auto_login import *


@unique
class Market(Enum):
    NULL = 0
    KOSPI = 1
    KOSDAQ = 2
    K_OTC = 3
    KRX = 4
    KONEX = 5


# @unique
# class StockSectionKind(Enum):
#     NULL = 0
#     ST = 1
#     MF = 2
#     RT = 3
#     SC = 4
#     IF = 5
#     DR = 6
#     SW = 7
#     SR = 8
#     ELW = 9
#     BC = 11
#     FETF = 12
#     FOREIGN = 13
#     FU = 14
#     OP = 15


class Ticker(AutoLogin):
    def __init__(self,
                 mkt: str = "KOSPI",
                 login_info: str = "login.json"):
        super().__init__()
        if not self.config_connect:
            self.process_connect(login_info=login_info)

        self.mkt = mkt

    @property
    def get_ticker(self):
        """
        Get tickers.
        """
        try:
            return self.objCpCodeMgr.GetStockListByMarket(Market[self.mkt].value)
        except KeyError:
            print(
                f"Invalid mkt. Select in {list(map(lambda member: member.name, Market))}.")

    @property
    def get_ticker_info(self):
        """
        Get additional information from tickers.
        """
        tickers = self.get_ticker
        ticker_info = []
        for ticker in tickers:
            stockSection = self.objCpCodeMgr.GetStockSectionKind(ticker)
            name = self.objCpCodeMgr.CodeToName(ticker)
            ticker_info.append([ticker, stockSection, name])

        df = pd.DataFrame(ticker_info,
                          columns=["Ticker", "StockSection", "Name"])
        return df


if __name__ == "__main__":
    ticker = Ticker(mkt='KOSPI')
    info = ticker.get_ticker_info
    print(info)
