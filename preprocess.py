import numpy as np
import pandas as pd

from datetime import datetime, timedelta


class PreProcess:
    def __init__(self, start: str, end: str):
        self.start = start
        self.end = end

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

    def pp_time_diff(self, time_interval: int = 252 * 3) -> list:
        """
        Limit time difference to 3-year business days.
        Slice time interval.
        """
        time_interval_dt = timedelta(days=time_interval)
        start_dt = datetime.strptime(self.start, "%Y%m%d")
        end_dt = datetime.strptime(self.end, "%Y%m%d")

        res_dt = []
        while start_dt < end_dt:
            next_dt = start_dt + time_interval_dt
            res_dt.append([datetime.strftime(start_dt, "%Y%m%d"),
                           datetime.strftime(min(next_dt, end_dt), "%Y%m%d")])
            start_dt = next_dt + timedelta(days=1)
        return res_dt
