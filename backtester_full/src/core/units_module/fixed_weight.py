import pandas as pd
from my_holidays.holiday_utils import *
from commodity.commodconfig import COMMODINFO
from backtester.src.core.portfolio import Trade
import re

class FixedWeight:
    """
    A fixed weight module that only trade strategies.
    """

    def __init__(self, params: dict):
        self.params = params

    @property
    def holiday_calendar(self):
        """
        Get the holiday calendar for the commodity.
        :return: str, holiday calendar code
        """
        strategies = self.params['asset_table']
        holidays = []
        for row in strategies:
            holidays.append(row.holidays)
        return list(set(holidays)).join("|")

    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Check if a trade can be executed on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :return: [] list of trades to be executed on the date
        """
        pass
