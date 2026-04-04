import pandas as pd
import numpy as np
from commodity.commodconfig import COMMODINFO
from my_holidays.holiday_utils import *
import pickle
from dateutil.relativedelta import relativedelta
from backtester.src.core.utils.global_params_helper import GLOBALPARAMS

import os
CODE_MAP = {
        'F': 1,  # January
        'G': 2,  # February
        'H': 3,  # March
        'J': 4,  # April
        'K': 5,  # May
        'M': 6,  # June
        'N': 7,  # July
        'Q': 8,  # August
        'U': 9,  # September
        'V': 10, # October
        'X': 11, # November
        'Z': 12  # December
    }

def partition_ticker(ticker):
    """
    Take a ticker string and break it down into the underlying symbol, month code, and year.
    ticker : str  The ticker string to be parsed
    Returns: Containing the underlying symbol, month code, and year of the ticker
    """
    for s in COMMODINFO:
        if ticker.startswith(s):
            match = s
            break
    month = ticker[len(match):-2]
    year = 2000 + int( ticker[-2:])
    return match, month, year

class Future:
    """
    class used to define futures contracts:
        ticker: bloomberg ticker: string
        data: time series data related to the contract: series 
    """
    def __init__(self, ticker):
        """        ticker: str, the Bloomberg ticker for the future
        month: int, the month of the contract (1-12)
        year: int, the year of the contract
        month_code: str, optional, the month code for the contract (e.g., 'F' for January, 'G' for February, etc.)
        """
        self.ticker = ticker
        self.symbol, self.month, self.year = partition_ticker(ticker)
        self.info = COMMODINFO.get(self.symbol, {})
        self.data = self.load_data()

    @property
    def closes(self):
        """
        Get the closing prices of the future.
        :return: pd.series, mapping of date to closing price
        """
        return self.data['close']

    def close(self, date):
        """
        Get the closing price of the future for a specific date.
        :param date: str, date in 'YYYY-MM-DD' format
        :return: float, closing price
        """
        # print(self.closes)
        return self.closes.at[date]
    
    def load_data(self, data_source='folder'):
        """ 
        Load the time series data for the future.
            currently read data from folder, maybe later try option with database and bbg. 
        :return: pd.DataFrame, DataFrame 
        """
        if data_source == 'folder':
            # print('loaded data from folder')
            df = pd.read_csv(f"{GLOBALPARAMS['path']}/data/{self.symbol}/{self.month + self.ticker[-2:]}.csv")
            df['date'] = pd.to_datetime(df['date'])
            df = df.reset_index(drop=True).set_index('date')
            df = df.loc[:self.last_trading_day]
            return df
        else:
            raise ValueError("Unsupported data source. Currently only 'folder' is supported.")
       
    def get_data(self, date, cols):
        """
        get data for asked columns
        """
        price_data = {}
        date_to_use = date
        ltd = pd.to_datetime(self.last_trading_day)
        multiplier = 1
        if date == pd.Timestamp.today().normalize() and GLOBALPARAMS['risk_mode']:
            date_to_use = previous_business_day(date, 'UK_ENG')
        # here we want to handle the case of december roll
        if date > ltd or pd.to_datetime(date_to_use) > ltd:
            # really stupid but we dont have fixing in our db
            date_to_use = previous_business_day(ltd, 'UK_ENG')
            # due to stupidity we add a multiplier of 100000 so that if something is wrong we know. 
            multiplier = 10000000
     
        for col in cols:
            if col == 'close':
                # print( date_to_use, date)
                price_data[col] = float( self.close(date_to_use) )*multiplier
        return price_data

    @property
    def last_trading_day(self):
        """
        Get the expiration date of the future.
        :return: str, expiration date in 'YYYY-MM-DD' format
        """
        cal = self.info.get('holiday','UK_ENG')
        if cal is None:
            raise ValueError(f"No holiday calendar found for {self.ticker}.")
        # designed for freight only. 
        # for other commodities, the last trading day might be different.
        if self.symbol in ['C5TC','P4TC','S10TC']:
            if self.month == "Z":
                return previous_business_day(next_business_day(f"{self.year}-{12}-24", cal),cal)
            elif self.month  == 'YZ':
                year = self.year - 1
                return previous_business_day(next_business_day(f"{year}-{12}-24", cal),cal)
            else:
        
                month = MONTH_TO_NUM[self.month[-1]]
                date = pd.Timestamp(year = self.year, month = month, day = 1)
                if len(self.month) == 2:
                    date -= pd.DateOffset(months = 2)
                return last_business_day(date.strftime('%Y-%m-%d'), cal )
        else:
            raise ValueError(f"Unsupported symbol: {self.symbol}")

class NearbyFuture:
    """
    class used to define futures contracts:
        ticker: bloomberg ticker: string
    """
    def __init__(self, symbol, params):
        """
        Initialize a NearbyFuture object with the given parameters.
        :param asset: str, the ticker symbol of the asset
        :param k_nearby: int, the number of nearby contracts to consider (default is 1)
        :param roll_schedule: str, the schedule for rolling the contracts (default is an empty string)
        """
        self.symbol = symbol
        for k, v in params.items():
            setattr(self, k, v)
        self.k_nearby = params['k_nearby']
        self.roll_schedule = params['roll_schedule']
        self.logreturn = params.get('logreturn', False)
        self.skew_table = params.get('skew_table', [])
        self.data = self.load_data()

 
    
    def load_data(self):
        """ 
        Load the time series data for the future.
            currently read data from folder, maybe later try option with database and bbg. 
        :return: pd.DataFrame, DataFrame 
        """
        if self.symbol[-1] in ['Y','Q']:
            temp = self.symbol[:-1]
        else: 
            temp = self.symbol
        df = pd.read_csv(f"{GLOBALPARAMS['path']}/data/series/{temp}/{self.symbol}_{self.k_nearby}_{self.roll_schedule}.csv")
        df['date'] = pd.to_datetime(df['date'])
        df = df.reset_index(drop=True).set_index('date')
        if self.skew_table:
            for row in self.skew_table:
                if self.logreturn:
                    df[f'skew_{row["period"]}'] = df['log_return'].rolling(row['period']).skew()
                else:
                    df[f'skew_{row["period"]}'] = df['return'].rolling(row['period']).skew()
            skew_cols = [f'skew_{row["period"]}' for row in self.skew_table]    
            df['skew'] = df[skew_cols].mean(axis=1)
            df.dropna(subset=['skew'],inplace=True)
        return df
       
    def get_data(self, date, cols):
        """
        get data for asked columns
        """
        price_data = {}
        if date == pd.Timestamp.today().normalize() and GLOBALPARAMS['risk_mode']:
            date_to_use = previous_business_day(date, 'UK_ENG')
        else:
            date_to_use = date
        for col in cols:
            price_data[col] = self.data[col].at[date_to_use]
        return price_data

class Strategy:
    """
    Class used to define strategies:
        ticker: Bloomberg ticker: string
    """
    def __init__(self, strategy):
        """
        Initialize a Strategy object with the given parameters.
        :param strategy: str, the name of the strategy
        """
        self.strategy = strategy
        self.data = self.load_data()

    def load_data(self):
        """
        Load the backtest history of the strategy from a pickle file.

        :return: dict, the backtest history of the strategy
        """
        if GLOBALPARAMS['risk_mode']:
            risk_tail = '_risk'
        else:
            risk_tail = ''
        try:
            with open(f"{GLOBALPARAMS['path']}/backtester/strategy/backtest/{self.strategy}{risk_tail}.pkl", 'rb') as f:
                history = pickle.load(f)
            return history
        except FileNotFoundError:
            raise FileNotFoundError(f"Backtest file for strategy {self.strategy} not found.")

    def get_data(self, date):
        """
        Get the data for the given date and columns.

        :param date: str, the date in 'YYYY-MM-DD' format
        :param cols: list, the columns to get the data for
        :return: dict, a dictionary with the column names as keys and the data as values
        """
        return self.data[date]
        
