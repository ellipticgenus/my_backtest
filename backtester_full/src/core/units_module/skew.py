from .base_module import BaseUnitModule
import pandas as pd
from my_holiday.holiday_utils import *
from backtester_full.src.core.portfolio import Trade
from backtester_full.src.core.utils.utils import partition_ticker
import numpy as np


class Skew(BaseUnitModule):
    def __init__(self, params):
        super().__init__()
        self.params = params
        self.lookbacks = params['lookbacks']
        self.roll_info = params.get('roll_info',[(0,7),(1,7)])
        self.contract_type = params.get('contract_type','monthly') 
        self.future_instruments = params.get('future_instruments', [])
        self.initial_date = params['initial_date']
        self.end_date = params['end_date']


    @property
    def holiday_calendar(self):
        return self.params['holiday_calendar']
    
    @property
    def prefix(self):
        return {'quarterly': 'Q', 'yearly': 'Y', 'monthly': ''}[self.contract_type]

    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()
        # trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        # print(date, trades_on_date)
        # trades_on_date = [trade for trade in trades_on_date if trade.date == date]
        if date not in rebal_dates:
            return []
    
        #    Check if strategy instruments are implemented
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        # print(date, holdings_on_date)       
        skew , total_values= self.signals_on_date(date, holdings_on_date, risks_on_dates)
        if skew*total_values>0:
            portfolio.portfolio_state.flip_trades_direction()
            # print(date, skew, total_values)
            return self.generate_trade(holdings_on_date,   date)
        else:
            return []

     
    
    def generate_trade(self, positions, date):
        trades = []
        for ticker, size in positions.items():
            if ticker != 'USD':
                trades.append(Trade(
                    ticker,
                    date,
                    -size *2,
                    denominate='USD',
                    trade_type='neutral',
                    symbol = partition_ticker(ticker)[0]
                ))
        return trades

    def instrument_on_date(self, date):
        instruments = []
        for nearby_future in self.future_instruments:
            for n_nearby, roll_schedule in self.roll_info:
                ticker = f'{nearby_future}{self.prefix}_{n_nearby}_{roll_schedule}'
                instruments.append({'ticker': ticker, 'type': 'NearbyFuture'})
        
        return instruments

    def signals_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        """
        Get the skew signal for a given date
        :param date: str, date in 'YYYY-MM-DD' format
        :param risks_on_dates: dict, mapping of date to risk values
        :return: dict, mapping of instrument to skew signal
        """
        tickers =[ticker for ticker in  list(holdings_on_date.keys()) if ticker != 'USD' and holdings_on_date[ticker] != 0]

        if not tickers:
            return 0, 0
        returns = {}
        
        prev_date = previous_business_day(date, self.holiday_calendar)
        prev_date = pd.to_datetime(prev_date)
        buz_dates = business_days_until(prev_date, max(self.lookbacks) + 10, self.holiday_calendar)
        # print(date, holdings_on_date)
        portfolio_values = [holdings_on_date[ticker] * risks_on_dates[prev_date][ticker]['close'] for  ticker in tickers]
        total_value = sum(portfolio_values)
        weights = np.array(portfolio_values)/total_value
        for ticker in tickers:
            # print(date, ticker)
            nearby = contract_to_nearby(date, ticker[-3:], roll_schedule, contract_type = self.contract_type )
            if self.contract_type != 'monthly':
                symbol = ticker[:-4]
            else:
                symbol = ticker[:-3]
            # print(ticker,symbol, self.prefix, nearby,roll_schedule)
            # print(risks_on_dates[buz_dates[0]])
            ret = [ risks_on_dates[buz_date][f'{symbol}{self.prefix}_{nearby}_{roll_schedule}']['return'] for buz_date in buz_dates]
            # print(ret)
            returns[ticker] = ret
        

        total_return = pd.DataFrame(returns)
        portfolio_return = total_return.dot(weights)
        skew_signal = 0
        for lookback in self.lookbacks:
            # print((portfolio_return.rolling(lookback).skew()))
            skew_signal += (portfolio_return.rolling(lookback).skew()).iloc[-1]
        return skew_signal/len(self.lookbacks), total_value


    def lookback_dates(self, date):
        """
        Generate a list of lookback dates to calculate the risk metric
        :param date: str, date in 'YYYY-MM-DD' format
        :return: list of str, dates in 'YYYY-MM-DD' format
        """
        period = max(self.lookbacks) + 1
        dates = pd.date_range(end = date, periods=period + 10, freq='B')
        dates = [date for date in dates if not is_holiday(date, self.holiday_calendar)]
        return dates[-period:]

    def risk_dates(self, start_date, end_date):
        """
        Generate a list of dates to calculate the risk metric over a given period
        :param start_date: str, start date in 'YYYY-MM-DD' format
        :param end_date: str, end date in 'YYYY-MM-DD' format
        :return: list of str, dates in 'YYYY-MM-DD' format
        """
        extra_dates = self.lookback_dates(start_date)
        business_dates = business_days_between(start_date, end_date, self.holiday_calendar)
        risk_dates = sorted(set(extra_dates+business_dates))
        return risk_dates
    
    def rebal_dates(self):
        business_dates = [pd.to_datetime(date) for date in business_days_between(self.initial_date, self.end_date, self.holiday_calendar)]
        
        return business_dates[::1]