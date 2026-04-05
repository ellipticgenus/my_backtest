"""
Base Signal Module for Trading Strategies.

Contains the abstract TS_Signal base class for all signal generation modules.
"""

from backtester_full.src.core.units_module.base_module import BaseUnitModule
import pandas as pd
from my_holiday.holiday_utils import *
from backtester_full.src.core.portfolio import Trade
from backtester_full.src.core.utils.utils import partition_ticker
import numpy as np
from abc import abstractmethod

CONTRACT_TYPE = {'Q': 'quarterly', 'Y': 'yearly', '': 'monthly'}


def final_positions_on_date(date, holdings, strategy_trades):
    """
    Calculate final positions on a given date.
    
    Args:
        date: Date to calculate positions for
        holdings: Current holdings by strategy
        strategy_trades: Trades by strategy
        
    Returns:
        Updated holdings dictionary
    """
    final_holdings = {}
    for strategy, pos in holdings.items():
        if strategy in strategy_trades:
            trades = strategy_trades[strategy]
            for trade in trades:
                if trade['date'] == date:
                    pos[trade['ticker']] = pos.get(trade['ticker'], 0) + round(trade['size'], 10)
        final_holdings[strategy] = pos.copy()
    return final_holdings


class TS_Signal(BaseUnitModule):
    """
    Base class for time series signal modules.
    
    This abstract class provides the foundation for all signal-based trading modules.
    Subclasses must implement the trades_on_date and signals_on_date methods.
    """
    
    def __init__(self, params):
        """
        Initialize the signal module.
        
        Args:
            params: Dictionary containing:
                - lookbacks: List of lookback periods
                - returns: Return period (default: 1)
                - roll_info: Roll schedule info (default: [(0,7),(1,7)])
                - contract_type: 'monthly', 'quarterly', or 'yearly'
                - future_instruments: List of future instrument symbols
                - initial_date: Start date for the strategy
                - end_date: End date for the strategy
                - decay_factor: Decay factor for trend calculation
                - rebal_freq: Rebalancing frequency in days
                - inverse_signal: Signal direction multiplier
                - floor_nearby: Whether to floor nearby at 0
                - zero_delay: Whether to use zero delay
        """
        super().__init__()
        self.params = params
        self.lookbacks = params['lookbacks']
        self.returns = params.get('returns', 1)
        self.roll_info = params.get('roll_info', [(0, 7), (1, 7)])
        self.contract_type = params.get('contract_type', 'monthly')
        self.future_instruments = params.get('future_instruments', [])
        self.initial_date = params['initial_date']
        self.end_date = params['end_date']
        self.decay_factor = params.get('decay_factor', 1.0)
        self.rebal_freq = params.get('rebal_freq', 1)
        self.inverse_signal = params.get('inverse_signal', 1)
        self.floor_nearby = params.get('floor_nearby', False)
        self.zero_delay = params.get('zero_delay', False)
    
    @property
    def holiday_calendar(self):
        """Get the holiday calendar from params."""
        return self.params['holiday_calendar']
    
    @property
    def prefix(self):
        """Get the contract type prefix."""
        return {'quarterly': 'Q', 'yearly': 'Y', 'monthly': ''}[self.contract_type]

    @abstractmethod
    def trades_on_date(self, _date, portfolio, risks_on_dates):
        """
        Generate trades for a specific date.
        
        Args:
            _date: Date to generate trades for
            portfolio: Portfolio object
            risks_on_dates: Risk data by date
            
        Returns:
            List of Trade objects
        """
        pass

    def instrument_on_date(self, date):
        """
        Get the instruments needed for a specific date.
        
        Args:
            date: Date to get instruments for
            
        Returns:
            List of instrument dictionaries
        """
        instruments = []
        for nearby_future in self.future_instruments:
            for n_nearby, roll_schedule in self.roll_info:
                ticker = f'{nearby_future}{self.prefix}_{n_nearby}_{roll_schedule}'
                instruments.append({'ticker': ticker, 'type': 'NearbyFuture'})
        
        return instruments

    @abstractmethod
    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule=7):
        """
        Calculate signals for a specific date.
        
        Args:
            _date: Date to calculate signals for
            holdings_on_date: Current holdings
            risks_on_dates: Risk data by date
            roll_schedule: Roll schedule in days
            
        Returns:
            Signal value and optionally total value
        """
        pass

    def prepare_ts_data(self, _date, holdings_on_date, risks_on_dates, roll_schedule=7):
        """
        Prepare time series data for signal calculation.
        
        Args:
            _date: Date to prepare data for
            holdings_on_date: Current holdings
            risks_on_dates: Risk data by date
            roll_schedule: Roll schedule in days
            
        Returns:
            Tuple of (total_return DataFrame, weights array, total_value)
        """
        tickers = [ticker for ticker in list(holdings_on_date.keys()) 
                   if ticker != 'USD' and holdings_on_date[ticker] != 0]
        if not tickers:
            return pd.DataFrame(), np.array([]), 0
        
        returns = {}
        if self.zero_delay:
            prev_date = _date
        else:
            prev_date = previous_business_day(_date, self.holiday_calendar)
            prev_date = pd.to_datetime(prev_date)
        
        buz_dates = business_days_until(prev_date, max(self.lookbacks) + 8, self.holiday_calendar)
        
        portfolio_values = [holdings_on_date[ticker] * risks_on_dates[prev_date][ticker]['close'] 
                          for ticker in tickers if ticker != 'USD']
        total_value = sum(portfolio_values)
        weights = np.array(portfolio_values) / total_value
        
        for ticker in tickers:
            symbol, month, _ = partition_ticker(ticker)
            if len(month) == 2:
                suffix = month[0]
            else:
                suffix = ''
            nearby = contract_to_nearby(_date, ticker[-3:], roll_schedule, 
                                        contract_type=CONTRACT_TYPE[suffix])
            if self.floor_nearby:
                nearby = max(0, nearby)

            ret = [risks_on_dates[buz_date][f'{symbol}{suffix}_{nearby}_{roll_schedule}']['return'] 
                   for buz_date in buz_dates]
            returns[ticker] = ret
        
        total_return = pd.DataFrame(returns)
        return total_return, weights, total_value

    def generate_trade(self, positions, _date):
        """
        Generate trades from position changes.
        
        Args:
            positions: Position dictionary
            _date: Trade date
            
        Returns:
            List of Trade objects
        """
        trades = []
        for ticker, size in positions.items():
            if ticker != 'USD':
                trades.append(Trade(
                    ticker,
                    _date,
                    -size * 2,
                    denominate='USD',
                    trade_type='neutral',
                    symbol=partition_ticker(ticker)[0]
                ))
        return trades

    def lookback_dates(self, _date):
        """
        Generate a list of lookback dates to calculate the risk metric.
        
        Args:
            _date: End date
            
        Returns:
            List of dates in 'YYYY-MM-DD' format
        """
        period = max(self.lookbacks) + 10
        dates = pd.date_range(end=_date, periods=period + 10, freq='B')
        dates = [date for date in dates if not is_holiday(date, self.holiday_calendar)]
        return dates[-period:]

    def risk_dates(self, start_date, end_date):
        """
        Generate a list of dates to calculate the risk metric over a given period.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of dates
        """
        extra_dates = self.lookback_dates(start_date)
        business_dates = business_days_between(start_date, end_date, self.holiday_calendar)
        risk_dates = sorted(set(extra_dates + business_dates))
        return risk_dates
    
    def rebal_dates(self):
        """
        Get the rebalancing dates.
        
        Returns:
            List of rebalancing dates
        """
        end_date = pd.to_datetime(self.end_date) + pd.Timedelta(days=10)
        end_date = end_date.strftime('%Y-%m-%d')
        business_dates = [pd.to_datetime(date) for date in 
                         business_days_between(self.initial_date, end_date, self.holiday_calendar)]
        
        return business_dates[::self.rebal_freq]