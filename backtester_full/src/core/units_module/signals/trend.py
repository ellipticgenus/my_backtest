"""
Trend Signal Module for Trading Strategies.

Contains the TS_Trend class for trend-following strategies.
"""

import pandas as pd
import numpy as np
from backtester_full.src.core.units_module.signals.base import TS_Signal


class TS_Trend(TS_Signal):
    """
    Trend-following signal module.
    
    Generates trading signals based on trend analysis using decay-weighted
    cumulative returns.
    """
    
    def __init__(self, params):
        """
        Initialize the trend signal module.
        
        Args:
            params: Dictionary containing base params plus:
                - decay_factor: Decay factor for trend calculation (default: 1.0)
                - returns: Return period for trend calculation (default: 1)
        """
        super().__init__(params)
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Generate trades for a specific date based on trend signals.
        
        Args:
            date: Date to generate trades for
            portfolio: Portfolio object
            risks_on_dates: Risk data by date
            
        Returns:
            List of Trade objects or empty list
        """
        rebal_dates = self.rebal_dates()
        
        if date not in rebal_dates:
            return []
        
        holdings_on_date = portfolio.portfolio_state.positions.copy()
        trend, total_values = self.signals_on_date(date, holdings_on_date, risks_on_dates)
        
        if self.inverse_signal * trend * total_values < 0:
            portfolio.portfolio_state.flip_trades_direction()
            return self.generate_trade(holdings_on_date, date)
        else:
            return []
    
    def signals_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule=7):
        """
        Calculate trend signals for a specific date.
        
        Uses decay-weighted cumulative returns to determine trend direction.
        
        Args:
            date: Date to calculate signals for
            holdings_on_date: Current holdings
            risks_on_dates: Risk data by date
            roll_schedule: Roll schedule in days (default: 7)
            
        Returns:
            Tuple of (trend_signal, total_value)
        """
        total_return, weights, total_value = self.prepare_ts_data(
            date, holdings_on_date, risks_on_dates, roll_schedule
        )
        
        if total_return.empty:
            return 0, 0
        
        portfolio_return = total_return.dot(weights)
        portfolio_return = pd.DataFrame(portfolio_return, columns=['return'])
        portfolio_return['cumreturn'] = (1 + portfolio_return['return']).cumprod()

        trend_signal = 0
        for lookback in self.lookbacks:
            decay_factor = self.decay_factor
            decay_factors = decay_factor ** np.arange(lookback)[::-1]
            decay_factors = decay_factors / np.sum(decay_factors)
            
            trend_signal += (
                portfolio_return['cumreturn']
                .rolling(lookback)
                .apply(lambda x: np.sum(decay_factors * x) / np.mean(x.iloc[-self.returns:]))
                .rolling(5)
                .mean()
                .iloc[-1]
            )
        
        return trend_signal / len(self.lookbacks) - 1, total_value