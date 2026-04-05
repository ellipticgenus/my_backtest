"""
COT (Commitment of Traders) Signal Module for Trading Strategies.

Contains the TS_COT class for COT-based trading signals.
"""

import pandas as pd
import numpy as np
from backtester_full.src.core.units_module.signals.base import TS_Signal


class TS_COT(TS_Signal):
    """
    COT-based signal module.
    
    Generates trading signals based on Commitment of Traders data analysis,
    using z-scores and momentum differentials.
    """
    
    def __init__(self, params):
        """
        Initialize the COT signal module.
        
        Args:
            params: Dictionary containing base params plus:
                - cot_data: DataFrame with COT data
                - signal_delay: Days to delay signal (default: 5)
                - zscore_threshold: Z-score threshold for signals (default: 1.5)
                - diff_threshold: Diff threshold for signals (default: 0.6)
        """
        super().__init__(params)
        self.cot_data = params.get('cot_data', pd.DataFrame())
        self.signal_delay = params.get('signal_delay', 5)
        self.zscore_threshold = params.get('zscore_threshold', 1.5)
        self.diff_threshold = params.get('diff_threshold', 0.6)
    
    def get_cot_signal(self, date):
        """
        Get COT signal for a specific date.
        
        Args:
            date: Date to get signal for
            
        Returns:
            Tuple of (zscore, diff) or (None, None) if not available
        """
        if self.cot_data.empty:
            return None, None
        
        signal_date = date - pd.Timedelta(days=self.signal_delay)
        if signal_date in self.cot_data.index:
            zscore = self.cot_data.loc[signal_date, 'MM ZScore']
            diff = self.cot_data.loc[signal_date, 'Diff']
            return zscore, diff
        return None, None
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Generate trades for a specific date based on COT signals.
        
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
        signal, total_values = self.signals_on_date(date, holdings_on_date, risks_on_dates)
        
        if self.inverse_signal * signal * total_values < 0:
            portfolio.portfolio_state.flip_trades_direction()
            return self.generate_trade(holdings_on_date, date)
        else:
            return []
    
    def signals_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule=7):
        """
        Calculate COT signals for a specific date.
        
        Args:
            date: Date to calculate signals for
            holdings_on_date: Current holdings
            risks_on_dates: Risk data by date
            roll_schedule: Roll schedule in days (default: 7)
            
        Returns:
            Tuple of (signal_value, total_value)
        """
        total_return, weights, total_value = self.prepare_ts_data(
            date, holdings_on_date, risks_on_dates, roll_schedule
        )
        
        zscore, diff = self.get_cot_signal(date)
        
        if zscore is None:
            return 0, total_value
        
        signal = 0
        if abs(zscore) > self.zscore_threshold:
            signal = -np.sign(zscore)
        elif abs(diff) > self.diff_threshold:
            signal = np.sign(diff) * 0.5
        
        return signal, total_value