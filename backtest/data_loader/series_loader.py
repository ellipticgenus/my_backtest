"""
Series Data Loader.

Loads time series return data for futures contracts.
"""

import os
import pandas as pd
from typing import Optional, Dict, Any, List

from backtest.data_loader.base_loader import BaseLoader
from backtest.utils.constants import PREROLL


class SeriesLoader(BaseLoader):
    """
    Loader for time series return data.
    
    Loads return series data organized by commodity and roll nearby.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.series_subfolder = self.config.get('series_subfolder', 'timeseries')
    
    def load(
        self,
        ticker: str,
        roll_nearby: int = 0,
        max_roll_date: int = 10,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load return series for a specific ticker and roll nearby.
        
        Args:
            ticker: Ticker symbol (e.g., 'C5TC', 'S10TC')
            roll_nearby: Roll nearby number (0, 1, 2, etc.)
            max_roll_date: Maximum roll date
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with return series data
        """
        symbol = f'{ticker}_{roll_nearby}_{max_roll_date}'
        
        # Handle quarterly contracts
        if ticker[-1] in ['Q']:
            folder_name = ticker[:-1]
        else:
            folder_name = ticker
        
        df = self.load_data(
            filename=symbol,
            subfolder=f'{self.series_subfolder}/{folder_name}',
            extension=extension
        )
        
        return df
    
    def load_multiple_nearby(
        self,
        ticker: str,
        max_roll_date: int = 10,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load return series for all available roll nearbys for a ticker.
        
        Args:
            ticker: Ticker symbol
            max_roll_date: Maximum roll date
            extension: File extension
            
        Returns:
            DataFrame with all nearby series merged
        """
        # Get available roll nearbys from PREROLL config
        roll_nearbys = PREROLL.get(ticker[-1], [0, 1, 2])
        
        ts_list = []
        for roll_nearby in roll_nearbys:
            try:
                df = self.load(ticker, roll_nearby, max_roll_date, extension)
                symbol = f'{ticker}_{roll_nearby}_{max_roll_date}'
                df = df.rename(columns={'return': symbol})
                ts_list.append(df[symbol])
            except FileNotFoundError:
                print(f"Warning: Could not load {ticker} nearby {roll_nearby}")
                continue
        
        if not ts_list:
            raise FileNotFoundError(f"No series data found for {ticker}")
        
        merged_df = pd.concat(ts_list, axis=1)
        return merged_df
    
    def load_portfolio_series(
        self,
        tickers: List[str],
        max_roll_date: int = 10,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load return series for multiple tickers.
        
        Args:
            tickers: List of ticker symbols
            max_roll_date: Maximum roll date
            extension: File extension
            
        Returns:
            DataFrame with all ticker series merged
        """
        all_series = []
        
        for ticker in tickers:
            try:
                df = self.load_multiple_nearby(ticker, max_roll_date, extension)
                all_series.append(df)
            except FileNotFoundError:
                print(f"Warning: Could not load series for {ticker}")
                continue
        
        if not all_series:
            raise FileNotFoundError("No series data found for any ticker")
        
        merged_df = pd.concat(all_series, axis=1)
        return merged_df