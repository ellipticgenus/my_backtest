"""
Series Loader for loading timeseries data.

Supports loading timeseries data from the data/timeseries folder.
"""

import pandas as pd
import os
from typing import Optional, Dict, Any, List
from backtester_full.src.core.data_loader.base_loader import BaseLoader


class SeriesLoader(BaseLoader):
    """
    Loader for timeseries data.
    
    Loads timeseries data from the data/timeseries folder.
    Supports both CSV and Parquet formats.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the series loader.
        
        Args:
            config: Configuration dictionary with optional keys:
                - base_path: Base path for data files (default: 'data')
                - cache_enabled: Whether to cache loaded data (default: True)
                - series_subfolder: Subfolder for timeseries data (default: 'timeseries')
        """
        super().__init__(config)
        self.series_subfolder = self.config.get('series_subfolder', 'timeseries')
    
    def load_series(
        self,
        filename: str,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load a timeseries file.
        
        Args:
            filename: Name of the file (with or without extension)
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with timeseries data indexed by date
        """
        df = self.load_data(
            filename=filename,
            subfolder=self.series_subfolder,
            extension=extension
        )
        
        return self.set_index_by_date(df)
    
    def load_signal_series(
        self,
        symbol: str,
        signal_name: str,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load signal timeseries data for a symbol.
        
        Args:
            symbol: Commodity symbol
            signal_name: Name of the signal
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with signal data indexed by date
        """
        filename = f"{symbol}_{signal_name}"
        return self.load_series(filename, extension)
    
    def load_vol_series(
        self,
        symbol: str,
        k_nearby: int = 1,
        roll_schedule: int = 7,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load volatility series data.
        
        Args:
            symbol: Commodity symbol
            k_nearby: Nearby contract number
            roll_schedule: Roll schedule in business days
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with volatility data indexed by date
        """
        filename = f"{symbol}_{k_nearby}_{roll_schedule}"
        return self.load_series(filename, extension)
    
    def load(
        self,
        filename: str,
        extension: str = 'csv',
        **kwargs
    ) -> pd.DataFrame:
        """
        Load timeseries data (main entry point).
        
        Args:
            filename: Name of the file
            extension: File extension ('csv' or 'parquet')
            **kwargs: Additional arguments
            
        Returns:
            DataFrame with timeseries data
        """
        return self.load_series(filename, extension)
    
    def get_available_series(self) -> List[str]:
        """
        Get list of available timeseries files.
        
        Returns:
            List of available series filenames
        """
        series = []
        folder_path = os.path.join(self.base_path, self.series_subfolder)
        
        if not os.path.exists(folder_path):
            return series
        
        for file in os.listdir(folder_path):
            if file.endswith('.csv') or file.endswith('.parquet'):
                series.append(file)
        
        return sorted(series)
    
    def save_series(
        self,
        df: pd.DataFrame,
        filename: str,
        extension: str = 'parquet'
    ) -> str:
        """
        Save a DataFrame to the timeseries folder.
        
        Args:
            df: DataFrame to save
            filename: Name of the file (without extension)
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            Full path of the saved file
        """
        folder_path = os.path.join(self.base_path, self.series_subfolder)
        os.makedirs(folder_path, exist_ok=True)
        
        filepath = os.path.join(folder_path, f"{filename}.{extension}")
        
        # Reset index if date is in index
        if df.index.name == 'date' or df.index.name is None:
            df = df.reset_index()
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        if extension == 'parquet':
            df.to_parquet(filepath, index=False)
        else:
            df.to_csv(filepath, index=False)
        
        return filepath