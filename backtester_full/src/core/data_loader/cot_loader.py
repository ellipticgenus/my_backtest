"""
COT (Commitment of Traders) Data Loader.

Supports loading COT data from the data/cot folder.
"""

import pandas as pd
import os
from typing import Optional, Dict, Any, List
from backtester_full.src.core.data_loader.base_loader import BaseLoader


class COTLoader(BaseLoader):
    """
    Loader for Commitment of Traders (COT) data.
    
    Loads COT data from the data/cot folder.
    Supports both CSV and Parquet formats.
    """
    
    # Default COT subfolder
    COT_SUBFOLDER = 'cot'
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the COT loader.
        
        Args:
            config: Configuration dictionary with optional keys:
                - base_path: Base path for data files (default: 'data')
                - cache_enabled: Whether to cache loaded data (default: True)
                - cot_subfolder: Subfolder for COT data (default: 'cot')
        """
        super().__init__(config)
        self.cot_subfolder = self.config.get('cot_subfolder', self.COT_SUBFOLDER)
    
    def load_cot(
        self,
        source: str,
        symbol: Optional[str] = None,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load COT data for a specific source.
        
        Args:
            source: COT data source (e.g., 'SGX', 'EEX', 'CFTC')
            symbol: Commodity symbol to filter by (optional)
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with COT data indexed by date
        """
        filename = f"{source}_COT"
        
        df = self.load_data(
            filename=filename,
            subfolder=self.cot_subfolder,
            extension=extension
        )
        
        # Filter by symbol if provided
        if symbol and 'Symbol' in df.columns:
            df = df[df['Symbol'] == symbol]
        
        # Handle date column variations
        date_cols = ['date', 'Date', 'Clear Date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                df = df.set_index(col)
                break
        
        return df
    
    def load_combined_cot(
        self,
        symbols: List[str],
        sources: Optional[List[str]] = None,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load and combine COT data from multiple sources.
        
        Args:
            symbols: List of commodity symbols to include
            sources: List of COT sources to combine (default: ['SGX', 'EEX'])
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            Combined DataFrame with COT data
        """
        if sources is None:
            sources = ['SGX', 'EEX']
        
        combined_dfs = []
        
        for source in sources:
            for symbol in symbols:
                try:
                    df = self.load_cot(source, symbol, extension)
                    df['source'] = source
                    combined_dfs.append(df)
                except FileNotFoundError:
                    continue
        
        if not combined_dfs:
            raise FileNotFoundError(f"No COT data found for symbols: {symbols}")
        
        return pd.concat(combined_dfs)
    
    def process_cot_data(
        self,
        df: pd.DataFrame,
        include_zscore: bool = True,
        zscore_lookback: int = 26
    ) -> pd.DataFrame:
        """
        Process raw COT data to add derived columns.
        
        Args:
            df: Raw COT DataFrame
            include_zscore: Whether to calculate z-scores
            zscore_lookback: Lookback period for z-score calculation
            
        Returns:
            Processed DataFrame with additional columns
        """
        df = df.copy()
        
        # Calculate net positions
        if 'Managed Money Long' in df.columns and 'Managed Money Short' in df.columns:
            df['MM Net'] = df['Managed Money Long'] - df['Managed Money Short']
            if 'Open Interest' in df.columns:
                df['MM Ratio'] = df['MM Net'] / df['Open Interest']
        
        if 'Financial Institutions Long' in df.columns and 'Financial Institutions Short' in df.columns:
            df['FI Net'] = df['Financial Institutions Long'] - df['Financial Institutions Short']
            if 'Open Interest' in df.columns:
                df['FI Ratio'] = df['FI Net'] / df['Open Interest']
        
        if 'Physicals Long' in df.columns and 'Physicals Short' in df.columns:
            df['P Net'] = df['Physicals Long'] - df['Physicals Short']
            if 'Open Interest' in df.columns:
                df['P Ratio'] = df['P Net'] / df['Open Interest']
        
        # Calculate z-scores
        if include_zscore:
            if 'MM Ratio' in df.columns:
                df['MM ZScore'] = (
                    df['MM Ratio'] - df['MM Ratio'].rolling(zscore_lookback).mean()
                ) / df['MM Ratio'].rolling(20).std(ddof=0)
            
            if 'FI Ratio' in df.columns:
                df['FI ZScore'] = (
                    df['FI Ratio'] - df['FI Ratio'].rolling(zscore_lookback).mean()
                ) / df['FI Ratio'].rolling(20).std(ddof=0)
            
            if 'P Ratio' in df.columns:
                df['P ZScore'] = (
                    df['P Ratio'] - df['P Ratio'].rolling(zscore_lookback).mean()
                ) / df['P Ratio'].rolling(20).std(ddof=0)
        
        return df
    
    def load(
        self,
        source: str,
        symbol: Optional[str] = None,
        extension: str = 'csv',
        **kwargs
    ) -> pd.DataFrame:
        """
        Load COT data (main entry point).
        
        Args:
            source: COT data source
            symbol: Commodity symbol to filter by (optional)
            extension: File extension ('csv' or 'parquet')
            **kwargs: Additional arguments
                - process: Whether to process the data (default: False)
                - include_zscore: Whether to include z-scores (default: True)
            
        Returns:
            DataFrame with COT data
        """
        df = self.load_cot(source, symbol, extension)
        
        if kwargs.get('process', False):
            df = self.process_cot_data(
                df,
                include_zscore=kwargs.get('include_zscore', True)
            )
        
        return df
    
    def get_available_sources(self) -> List[str]:
        """
        Get list of available COT data sources.
        
        Returns:
            List of available source names
        """
        sources = []
        folder_path = os.path.join(self.base_path, self.cot_subfolder)
        
        if not os.path.exists(folder_path):
            return sources
        
        for file in os.listdir(folder_path):
            if file.endswith('_COT.csv') or file.endswith('_COT.parquet'):
                source = file.replace('_COT.csv', '').replace('_COT.parquet', '')
                sources.append(source)
        
        return sorted(set(sources))
    
    def save_cot(
        self,
        df: pd.DataFrame,
        source: str,
        extension: str = 'csv'
    ) -> str:
        """
        Save COT data to a file.
        
        Args:
            df: DataFrame to save
            source: COT data source name
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            Full path of the saved file
        """
        folder_path = os.path.join(self.base_path, self.cot_subfolder)
        os.makedirs(folder_path, exist_ok=True)
        
        filename = f"{source}_COT"
        filepath = os.path.join(folder_path, f"{filename}.{extension}")
        
        # Reset index if it's a date
        if df.index.name in ['date', 'Date', 'Clear Date']:
            df = df.reset_index()
        
        if extension == 'parquet':
            df.to_parquet(filepath, index=False)
        else:
            df.to_csv(filepath, index=False)
        
        return filepath