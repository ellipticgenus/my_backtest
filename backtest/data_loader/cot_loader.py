"""
COT (Commitment of Traders) Data Loader.

Loads COT report data from CSV files.
"""

import os
import pandas as pd
from typing import Optional, Dict, Any, List

from backtest.data_loader.base_loader import BaseLoader


class COTLoader(BaseLoader):
    """
    Loader for COT (Commitment of Traders) report data.
    
    Loads COT data from CSV files organized by commodity.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.cot_subfolder = self.config.get('cot_subfolder', 'cot')
    
    def load(
        self,
        commodity: str,
        columns: Optional[List[str]] = None,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load COT data for a specific commodity.
        
        Args:
            commodity: Commodity symbol
            columns: List of columns to load (default: all)
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with COT data
        """
        df = self.load_data(
            filename=commodity,
            subfolder=self.cot_subfolder,
            extension=extension
        )
        
        if columns:
            df = df[columns]
        
        return df
    
    def load_multiple(
        self,
        commodities: List[str],
        columns: Optional[List[str]] = None,
        extension: str = 'csv'
    ) -> Dict[str, pd.DataFrame]:
        """
        Load COT data for multiple commodities.
        
        Args:
            commodities: List of commodity symbols
            columns: List of columns to load (default: all)
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            Dictionary mapping commodity to DataFrame
        """
        result = {}
        for commodity in commodities:
            try:
                result[commodity] = self.load(commodity, columns, extension)
            except FileNotFoundError as e:
                print(f"Warning: Could not load COT data for {commodity}: {e}")
        return result
    
    def load_with_date_index(
        self,
        commodity: str,
        date_column: str = 'date',
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load COT data with date as index.
        
        Args:
            commodity: Commodity symbol
            date_column: Name of the date column
            extension: File extension
            
        Returns:
            DataFrame with COT data indexed by date
        """
        df = self.load(commodity, extension=extension)
        
        # Convert date column
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column])
            df = df.set_index(date_column)
        
        # Sort by date
        df = df.sort_index()
        
        return df