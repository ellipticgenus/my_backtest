"""
Price Data Loader.

Loads futures price data from CSV files.
"""

import os
import pandas as pd
from typing import Optional, Dict, Any, List

from backtest.data_loader.base_loader import BaseLoader


class PriceLoader(BaseLoader):
    """
    Loader for futures price data.
    
    Loads price data from CSV files organized by commodity.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.price_subfolder = self.config.get('price_subfolder', 'price')
    
    def load(
        self,
        commodity: str,
        columns: Optional[List[str]] = None,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load price data for a specific commodity.
        
        Args:
            commodity: Commodity symbol (e.g., 'S10TC', 'C5TC')
            columns: List of columns to load (default: all)
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with price data
        """
        df = self.load_data(
            filename=commodity,
            subfolder=self.price_subfolder,
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
        Load price data for multiple commodities.
        
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
                print(f"Warning: Could not load {commodity}: {e}")
        return result
    
    def load_contracts(
        self,
        commodity: str,
        contract_pattern: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Load all contract data for a commodity from individual contract files.
        
        Args:
            commodity: Commodity symbol
            contract_pattern: Pattern to filter contracts (optional)
            
        Returns:
            DataFrame with contracts as columns and dates as index
        """
        commodity_path = self._resolve_path(commodity, self.price_subfolder, '')
        
        if not os.path.exists(commodity_path):
            raise FileNotFoundError(f"Commodity path not found: {commodity_path}")
        
        files = [f for f in os.listdir(commodity_path) if f.endswith('.csv')]
        
        dfs = []
        for file in files:
            if contract_pattern and contract_pattern not in file:
                continue
            df = pd.read_csv(os.path.join(commodity_path, file))
            df['contract'] = file.replace('.csv', '')
            df['date'] = pd.to_datetime(df['date'])
            dfs.append(df)
        
        if not dfs:
            raise FileNotFoundError(f"No contract files found in {commodity_path}")
        
        big_df = pd.concat(dfs, ignore_index=True)
        pivot_df = big_df.pivot(index='date', columns='contract', values='close')
        
        return pivot_df