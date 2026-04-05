"""
Price Loader for loading futures and spot price data.

Supports loading price data organized by exchange and ticker from the data/price folder.
"""

import pandas as pd
import os
from typing import Optional, Dict, Any, List
from backtester_full.src.core.data_loader.base_loader import BaseLoader


class PriceLoader(BaseLoader):
    """
    Loader for futures and spot price data.
    
    Loads price data from the data/price folder structure:
    data/price/{exchange}/{ticker}/
    
    Supports both CSV and Parquet formats.
    """
    
    # Exchange folder mapping
    EXCHANGE_FOLDERS = {
        'CBT': 'cbt',
        'DCE': 'dce',
        'CZCE': 'czce',
        'ICE': 'ice',
        'NYB': 'nyb',
        'SGX': 'sgx',
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the price loader.
        
        Args:
            config: Configuration dictionary with optional keys:
                - base_path: Base path for data files (default: 'data')
                - cache_enabled: Whether to cache loaded data (default: True)
                - price_subfolder: Subfolder for price data (default: 'price')
        """
        super().__init__(config)
        self.price_subfolder = self.config.get('price_subfolder', 'price')
    
    def _get_exchange_folder(self, exchange: str) -> str:
        """
        Get the folder name for an exchange.
        
        Args:
            exchange: Exchange code (e.g., 'CBT', 'DCE')
            
        Returns:
            Folder name for the exchange
        """
        return self.EXCHANGE_FOLDERS.get(exchange, exchange.lower())
    
    def load_future_price(
        self,
        symbol: str,
        contract: str,
        exchange: Optional[str] = None,
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load price data for a specific futures contract.
        
        Args:
            symbol: Commodity symbol (e.g., 'S' for Soybeans, 'C' for Corn)
            contract: Contract code (e.g., 'H24', 'Z24')
            exchange: Exchange code (optional, used to determine folder)
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with price data indexed by date
        """
        # Construct the file path
        if exchange:
            exchange_folder = self._get_exchange_folder(exchange)
            subfolder = os.path.join(self.price_subfolder, exchange_folder, symbol.lower())
        else:
            # Try to find the symbol in any exchange folder
            subfolder = os.path.join(self.price_subfolder)
        
        filename = f"{symbol}{contract}"
        
        df = self.load_data(
            filename=filename,
            subfolder=subfolder,
            extension=extension
        )
        
        return self.set_index_by_date(df)
    
    def load_nearby_series(
        self,
        symbol: str,
        k_nearby: int,
        roll_schedule: int,
        contract_type: str = 'monthly',
        extension: str = 'csv'
    ) -> pd.DataFrame:
        """
        Load nearby futures series data.
        
        Args:
            symbol: Commodity symbol
            k_nearby: Nearby contract number (1, 2, 3, etc.)
            roll_schedule: Roll schedule in days
            contract_type: 'monthly', 'quarterly', or 'yearly'
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            DataFrame with series data indexed by date
        """
        # Handle quarterly/yearly prefix
        if contract_type == 'quarterly':
            prefix = 'Q'
        elif contract_type == 'yearly':
            prefix = 'Y'
        else:
            prefix = ''
        
        # Construct filename
        filename = f"{symbol}{prefix}_{k_nearby}_{roll_schedule}"
        
        # Series are stored in data/timeseries or data/price/series
        subfolder = os.path.join('timeseries')
        
        df = self.load_data(
            filename=filename,
            subfolder=subfolder,
            extension=extension
        )
        
        return self.set_index_by_date(df)
    
    def load(
        self,
        symbol: str,
        contract: Optional[str] = None,
        exchange: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load price data (main entry point).
        
        Args:
            symbol: Commodity symbol
            contract: Contract code (optional for series data)
            exchange: Exchange code (optional)
            **kwargs: Additional arguments passed to specific load methods
            
        Returns:
            DataFrame with price data
        """
        if contract:
            return self.load_future_price(symbol, contract, exchange, **kwargs)
        else:
            # Load as series if no contract specified
            k_nearby = kwargs.get('k_nearby', 1)
            roll_schedule = kwargs.get('roll_schedule', 7)
            contract_type = kwargs.get('contract_type', 'monthly')
            return self.load_nearby_series(
                symbol, k_nearby, roll_schedule, contract_type, **kwargs
            )
    
    def get_available_contracts(
        self,
        symbol: str,
        exchange: Optional[str] = None
    ) -> List[str]:
        """
        Get list of available contracts for a symbol.
        
        Args:
            symbol: Commodity symbol
            exchange: Exchange code (optional)
            
        Returns:
            List of available contract codes
        """
        contracts = []
        
        if exchange:
            exchange_folder = self._get_exchange_folder(exchange)
            folder_path = os.path.join(
                self.base_path, 
                self.price_subfolder, 
                exchange_folder, 
                symbol.lower()
            )
        else:
            # Search in all exchange folders
            folder_path = os.path.join(self.base_path, self.price_subfolder)
        
        if not os.path.exists(folder_path):
            return contracts
        
        for file in os.listdir(folder_path):
            if file.endswith('.csv') or file.endswith('.parquet'):
                # Extract contract code from filename
                contract = file.split('.')[0]
                if contract.startswith(symbol):
                    contract = contract[len(symbol):]
                contracts.append(contract)
        
        return sorted(contracts)