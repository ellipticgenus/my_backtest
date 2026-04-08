"""
Wind Data Downloader.

Provides interface for downloading data from Wind Terminal using WindPy.
"""

import logging
import time
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List, Union
from pathlib import Path

import pandas as pd
import numpy as np
from WindPy import w

from data_api.base import DataDownloader
from data_api.wind.config import (
    WindConfig, get_symbol_type,
    DEFAULT_FIELDS, FUTURE_FIELDS, INDEX_FIELDS, FUND_FIELDS
)

# Configure logging
logger = logging.getLogger(__name__)

def data_to_df(return_query):
    data = {}
    data['date'] = return_query.Times
    for col in range(len(return_query.Fields)): #df.Fields:
        data[return_query.Fields[col]] = return_query.Data[col]
    final_df = pd.DataFrame(data)
    return final_df


class WindDownloader(DataDownloader):
    """
    Wind data downloader using WindPy API.
    
    Downloads market data from Wind financial terminal.
    Requires Wind Terminal to be running and WindPy to be installed.
    
    Example:
        >>> config = WindConfig(data_path='data/wind')
        >>> downloader = WindDownloader(config.to_dict())
        >>> downloader.connect()
        >>> df = downloader.fetch_data('AU.SHF', '2024-01-01', '2024-12-31')
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Wind downloader.
        
        Args:
            config: Configuration dictionary (can be from WindConfig.to_dict())
        """
        super().__init__(config)
        self._connected = False
        
    def connect(self) -> bool:
        """
        Connect to Wind Terminal.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Check if already connected
            if w.isconnected():
                self._connected = True
                logger.info("Already connected to Wind Terminal")
                return True
            
            # Try to start connection
            w.start()
            time.sleep(2)  # Wait for connection
            
            if w.isconnected():
                self._connected = True
                logger.info("Successfully connected to Wind Terminal")
                return True
            else:
                logger.error("Failed to connect to Wind Terminal")
                return False
                    
        except ImportError:
            logger.error(
                "WindPy not found. Please ensure Wind Terminal is installed "
                "and WindPy is available in your Python environment."
            )
            return False
        except Exception as e:
            logger.error(f"Error connecting to Wind Terminal: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from Wind Terminal.
        
        Returns:
            True if disconnection successful, False otherwise
        """
        try:
            if self._connected:
                w.stop()
                self._connected = False
                logger.info("Disconnected from Wind Terminal")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from Wind Terminal: {e}")
            return False
    
    def is_connected(self) -> bool:
        """
        Check if connected to Wind Terminal.
        
        Returns:
            True if connected, False otherwise
        """
        if not self._connected:
            return False
        
        try:
            return w.isconnected()
        except:
            return False
    
    def fetch_data(
        self,
        symbol: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        fields: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch historical data for a single symbol.
        
        Args:
            symbol: Wind symbol (e.g., 'AU.SHF', 'IF.CFE', 'SH000001')
            start_date: Start date for data
            end_date: End date for data
            fields: List of fields to fetch (default: based on symbol type)
            options: Additional Wind API options
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with columns: date, open, high, low, close, volume, etc.
        """
        self._ensure_connected()
        
        # Format dates for Wind API
        start_str = self._format_date(start_date, '%Y-%m-%d')
        end_str = self._format_date(end_date, '%Y-%m-%d')
        
        # Determine fields based on symbol type if not specified
        if fields is None:
            symbol_type = get_symbol_type(symbol)
            if symbol_type in ['commodity_future', 'index_future', 'bond_future']:
                fields = self.config.get('future_fields', FUTURE_FIELDS)
            elif symbol_type == 'index':
                fields = self.config.get('index_fields', INDEX_FIELDS)
            elif symbol_type == 'fund':
                fields = self.config.get('fund_fields', FUND_FIELDS)
            else:
                fields = self.config.get('default_fields', DEFAULT_FIELDS)
        
        # Build options string
        opt_str = self._build_options(options)
        
        # Convert fields list to comma-separated string for Wind API
        fields_str = ",".join(fields) if isinstance(fields, list) else fields
        
        # Fetch data with retry logic
        for attempt in range(self.retry_count):
            try:
                logger.info(f"Fetching {symbol} from {start_str} to {end_str} (attempt {attempt + 1})")
                print(symbol, fields_str, start_str, end_str, opt_str)

                result = w.wsd(
                    symbol,
                    fields_str,
                    start_str,
                    end_str,
                    opt_str
                )
                # print(result)
                if result.ErrorCode != 0:
                    logger.warning(f"Wind API error: {result.ErrorCode}")
                    if attempt < self.retry_count - 1:
                        time.sleep(self.retry_delay)
                        continue
                    raise Exception(f"Wind API error code: {result.ErrorCode}")
                
                # Convert to DataFrame
                df = data_to_df(result)
                
                logger.info(f"Fetched {len(df)} records for {symbol}")
                return df
                
            except Exception as e:
                logger.error(f"Error fetching data (attempt {attempt + 1}): {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
        
        return pd.DataFrame()
    
    def fetch_data_batch(
        self,
        symbols: List[str],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        fields: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch historical data for multiple symbols.
        
        Args:
            symbols: List of Wind symbols
            start_date: Start date for data
            end_date: End date for data
            fields: List of fields to fetch
            options: Additional Wind API options
            **kwargs: Additional parameters
            
        Returns:
            Dictionary mapping symbols to DataFrames
        """
        results = {}
        
        for symbol in symbols:
            try:
                df = self.fetch_data(symbol, start_date, end_date, fields, options, **kwargs)
                if not df.empty:
                    results[symbol] = df
            except Exception as e:
                logger.error(f"Failed to fetch {symbol}: {e}")
                continue
        
        return results
    