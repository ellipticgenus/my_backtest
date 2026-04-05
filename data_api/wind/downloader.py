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

from data_api.base import DataDownloader
from data_api.wind.config import WindConfig, get_symbol_type

# Configure logging
logger = logging.getLogger(__name__)


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
        self._wind = None
        self._connected = False
        
    def connect(self) -> bool:
        """
        Connect to Wind Terminal.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to import WindPy
            import WindPy
            self._wind = WindPy
            
            # Initialize Wind API
            result = WindPy.w.isconnected()
            
            if result or WindPy.w.isconnected():
                self._connected = True
                logger.info("Successfully connected to Wind Terminal")
                return True
            else:
                # Try to start connection
                WindPy.w.start()
                time.sleep(2)  # Wait for connection
                if WindPy.w.isconnected():
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
            if self._wind is not None:
                self._wind.w.stop()
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
        if not self._connected or self._wind is None:
            return False
        
        try:
            return self._wind.w.isconnected()
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
                fields = self.config.get('future_fields', WindConfig.future_fields)
            elif symbol_type == 'index':
                fields = self.config.get('index_fields', WindConfig.index_fields)
            elif symbol_type == 'fund':
                fields = self.config.get('fund_fields', WindConfig.fund_fields)
            else:
                fields = self.config.get('default_fields', WindConfig.default_fields)
        
        # Build options string
        opt_str = self._build_options(options)
        
        # Fetch data with retry logic
        for attempt in range(self.retry_count):
            try:
                logger.info(f"Fetching {symbol} from {start_str} to {end_str} (attempt {attempt + 1})")
                
                result = self._wind.w.wsd(
                    symbol,
                    fields,
                    start_str,
                    end_str,
                    opt_str
                )
                
                if result.ErrorCode != 0:
                    logger.warning(f"Wind API error: {result.ErrorCode}")
                    if attempt < self.retry_count - 1:
                        time.sleep(self.retry_delay)
                        continue
                    raise Exception(f"Wind API error code: {result.ErrorCode}")
                
                # Convert to DataFrame
                df = self._convert_to_dataframe(result, fields)
                
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
    
    def fetch_intraday_data(
        self,
        symbol: str,
        start_time: Union[str, datetime],
        end_time: Union[str, datetime],
        bar_size: str = '1',
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch intraday/min bar data for a symbol.
        
        Args:
            symbol: Wind symbol
            start_time: Start datetime
            end_time: End datetime
            bar_size: Bar size in minutes ('1', '5', '15', '30', '60')
            fields: List of fields to fetch
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with intraday data
        """
        self._ensure_connected()
        
        # Default intraday fields
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume', 'amt']
        
        # Format times
        if isinstance(start_time, str):
            start_str = start_time
        else:
            start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            
        if isinstance(end_time, str):
            end_str = end_time
        else:
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Build options
        options = f"BarSize={bar_size}"
        
        for attempt in range(self.retry_count):
            try:
                logger.info(f"Fetching intraday {symbol} from {start_str} to {end_str}")
                
                result = self._wind.w.wsi(
                    symbol,
                    fields,
                    start_str,
                    end_str,
                    options
                )
                
                if result.ErrorCode != 0:
                    if attempt < self.retry_count - 1:
                        time.sleep(self.retry_delay)
                        continue
                    raise Exception(f"Wind API error code: {result.ErrorCode}")
                
                df = self._convert_to_dataframe(result, fields)
                return df
                
            except Exception as e:
                logger.error(f"Error fetching intraday data: {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
        
        return pd.DataFrame()
    
    def fetch_realtime_data(
        self,
        symbols: Union[str, List[str]],
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch realtime snapshot data.
        
        Args:
            symbols: Wind symbol or list of symbols
            fields: List of fields to fetch
            
        Returns:
            DataFrame with realtime data
        """
        self._ensure_connected()
        
        if isinstance(symbols, str):
            symbols = [symbols]
        
        if fields is None:
            fields = ['rt_last', 'rt_bid1', 'rt_ask1', 'rt_vol', 'rt_amt']
        
        for attempt in range(self.retry_count):
            try:
                result = self._wind.w.wsq(symbols, fields)
                
                if result.ErrorCode != 0:
                    if attempt < self.retry_count - 1:
                        time.sleep(self.retry_delay)
                        continue
                    raise Exception(f"Wind API error code: {result.ErrorCode}")
                
                # Convert to DataFrame
                data = {}
                for i, symbol in enumerate(symbols):
                    data[symbol] = {field: result.Data[j][i] for j, field in enumerate(fields)}
                
                df = pd.DataFrame(data).T
                df.index.name = 'symbol'
                return df
                
            except Exception as e:
                logger.error(f"Error fetching realtime data: {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
        
        return pd.DataFrame()
    
    def fetch_future_contracts(
        self,
        underlying: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        contract_months: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for all contracts of a futures underlying.
        
        Args:
            underlying: Underlying symbol (e.g., 'AU', 'IF', 'CU')
            start_date: Start date for data
            end_date: End date for data
            contract_months: List of contract months to fetch (e.g., ['01', '02', '03'])
            **kwargs: Additional parameters
            
        Returns:
            Dictionary mapping contract codes to DataFrames
        """
        self._ensure_connected()
        
        # Get exchange from underlying
        exchange_map = {
            'AU': 'SHF', 'AG': 'SHF', 'CU': 'SHF', 'AL': 'SHF', 'ZN': 'SHF',
            'RB': 'SHF', 'HC': 'SHF', 'RU': 'SHF', 'FU': 'SHF', 'BU': 'SHF',
            'C': 'DCE', 'CS': 'DCE', 'A': 'DCE', 'M': 'DCE', 'Y': 'DCE',
            'P': 'DCE', 'L': 'DCE', 'PP': 'DCE', 'J': 'DCE', 'I': 'DCE',
            'CF': 'ZCE', 'SR': 'ZCE', 'TA': 'ZCE', 'MA': 'ZCE', 'FG': 'ZCE',
            'IF': 'CFE', 'IC': 'CFE', 'IH': 'CFE', 'IM': 'CFE',
            'T': 'CFE', 'TF': 'CFE', 'TS': 'CFE', 'TL': 'CFE',
        }
        
        exchange = exchange_map.get(underlying.upper())
        if exchange is None:
            raise ValueError(f"Unknown underlying: {underlying}")
        
        # Generate contract codes
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date)
        
        contracts = self._generate_future_contracts(
            underlying, exchange, start_dt, end_dt, contract_months
        )
        
        # Fetch data for each contract
        results = {}
        for contract in contracts:
            try:
                df = self.fetch_data(contract, start_date, end_date, **kwargs)
                if not df.empty:
                    results[contract] = df
            except Exception as e:
                logger.warning(f"Failed to fetch {contract}: {e}")
                continue
        
        return results
    
    def _generate_future_contracts(
        self,
        underlying: str,
        exchange: str,
        start_date: datetime,
        end_date: datetime,
        contract_months: Optional[List[str]] = None
    ) -> List[str]:
        """
        Generate list of future contract codes.
        
        Args:
            underlying: Underlying symbol
            exchange: Exchange code
            start_date: Start date
            end_date: End date
            contract_months: Specific months to include
            
        Returns:
            List of contract codes (e.g., ['AU2406.SHF', 'AU2408.SHF'])
        """
        contracts = []
        
        # Default contract months based on exchange
        if contract_months is None:
            if exchange == 'SHF':
                contract_months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
            elif exchange == 'DCE':
                contract_months = ['01', '03', '05', '07', '09', '11']
            elif exchange == 'ZCE':
                contract_months = ['01', '03', '05', '07', '09', '11']
            elif exchange == 'CFE':
                if underlying in ['IF', 'IC', 'IH', 'IM']:
                    contract_months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
                else:
                    contract_months = ['03', '06', '09', '12']
            else:
                contract_months = ['01', '03', '05', '07', '09', '11']
        
        # Generate contracts for relevant years
        years = range(start_date.year - 1, end_date.year + 2)
        
        for year in years:
            yy = str(year)[-2:]
            for month in contract_months:
                contract = f"{underlying}{yy}{month}.{exchange}"
                contracts.append(contract)
        
        return contracts
    
    def _build_options(self, options: Optional[Dict[str, Any]] = None) -> str:
        """
        Build Wind API options string.
        
        Args:
            options: Dictionary of option key-value pairs
            
        Returns:
            Options string for Wind API
        """
        if options is None:
            return ""
        
        opt_parts = []
        for key, value in options.items():
            if isinstance(value, str):
                opt_parts.append(f"{key}={value}")
            elif isinstance(value, bool):
                opt_parts.append(f"{key}={'Yes' if value else 'No'}")
            else:
                opt_parts.append(f"{key}={value}")
        
        return ";".join(opt_parts)
    
    def _convert_to_dataframe(
        self,
        result,
        fields: List[str]
    ) -> pd.DataFrame:
        """
        Convert Wind API result to DataFrame.
        
        Args:
            result: Wind API result object
            fields: List of field names
            
        Returns:
            pandas DataFrame
        """
        # Create DataFrame from result
        data = {}
        
        # Times/Dates are in result.Times
        if hasattr(result, 'Times') and result.Times:
            data['date'] = result.Times
        elif hasattr(result, 'Data') and result.Data:
            # For timeseries, first column might be dates
            pass
        
        # Data values are in result.Data (list of lists, one per field)
        if hasattr(result, 'Data') and result.Data:
            for i, field in enumerate(fields):
                if i < len(result.Data):
                    data[field] = result.Data[i]
        
        # Codes are in result.Codes for multi-symbol queries
        if hasattr(result, 'Codes') and result.Codes:
            data['symbol'] = result.Codes
        
        df = pd.DataFrame(data)
        
        # Convert date column
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
        
        # Clean up NaN values
        df = df.replace(-2147483648, np.nan)  # Wind's null value for integers
        
        return df
    
    def get_trading_dates(
        self,
        exchange: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime]
    ) -> List[date]:
        """
        Get trading dates for an exchange.
        
        Args:
            exchange: Exchange code (e.g., 'SHF', 'DCE', 'CFE')
            start_date: Start date
            end_date: End date
            
        Returns:
            List of trading dates
        """
        self._ensure_connected()
        
        start_str = self._format_date(start_date, '%Y-%m-%d')
        end_str = self._format_date(end_date, '%Y-%m-%d')
        
        # Use trading calendar function
        try:
            result = self._wind.w.tdays(start_str, end_str, f"Exchange={exchange}")
            
            if result.ErrorCode == 0 and hasattr(result, 'Times'):
                return [t.date() for t in result.Times]
        except Exception as e:
            logger.error(f"Error getting trading dates: {e}")
        
        # Fallback: return all weekdays
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date)
        
        trading_dates = []
        current = start_dt
        while current <= end_dt:
            if current.weekday() < 5:  # Monday to Friday
                trading_dates.append(current.date())
            current += timedelta(days=1)
        
        return trading_dates