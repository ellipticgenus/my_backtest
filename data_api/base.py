"""
Base classes for Data API.

Provides abstract base classes for data downloading and pipeline management.
"""

import os
import logging
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Union
from pathlib import Path

import pandas as pd
import numpy as np


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataDownloader(ABC):
    """
    Abstract base class for data downloaders.
    
    Provides common functionality for downloading data from various sources.
    Subclasses must implement the actual data fetching logic.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the data downloader.
        
        Args:
            config: Configuration dictionary containing:
                - data_path: Base path for saving data (default: 'data')
                - cache_enabled: Whether to enable caching (default: True)
                - retry_count: Number of retries on failure (default: 3)
                - retry_delay: Delay between retries in seconds (default: 1)
        """
        self.config = config or {}
        self.data_path = Path(self.config.get('data_path', 'data'))
        self.cache_enabled = self.config.get('cache_enabled', True)
        self.retry_count = self.config.get('retry_count', 3)
        self.retry_delay = self.config.get('retry_delay', 1)
        self._cache: Dict[str, pd.DataFrame] = {}
        self._connected = False
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Connect to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Disconnect from the data source.
        
        Returns:
            True if disconnection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if connected to the data source.
        
        Returns:
            True if connected, False otherwise
        """
        pass
    
    @abstractmethod
    def fetch_data(
        self,
        symbol: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch data for a single symbol.
        
        Args:
            symbol: Symbol/ticker to fetch
            start_date: Start date for data
            end_date: End date for data
            **kwargs: Additional parameters specific to the data source
            
        Returns:
            DataFrame with the fetched data
        """
        pass
    
    @abstractmethod
    def fetch_data_batch(
        self,
        symbols: List[str],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for multiple symbols.
        
        Args:
            symbols: List of symbols/tickers to fetch
            start_date: Start date for data
            end_date: End date for data
            **kwargs: Additional parameters specific to the data source
            
        Returns:
            Dictionary mapping symbols to DataFrames
        """
        pass
    
    def _ensure_connected(self) -> None:
        """Ensure connection to data source."""
        if not self.is_connected():
            if not self.connect():
                raise ConnectionError("Failed to connect to data source")
    
    def _parse_date(self, d: Union[str, date, datetime]) -> datetime:
        """
        Parse date from various formats.
        
        Args:
            d: Date in various formats (str, date, datetime)
            
        Returns:
            datetime object
        """
        if isinstance(d, datetime):
            return d
        elif isinstance(d, date):
            return datetime.combine(d, datetime.min.time())
        elif isinstance(d, str):
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%d-%m-%Y', '%d/%m/%Y']:
                try:
                    return datetime.strptime(d, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse date: {d}")
        else:
            raise TypeError(f"Unsupported date type: {type(d)}")
    
    def _format_date(self, d: Union[str, date, datetime], fmt: str = '%Y-%m-%d') -> str:
        """
        Format date to string.
        
        Args:
            d: Date in various formats
            fmt: Output format string
            
        Returns:
            Formatted date string
        """
        dt = self._parse_date(d)
        return dt.strftime(fmt)
    
    def save_to_csv(
        self,
        df: pd.DataFrame,
        filename: str,
        subfolder: str = '',
        **kwargs
    ) -> str:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Name of the file (without extension)
            subfolder: Subfolder within data directory
            **kwargs: Additional arguments passed to pd.to_csv
            
        Returns:
            Full path to the saved file
        """
        if subfolder:
            full_path = self.data_path / subfolder
        else:
            full_path = self.data_path
            
        full_path.mkdir(parents=True, exist_ok=True)
        filepath = full_path / f"{filename}.csv"
        
        df.to_csv(filepath, **kwargs)
        logger.info(f"Saved data to {filepath}")
        
        return str(filepath)
    
    def save_to_parquet(
        self,
        df: pd.DataFrame,
        filename: str,
        subfolder: str = '',
        **kwargs
    ) -> str:
        """
        Save DataFrame to Parquet file.
        
        Args:
            df: DataFrame to save
            filename: Name of the file (without extension)
            subfolder: Subfolder within data directory
            **kwargs: Additional arguments passed to pd.to_parquet
            
        Returns:
            Full path to the saved file
        """
        if subfolder:
            full_path = self.data_path / subfolder
        else:
            full_path = self.data_path
            
        full_path.mkdir(parents=True, exist_ok=True)
        filepath = full_path / f"{filename}.parquet"
        
        df.to_parquet(filepath, **kwargs)
        logger.info(f"Saved data to {filepath}")
        
        return str(filepath)
    
    def load_from_csv(
        self,
        filename: str,
        subfolder: str = '',
        parse_dates: bool = True,
        date_column: str = 'date',
        **kwargs
    ) -> pd.DataFrame:
        """
        Load DataFrame from CSV file.
        
        Args:
            filename: Name of the file (without extension)
            subfolder: Subfolder within data directory
            parse_dates: Whether to parse date columns
            date_column: Name of the date column
            **kwargs: Additional arguments passed to pd.read_csv
            
        Returns:
            Loaded DataFrame
        """
        if subfolder:
            filepath = self.data_path / subfolder / f"{filename}.csv"
        else:
            filepath = self.data_path / f"{filename}.csv"
        
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        df = pd.read_csv(filepath, **kwargs)
        
        if parse_dates and date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column])
        
        return df
    
    def clear_cache(self) -> None:
        """Clear the data cache."""
        self._cache.clear()
        logger.info("Cache cleared")


class BaseDataPipeline(ABC):
    """
    Abstract base class for data pipelines.
    
    Orchestrates the data download, processing, and storage workflow.
    """
    
    def __init__(
        self,
        downloader: DataDownloader,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the data pipeline.
        
        Args:
            downloader: DataDownloader instance for fetching data
            config: Configuration dictionary for the pipeline
        """
        self.downloader = downloader
        self.config = config or {}
        self._pipeline_status: Dict[str, Any] = {}
        
    @abstractmethod
    def run(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Run the complete pipeline.
        
        Returns:
            Dictionary containing pipeline results and status
        """
        pass
    
    @abstractmethod
    def validate_data(self, df: pd.DataFrame, **kwargs) -> bool:
        """
        Validate downloaded data.
        
        Args:
            df: DataFrame to validate
            **kwargs: Additional validation parameters
            
        Returns:
            True if validation passes, False otherwise
        """
        pass
    
    @abstractmethod
    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Process/clean the downloaded data.
        
        Args:
            df: Raw DataFrame to process
            **kwargs: Additional processing parameters
            
        Returns:
            Processed DataFrame
        """
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current pipeline status.
        
        Returns:
            Dictionary containing pipeline status information
        """
        return self._pipeline_status.copy()
    
    def _update_status(self, key: str, value: Any) -> None:
        """
        Update pipeline status.
        
        Args:
            key: Status key
            value: Status value
        """
        self._pipeline_status[key] = value
        self._pipeline_status['last_updated'] = datetime.now().isoformat()