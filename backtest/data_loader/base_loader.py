"""
Base Loader Class for Data Loading.

Provides abstract base class and common utilities for data loading.
Supports both CSV and Parquet formats.
"""

import pandas as pd
import os
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Union


class BaseLoader(ABC):
    """
    Abstract base class for data loaders.
    
    Provides common functionality for loading data from CSV and Parquet files.
    """
    
    # Default data path relative to project root
    DEFAULT_DATA_PATH = 'data'
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the base loader.
        
        Args:
            config: Configuration dictionary for the loader
        """
        self.config = config or {}
        self._cache: Dict[str, pd.DataFrame] = {}
        self.cache_enabled = self.config.get('cache_enabled', True)
    
    def _resolve_path(
        self,
        filename: str,
        subfolder: str = '',
        extension: str = 'csv'
    ) -> str:
        """
        Resolve the full path to a data file.
        
        Args:
            filename: Name of the file (without extension)
            subfolder: Subfolder within data directory
            extension: File extension ('csv' or 'parquet')
            
        Returns:
            Full path to the file
        """
        base_path = self.config.get('data_path', self.DEFAULT_DATA_PATH)
        
        if subfolder:
            return os.path.join(base_path, subfolder, f"{filename}.{extension}")
        return os.path.join(base_path, f"{filename}.{extension}")
    
    def _load_file(
        self,
        filepath: str,
        extension: str = 'csv',
        date_column: str = 'date',
        parse_dates: bool = True
    ) -> pd.DataFrame:
        """
        Load a data file (CSV or Parquet).
        
        Args:
            filepath: Path to the file (without extension)
            extension: File extension ('csv' or 'parquet')
            date_column: Name of the date column
            parse_dates: Whether to parse dates
            
        Returns:
            Loaded DataFrame
        """
        full_path = f"{filepath}.{extension}" if not filepath.endswith(f".{extension}") else filepath
        
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {full_path}")
        
        if extension == 'parquet':
            df = pd.read_parquet(full_path)
        elif extension == 'csv':
            df = pd.read_csv(full_path)
            if parse_dates and date_column in df.columns:
                df[date_column] = pd.to_datetime(df[date_column])
        else:
            raise ValueError(f"Unsupported extension: {extension}")
        
        # Cache the result
        if self.cache_enabled:
            self._cache[full_path] = df.copy()
        
        return df
    
    def load_data(
        self,
        filename: str,
        subfolder: str = '',
        extension: str = 'csv',
        date_column: str = 'date'
    ) -> pd.DataFrame:
        """
        Load data from a file, trying multiple extensions if needed.
        
        Args:
            filename: Name of the file (with or without extension)
            subfolder: Subfolder within data directory
            extension: File extension ('csv' or 'parquet')
            date_column: Name of the date column
            
        Returns:
            Loaded DataFrame
        """
        # Check cache first
        cache_key = f"{subfolder}/{filename}.{extension}"
        if self.cache_enabled and cache_key in self._cache:
            return self._cache[cache_key].copy()
        
        # Try the specified extension first
        try:
            return self._load_file(
                self._resolve_path(filename, subfolder, extension),
                extension,
                date_column
            )
        except FileNotFoundError:
            # Try alternative extension
            alt_extension = 'parquet' if extension == 'csv' else 'csv'
            try:
                return self._load_file(
                    self._resolve_path(filename, subfolder, alt_extension),
                    alt_extension,
                    date_column
                )
            except FileNotFoundError:
                raise FileNotFoundError(f"File not found: {filename} (tried .{extension} and .{alt_extension})")
    
    def _set_index_by_date(
        self,
        df: pd.DataFrame,
        date_column: str = 'date'
    ) -> pd.DataFrame:
        """
        Set the date column as the DataFrame index.
        
        Args:
            df: DataFrame to process
            date_column: Name of the date column
            
        Returns:
            DataFrame with date as index
        """
        if date_column in df.columns:
            df = df.set_index(date_column)
        return df
    
    def clear_cache(self):
        """Clear the data cache."""
        self._cache.clear()
    
    @abstractmethod
    def load(self, *args, **kwargs) -> pd.DataFrame:
        """
        Abstract method for loading data.
        
        Must be implemented by subclasses.
        """
        pass