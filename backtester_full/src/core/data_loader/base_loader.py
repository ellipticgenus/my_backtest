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
            config: Configuration dictionary with optional keys:
                - base_path: Base path for data files (default: 'data')
                - cache_enabled: Whether to cache loaded data (default: True)
        """
        self.config = config or {}
        self.base_path = self.config.get('base_path', self.DEFAULT_DATA_PATH)
        self.cache_enabled = self.config.get('cache_enabled', True)
        self._cache: Dict[str, pd.DataFrame] = {}
    
    def _get_file_path(
        self, 
        filename: str, 
        subfolder: str = '',
        extension: Optional[str] = None
    ) -> str:
        """
        Construct the full file path.
        
        Args:
            filename: Name of the file (without extension)
            subfolder: Subfolder within the data directory
            extension: File extension ('csv' or 'parquet'). If None, tries both.
            
        Returns:
            Full file path
        """
        if subfolder:
            return os.path.join(self.base_path, subfolder, filename)
        return os.path.join(self.base_path, filename)
    
    def _load_file(
        self, 
        filepath: str, 
        extension: str = 'csv',
        date_column: str = 'date'
    ) -> pd.DataFrame:
        """
        Load a data file (CSV or Parquet).
        
        Args:
            filepath: Path to the file (without extension)
            extension: File extension ('csv' or 'parquet')
            date_column: Name of the date column to parse
            
        Returns:
            Loaded DataFrame
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        # Check cache first
        cache_key = f"{filepath}.{extension}"
        if self.cache_enabled and cache_key in self._cache:
            return self._cache[cache_key].copy()
        
        full_path = f"{filepath}.{extension}"
        
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {full_path}")
        
        if extension == 'parquet':
            df = pd.read_parquet(full_path)
        elif extension == 'csv':
            df = pd.read_csv(full_path)
            if date_column in df.columns:
                df[date_column] = pd.to_datetime(df[date_column])
        else:
            raise ValueError(f"Unsupported extension: {extension}")
        
        # Cache the result
        if self.cache_enabled:
            self._cache[cache_key] = df.copy()
        
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
            subfolder: Subfolder within the data directory
            extension: Preferred file extension ('csv' or 'parquet')
            date_column: Name of the date column to parse
            
        Returns:
            Loaded DataFrame
        """
        filepath = self._get_file_path(filename, subfolder)
        
        # Remove extension from filename if present
        for ext in ['.csv', '.parquet']:
            if filepath.endswith(ext):
                filepath = filepath[:-len(ext)]
                break
        
        # Try preferred extension first, then fallback
        extensions_to_try = [extension]
        for ext in ['csv', 'parquet']:
            if ext not in extensions_to_try:
                extensions_to_try.append(ext)
        
        for ext in extensions_to_try:
            try:
                return self._load_file(filepath, ext, date_column)
            except FileNotFoundError:
                continue
        
        raise FileNotFoundError(f"Could not find file: {filepath} with any supported extension")
    
    def set_index_by_date(
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