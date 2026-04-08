"""
Wind Data Pipeline.

Orchestrates the data download, processing, and storage workflow.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List, Union
from pathlib import Path

import pandas as pd
import numpy as np

from data_api.base import BaseDataPipeline
from data_api.wind.downloader import WindDownloader
from data_api.wind.config import WindConfig,  get_symbol_type

# Configure logging
logger = logging.getLogger(__name__)


class WindPipeline(BaseDataPipeline):
    """
    Wind data pipeline for downloading and processing market data.
    
    Handles the complete workflow:
    1. Connect to Wind Terminal
    2. Download historical data
    3. Validate data quality
    4. Process/clean data
    5. Save to local storage
    
    Example:
        >>> config = WindConfig(data_path='data/wind')
        >>> downloader = WindDownloader(config.to_dict())
        >>> pipeline = WindPipeline(downloader, config.to_dict())
        >>> 
        >>> # Download single symbol
        >>> result = pipeline.run_single('AU.SHF', '2024-01-01', '2024-12-31')
        >>> 
        >>> # Download all commodity futures
        >>> result = pipeline.run_batch(
        ...     symbols=['A2601.DCE', 'A2603.DCE', 'A2601.DCE', 'A2605.DCE'],
        ...     start_date='2025-01-01',
        ...     end_date='2025-12-31'
        ... )
    """
    
    def __init__(
        self,
        downloader: WindDownloader,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Wind pipeline.
        
        Args:
            downloader: WindDownloader instance
            config: Configuration dictionary
        """
        super().__init__(downloader, config)
        self.config_obj = WindConfig(**{k: v for k, v in config.items() 
                                        if k in WindConfig.__dataclass_fields__})
        self._downloaded_data: Dict[str, pd.DataFrame] = {}
        
    def run(
        self,
        symbols: Union[str, List[str]],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        fields: Optional[List[str]] = None,
        save_to_disk: bool = True,
        file_format: str = 'csv',
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run the complete pipeline for multiple symbols.
        
        Args:
            symbols: Symbol or list of symbols to download
            start_date: Start date for data
            end_date: End date for data
            fields: Fields to fetch
            save_to_disk: Whether to save data to disk
            file_format: File format ('csv' or 'parquet')
            **kwargs: Additional parameters
            
        Returns:
            Dictionary containing results and status
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        
        self._update_status('status', 'running')
        self._update_status('start_time', datetime.now().isoformat())
        self._update_status('total_symbols', len(symbols))
        
        results = {
            'success': [],
            'failed': [],
            'data': {},
            'errors': {}
        }
        
        # Connect to Wind
        if not self.downloader.is_connected():
            if not self.downloader.connect():
                self._update_status('status', 'failed')
                self._update_status('error', 'Failed to connect to Wind Terminal')
                results['error'] = 'Failed to connect to Wind Terminal'
                return results
        
        # Download data for each symbol
        for i, symbol in enumerate(symbols):
            try:
                logger.info(f"Processing {symbol} ({i+1}/{len(symbols)})")
                
                # Fetch data
                df = self.downloader.fetch_data(
                    symbol, start_date, end_date, fields, **kwargs
                )
                
                if df.empty:
                    logger.warning(f"No data returned for {symbol}")
                    results['failed'].append(symbol)
                    continue
                
                # Validate
                if not self.validate_data(df):
                    logger.warning(f"Validation failed for {symbol}")
                    results['failed'].append(symbol)
                    continue
                
                # Process
                df = self.process_data(df, symbol=symbol)
                
                # Save to disk
                if save_to_disk:
                    self._save_data(df, symbol, file_format)
                
                results['success'].append(symbol)
                results['data'][symbol] = df
                self._downloaded_data[symbol] = df
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                results['failed'].append(symbol)
                results['errors'][symbol] = str(e)
        
        self._update_status('status', 'completed')
        self._update_status('end_time', datetime.now().isoformat())
        self._update_status('success_count', len(results['success']))
        self._update_status('failed_count', len(results['failed']))
        
        return results
    
    def run_single(
        self,
        symbol: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        fields: Optional[List[str]] = None,
        save_to_disk: bool = True,
        file_format: str = 'csv',
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run pipeline for a single symbol.
        
        Args:
            symbol: Symbol to download
            start_date: Start date
            end_date: End date
            fields: Fields to fetch
            save_to_disk: Whether to save to disk
            file_format: File format ('csv' or 'parquet')
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with result data
        """
        results = self.run(
            symbols=[symbol],
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            save_to_disk=save_to_disk,
            file_format=file_format,
            **kwargs
        )
        
        return {
            'success': symbol in results['success'],
            'data': results['data'].get(symbol),
            'error': results['errors'].get(symbol)
        }
    
    def run_batch(
        self,
        symbols: List[str],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        fields: Optional[List[str]] = None,
        save_to_disk: bool = True,
        file_format: str = 'csv',
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run pipeline for a batch of symbols.
        
        Same as run() method, provided for clarity.
        """
        return self.run(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            save_to_disk=save_to_disk,
            file_format=file_format,
            **kwargs
        )
    
    def run_futures(
        self,
        underlying: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        contract_months: Optional[List[str]] = None,
        save_to_disk: bool = True,
        file_format: str = 'csv',
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run pipeline for all contracts of a futures underlying.
        
        Args:
            underlying: Underlying symbol (e.g., 'AU', 'IF', 'CU')
            start_date: Start date
            end_date: End date
            contract_months: Specific contract months
            save_to_disk: Whether to save to disk
            file_format: File format
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with results
        """
        self._update_status('status', 'running')
        self._update_status('start_time', datetime.now().isoformat())
        
        results = {
            'underlying': underlying,
            'success': [],
            'failed': [],
            'data': {},
            'errors': {}
        }
        
        # Connect to Wind
        if not self.downloader.is_connected():
            if not self.downloader.connect():
                results['error'] = 'Failed to connect to Wind Terminal'
                return results
        
        # Fetch all contracts
        try:
            contracts_data = self.downloader.fetch_future_contracts(
                underlying, start_date, end_date, contract_months, **kwargs
            )
            
            for contract, df in contracts_data.items():
                if df.empty:
                    results['failed'].append(contract)
                    continue
                
                # Validate and process
                if self.validate_data(df):
                    df = self.process_data(df, symbol=contract)
                    
                    if save_to_disk:
                        # Save to subfolder named after underlying
                        self._save_data(df, contract.replace('.', '_'), file_format, 
                                       subfolder=f'futures/{underlying}')
                    
                    results['success'].append(contract)
                    results['data'][contract] = df
                else:
                    results['failed'].append(contract)
                    
        except Exception as e:
            logger.error(f"Error fetching futures for {underlying}: {e}")
            results['error'] = str(e)
        
        self._update_status('status', 'completed')
        self._update_status('end_time', datetime.now().isoformat())
        
        return results
    
    def update_existing(
        self,
        symbol: str,
        end_date: Union[str, date, datetime] = None,
        lookback_days: int = 10,
        file_format: str = 'csv',
        **kwargs
    ) -> Dict[str, Any]:
        """
        Update existing data file by appending new data.
        
        Args:
            symbol: Symbol to update
            end_date: End date (default: today)
            lookback_days: Days to look back for overlap validation
            file_format: File format
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with update results
        """
        result = {
            'symbol': symbol,
            'success': False,
            'records_added': 0,
            'error': None
        }
        
        try:
            # Load existing data
            existing_df = self._load_existing_data(symbol, file_format)
            
            if existing_df is None or existing_df.empty:
                # No existing data, do full download
                start_date = '2000-01-01'
            else:
                # Start from last date
                last_date = existing_df.index.max()
                start_date = (last_date - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            # Download new data
            new_df = self.downloader.fetch_data(symbol, start_date, end_date, **kwargs)
            
            if new_df.empty:
                result['error'] = 'No new data downloaded'
                return result
            
            # Merge with existing
            if existing_df is not None and not existing_df.empty:
                combined_df = self._merge_data(existing_df, new_df, lookback_days)
            else:
                combined_df = new_df
            
            # Validate and process
            if self.validate_data(combined_df):
                combined_df = self.process_data(combined_df, symbol=symbol)
                self._save_data(combined_df, symbol, file_format)
                
                result['success'] = True
                result['records_added'] = len(combined_df) - (len(existing_df) if existing_df is not None else 0)
                result['data'] = combined_df
            else:
                result['error'] = 'Validation failed'
                
        except Exception as e:
            logger.error(f"Error updating {symbol}: {e}")
            result['error'] = str(e)
        
        return result
    
    def validate_data(
        self,
        df: pd.DataFrame,
        check_columns: Optional[List[str]] = None,
        max_nan_ratio: float = 0.5,
        **kwargs
    ) -> bool:
        """
        Validate downloaded data.
        
        Args:
            df: DataFrame to validate
            check_columns: Columns to check for data quality
            max_nan_ratio: Maximum allowed ratio of NaN values
            **kwargs: Additional validation parameters
            
        Returns:
            True if validation passes
        """
        if df is None or df.empty:
            logger.warning("DataFrame is empty")
            return False
        
        # Check for required columns
        if check_columns is None:
            check_columns = ['close']
        
        for col in check_columns:
            if col not in df.columns:
                # Try to find similar column
                similar = [c for c in df.columns if col.lower() in c.lower()]
                if not similar:
                    logger.warning(f"Required column '{col}' not found")
                    return False
        
        # Check for NaN ratio
        for col in check_columns:
            if col in df.columns:
                nan_ratio = df[col].isna().sum() / len(df)
                if nan_ratio > max_nan_ratio:
                    logger.warning(f"Column '{col}' has {nan_ratio:.1%} NaN values (max: {max_nan_ratio:.1%})")
                    return False
        
        # Check for duplicate indices
        if df.index.duplicated().any():
            logger.warning("DataFrame has duplicate indices")
            return False
        
        # Check for sorted index
        if not df.index.is_monotonic_increasing:
            logger.info("Index not sorted, will be sorted during processing")
        
        return True
    
    def process_data(
        self,
        df: pd.DataFrame,
        symbol: Optional[str] = None,
        calculate_returns: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """
        Process/clean the downloaded data.
        
        Args:
            df: Raw DataFrame
            symbol: Symbol name (for logging)
            calculate_returns: Whether to calculate returns
            **kwargs: Additional processing parameters
            
        Returns:
            Processed DataFrame
        """
        df = df.copy()
        
        # Ensure index is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
        
        # Sort by date
        df = df.sort_index()
        
        # Remove duplicates
        df = df[~df.index.duplicated(keep='last')]
        
        # Standardize column names
        df.columns = df.columns.str.lower()
        # Add symbol column if provided
        if symbol:
            df['symbol'] = symbol
        
        return df
    
    def _save_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        file_format: str = 'csv',
        subfolder: str = ''
    ) -> str:
        """
        Save data to disk.
        
        Args:
            df: DataFrame to save
            symbol: Symbol name
            file_format: 'csv' or 'parquet'
            subfolder: Subfolder path
            
        Returns:
            Path to saved file
        """
        # Clean symbol for filename
        filename = symbol.replace('.', '_').replace(':', '_')
        
        if file_format == 'parquet':
            return self.downloader.save_to_parquet(df, filename, subfolder)
        else:
            return self.downloader.save_to_csv(df, filename, subfolder)
    
    def _load_existing_data(
        self,
        symbol: str,
        file_format: str = 'csv'
    ) -> Optional[pd.DataFrame]:
        """
        Load existing data file.
        
        Args:
            symbol: Symbol name
            file_format: File format
            
        Returns:
            DataFrame or None if not found
        """
        filename = symbol.replace('.', '_').replace(':', '_')
        
        try:
            if file_format == 'parquet':
                return self.downloader.load_from_csv(filename)  # TODO: add parquet loading
            else:
                return self.downloader.load_from_csv(filename)
        except FileNotFoundError:
            return None
    
    def _merge_data(
        self,
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame,
        lookback_days: int = 10
    ) -> pd.DataFrame:
        """
        Merge existing and new data with validation.
        
        Args:
            existing_df: Existing DataFrame
            new_df: New DataFrame
            lookback_days: Days to validate overlap
            
        Returns:
            Merged DataFrame
        """
        # Find overlap
        overlap_start = new_df.index.min()
        overlap_end = existing_df.index.max()
        
        if overlap_start <= overlap_end:
            # Validate overlapping data
            overlap_dates = existing_df.index[
                (existing_df.index >= overlap_start) & (existing_df.index <= overlap_end)
            ]
            
            if 'close' in existing_df.columns and 'close' in new_df.columns:
                for date in overlap_dates:
                    if date in new_df.index:
                        existing_val = existing_df.loc[date, 'close']
                        new_val = new_df.loc[date, 'close']
                        
                        if pd.notna(existing_val) and pd.notna(new_val):
                            if not np.isclose(existing_val, new_val, rtol=1e-6):
                                logger.warning(
                                    f"Data mismatch on {date}: "
                                    f"existing={existing_val}, new={new_val}"
                                )
        
        # Combine data - keep existing for overlap, add new
        combined = pd.concat([existing_df, new_df[~new_df.index.isin(existing_df.index)]])
        combined = combined.sort_index()
        
        return combined
    
    def get_downloaded_data(self, symbol: Optional[str] = None) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Get downloaded data from cache.
        
        Args:
            symbol: Specific symbol, or None for all
            
        Returns:
            DataFrame or dictionary of DataFrames
        """
        if symbol:
            return self._downloaded_data.get(symbol)
        return self._downloaded_data.copy()
    
    def clear_downloaded_data(self) -> None:
        """Clear downloaded data cache."""
        self._downloaded_data.clear()


# Convenience function for quick downloads
def download_wind_data(
    symbols: Union[str, List[str]],
    start_date: Union[str, date, datetime],
    end_date: Union[str, date, datetime],
    data_path: str = 'data/wind',
    fields: Optional[List[str]] = None,
    save_to_disk: bool = True,
    file_format: str = 'csv'
) -> Dict[str, Any]:
    """
    Convenience function to quickly download Wind data.
    
    Args:
        symbols: Symbol or list of symbols
        start_date: Start date
        end_date: End date
        data_path: Path to save data
        fields: Fields to fetch
        save_to_disk: Whether to save to disk
        file_format: File format
        
    Returns:
        Dictionary with results
    """
    config = WindConfig(data_path=data_path)
    downloader = WindDownloader(config.to_dict())
    pipeline = WindPipeline(downloader, config.to_dict())
    
    return pipeline.run(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        save_to_disk=save_to_disk,
        file_format=file_format
    )