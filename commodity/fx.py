import numpy as np
import pandas as pd
from typing import Union, Optional, List, Dict
import warnings
from datetime import datetime
import re
from dataclasses import dataclass
warnings.filterwarnings('ignore')


@dataclass
class FXConverterConfig:
    """Configuration for an FX converter."""
    interpolation_method: str = 'linear'
    fx_name: str = 'USD'

class FXForwardConverter:
    """
    FX forward price converter with exact date arithmetic.
    Converts future prices in other currency to USD using forward rates with tenor codes.
    """
    
    def __init__(self, df: pd.DataFrame, interpolation_method: str = 'linear',fx_name: str = 'USD'):
        """       
        Parameters:
        -----------
        df : pd.DataFrame
            Must contain columns: 'date', 'nearby', 'value'
            where relative_date is tenor codes like ['SPOT', '1M', '2M', '2Y', etc.]
        interpolation_method : str
            'linear' for linear interpolation, 'nearest' for nearest neighbor
        """
        self.interpolation_method = interpolation_method
        self.name = fx_name
        self._prepare_data(df)
    
    def update_data(self, df: pd.DataFrame, interpolation_method: Optional[str] = None):
        self.update_interpolation_method(interpolation_method)
        self._prepare_data(df)

    def update_interpolation_method(self, interpolation_method: str):
        if interpolation_method not in ['linear', 'nearest']:
            raise ValueError(f"Invalid interpolation method: {interpolation_method}. Use 'linear' or 'nearest'.")
        self.interpolation_method = interpolation_method

    def get_available_dates(self) -> list:
        """Get list of available dates in the forward curve."""
        if hasattr(self, 'unique_dates'):
            return sorted(pd.to_datetime(self.unique_dates).date.tolist())
        return []
    
    def get_available_tenors(self) -> list:
        """Get list of available tenors in the forward curve."""
        if hasattr(self, 'sorted_tenors'):
            return self.sorted_tenors.copy()
        return []
    
    def get_data_info(self) -> dict:
        """Get information about the current data."""
        return {
            'fx_name': self.fx_name,
            'interpolation_method': self.interpolation_method,
            'data_points': len(self.df),
            'date_range': {
                'min': self.df['date'].min().date() if not self.df.empty else None,
                'max': self.df['date'].max().date() if not self.df.empty else None
            },
            'tenor_range': {
                'min': self.df['relative_date'].min() if not self.df.empty else None,
                'max': self.df['relative_date'].max() if not self.df.empty else None
            },
            'last_update': self.last_update if hasattr(self, 'last_update') else None
        }

    def _parse_tenor_to_offset(self, tenor: str):
        """
        Parse tenor string to pandas DateOffset.
        
        Examples:
        - 'SPOT' → 0 days
        - '1D' → pd.DateOffset(days=1)
        - '1W' → pd.DateOffset(weeks=1)
        - '1M' → pd.DateOffset(months=1)  # Actual calendar month
        - '2M' → pd.DateOffset(months=2)
        - '1Y' → pd.DateOffset(years=1)   # Actual calendar year
        - '18M' → pd.DateOffset(months=18)
        """
        tenor = str(tenor).strip().upper()
        
        # Handle special cases
        if tenor in ['SPOT', '0D', '0M', '0Y']:
            return pd.DateOffset(days=0)
        elif tenor == 'TOD':
            return pd.DateOffset(days=0)
        elif tenor == 'TOM':
            return pd.DateOffset(days=1)
        elif tenor == 'SN':
            return pd.DateOffset(days=2)
        
        # Parse numeric tenor with unit
        match = re.match(r'(\d+)([DWMY])', tenor)
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            
            if unit == 'D':
                return pd.DateOffset(days=value)
            elif unit == 'W':
                return pd.DateOffset(weeks=value)
            elif unit == 'M':
                return pd.DateOffset(months=value)  # Real calendar months
            elif unit == 'Y':
                return pd.DateOffset(years=value)   # Real calendar years
        
        # Try to parse if it's already a number of days
        try:
            days = int(tenor)
            return pd.DateOffset(days=days)
        except:
            raise ValueError(f"Unknown tenor format: {tenor}")
    
    def _calculate_expiration_date_exact(self, current_date: pd.Timestamp, tenor: str) -> pd.Timestamp:
        offset = self._parse_tenor_to_offset(tenor)
        return current_date + offset
    
    def _calculate_days_between_exact(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
        return (end_date - start_date).days
    
    def _tenor_to_days_for_sorting(self, tenor: str) -> int:
        """
        This is just to order tenors correctly, not for date calculation.
        """
        tenor = str(tenor).strip().upper()
        
        if tenor in ['SPOT', '0D', '0M', '0Y', 'TOD']:
            return 0
        elif tenor == 'TOM':
            return 1
        elif tenor == 'SN':
            return 2
        
        match = re.match(r'(\d+)([DWMY])', tenor)
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            
            if unit == 'D':
                return value
            elif unit == 'W':
                return value * 7
            elif unit == 'M':
                return value * 30  # Approximation for sorting only
            elif unit == 'Y':
                return value * 365  # Approximation for sorting only
        
        try:
            return int(tenor)
        except:
            return 0
    
    def _sort_tenors(self, tenors: List[str]) -> List[str]:
        """Sort tenor codes by their approximate day equivalent for ordering."""
        tenor_days = [(tenor, self._tenor_to_days_for_sorting(tenor)) for tenor in tenors]
        tenor_days.sort(key=lambda x: x[1])
        return [tenor for tenor, _ in tenor_days]
    
    def _prepare_data(self, df: pd.DataFrame):
        self.df = df.copy()
        self.last_update = pd.Timestamp.now()

        df = df.rename(columns={'date': 'date', 'nearby': 'relative_date', 'value': 'forward_price'})
        # Ensure date columns are datetime
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date']).dt.normalize()
        
        # Store tenor codes
        df['tenor'] = df['relative_date'].astype(str).str.upper()
        
        # Get unique tenors and sort them
        unique_tenors = df['tenor'].unique().tolist()
        self.sorted_tenors = self._sort_tenors(unique_tenors)
        
        # Pre-calculate expiration dates for each (current_date, tenor) pair
        # This ensures exact calendar arithmetic
        print("Pre-calculating exact expiration dates...")
        df['expiration_date'] = df.apply(
            lambda row: self._calculate_expiration_date_exact(row['date'], row['tenor']),
            axis=1
        )
        
        # Convert to numpy arrays for performance
        self.dates = df['date'].values.astype('datetime64[ns]')
        self.expiration_dates = df['expiration_date'].values.astype('datetime64[ns]')
        self.forward_prices = df['forward_price'].values.astype(np.float64)
        self.tenors = df['tenor'].values.astype('U10')  # Up to 10 character strings
        
        # Create mapping from date+tenor to index for fast lookup
        self._create_lookup_structures(df)
        
        # Store for reference
        self.df = df
        self.unique_dates = np.unique(self.dates)
        self.unique_tenors = np.unique(self.tenors)
    
    def _create_lookup_structures(self, df: pd.DataFrame):
        """Create optimized lookup structures."""
        # Build exact match cache
        self._exact_cache = {}
        for idx, row in df.iterrows():
            date_key = row['date'].to_datetime64().astype('int64')
            tenor_key = str(row['tenor']).upper()
            key = (date_key, tenor_key)
            self._exact_cache[key] = float(row['forward_price'])
        
        # Group data by current date for interpolation
        self._date_groups = {}
        
        for idx, date in enumerate(self.dates):
            date_key = date.astype('int64')
            if date_key not in self._date_groups:
                self._date_groups[date_key] = {
                    'exp_dates': [],
                    'prices': [],
                    'tenors': [],
                    'indices': []
                }
            
            self._date_groups[date_key]['exp_dates'].append(self.expiration_dates[idx])
            self._date_groups[date_key]['prices'].append(self.forward_prices[idx])
            self._date_groups[date_key]['tenors'].append(self.tenors[idx])
            self._date_groups[date_key]['indices'].append(idx)
        
        # Convert lists to numpy arrays and sort by expiration date
        for date_key in self._date_groups:
            group = self._date_groups[date_key]
            
            # Sort by expiration date
            sort_idx = np.argsort(group['exp_dates'])
            
            group['exp_dates'] = np.array(group['exp_dates'])[sort_idx]
            group['prices'] = np.array(group['prices'])[sort_idx]
            group['tenors'] = np.array(group['tenors'])[sort_idx]
            group['indices'] = np.array(group['indices'])[sort_idx]
            
            # Store as int64 for faster comparison
            group['exp_dates_int'] = group['exp_dates'].astype('int64')
    
    def convert_to_usd(
        self,
        input_dates: Union[pd.DatetimeIndex, np.ndarray, list, pd.Series],
        future_prices_ccy: Union[np.ndarray, list, pd.Series],
        current_dates: Optional[Union[pd.DatetimeIndex, np.ndarray, list, pd.Series]] = None,
        input_tenors: Optional[Union[np.ndarray, list, pd.Series]] = None # for fx 
    ) -> np.ndarray:
        """
        Convert future prices to USD expiration prices.
        
        Parameters:
        -----------
        input_dates : array-like of dates
            Expiration dates OR current dates if tenors are provided
        future_prices_ccy : array-like of floats
            Future prices in another currency
        current_dates : array-like of dates, optional
            Current/valuation dates. Required if input_dates are expiration dates
        input_tenors : array-like of tenor strings, optional
            Tenor codes. If provided, input_dates are treated as current dates
            
        Returns:
        --------
        np.ndarray of USD prices
        """
        # Convert inputs to proper format
        if input_tenors is not None:
            # input_dates are current dates, tenors specify the term
            current_dates_np = self._to_timestamp(input_dates)
            tenors_np = np.asarray(input_tenors, dtype=str)
            
            # Calculate exact expiration dates
            expiration_dates_np = np.array([
                self._calculate_expiration_date_exact(pd.Timestamp(current_date), tenor)
                for current_date, tenor in zip(current_dates_np, tenors_np)
            ]).astype('datetime64[ns]')
        else:
            # input_dates are expiration dates
            expiration_dates_np = self._to_datetime64(input_dates)
            
            if current_dates is None:
                raise ValueError("current_dates must be provided when input_dates are expiration dates")
            current_dates_np = self._to_datetime64(current_dates)
        
        future_prices_np = np.asarray(future_prices_ccy, dtype=np.float64)
        
        # Validate lengths
        if len(expiration_dates_np) != len(future_prices_np):
            raise ValueError("Number of dates and prices must match")
        if input_tenors is None and len(expiration_dates_np) != len(current_dates_np):
            raise ValueError("expiration_dates and current_dates must have same length")
        
        # Get forward rates
        forward_rates = self._get_forward_rates_vectorized(
            current_dates_np if input_tenors is None else self._to_datetime64(input_dates),
            expiration_dates_np,
            input_tenors
        )
        
        # Convert: USD = CCY / forward_rate
        usd_prices = future_prices_np / forward_rates
        
        return usd_prices
    
    def _to_datetime64(self, dates):
        """Convert to numpy datetime64[ns]."""
        if isinstance(dates, pd.DatetimeIndex):
            return dates.to_numpy(dtype='datetime64[ns]')
        elif isinstance(dates, pd.Series):
            return dates.values.astype('datetime64[ns]')
        elif isinstance(dates, list):
            return np.array([np.datetime64(pd.Timestamp(d).date(), 'ns') for d in dates])
        elif isinstance(dates, np.ndarray):
            if dates.dtype.kind == 'M':
                return dates.astype('datetime64[ns]')
            else:
                return np.array([np.datetime64(pd.Timestamp(d).date(), 'ns') for d in dates])
        return dates
    
    def _to_timestamp(self, dates):
        """Convert to pandas Timestamp for date arithmetic."""
        if isinstance(dates, (pd.DatetimeIndex, pd.Series)):
            return dates
        elif isinstance(dates, np.ndarray):
            if dates.dtype.kind == 'M':
                return pd.DatetimeIndex(dates)
            else:
                return pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
        elif isinstance(dates, list):
            return pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
        return dates
    
    def _get_forward_rates_vectorized(self, current_dates, expiration_dates, input_tenors=None):
        """
        Vectorized forward rate lookup with exact date matching.
        """
        n = len(current_dates)
        forward_rates = np.empty(n, dtype=np.float64)
        
        # Convert to int64 for fast comparison
        current_int = current_dates.astype('int64')
        expiration_int = expiration_dates.astype('int64')
        
        # Process in batches for better cache performance
        batch_size = 1000
        
        for batch_start in range(0, n, batch_size):
            batch_end = min(batch_start + batch_size, n)
            
            for i in range(batch_start, batch_end):
                current_date_int = current_int[i]
                expiration_date_int = expiration_int[i]
                
                # Check if we have an exact tenor match
                if input_tenors is not None:
                    tenor = str(input_tenors[i]).upper()
                    exact_key = (current_date_int, tenor)
                    if exact_key in self._exact_cache:
                        forward_rates[i] = self._exact_cache[exact_key]
                        continue
                
                # Get rate with interpolation
                if current_date_int in self._date_groups:
                    group = self._date_groups[current_date_int]
                    forward_rates[i] = self._interpolate_for_date(
                        expiration_date_int,
                        group['exp_dates_int'],
                        group['prices']
                    )
                else:
                    # Current date not in data, find nearest
                    forward_rates[i] = self._get_rate_from_nearest_date(
                        current_date_int, expiration_date_int
                    )
        
        return forward_rates
    
    def _interpolate_for_date(self, target_date_int, available_dates_int, available_prices):
        """Interpolate forward rate for target date."""
        if self.interpolation_method == 'nearest':
            idx = np.argmin(np.abs(available_dates_int - target_date_int))
            return available_prices[idx]
        
        # Linear interpolation
        if target_date_int <= available_dates_int[0]:
            return available_prices[0]
        elif target_date_int >= available_dates_int[-1]:
            return available_prices[-1]
        
        # Find bracketing indices
        idx = np.searchsorted(available_dates_int, target_date_int)
        lower_idx = idx - 1
        upper_idx = idx
        
        lower_date = available_dates_int[lower_idx]
        upper_date = available_dates_int[upper_idx]
        
        if upper_date == lower_date:
            return available_prices[lower_idx]
        
        # Linear interpolation in time
        weight = (target_date_int - lower_date) / (upper_date - lower_date)
        rate = available_prices[lower_idx] * (1 - weight) + available_prices[upper_idx] * weight
        
        return rate
    
    def _get_rate_from_nearest_date(self, current_date_int, target_date_int):
        """Get forward rate from nearest available current date."""
        # Find nearest current date
        available_dates_int = np.array(list(self._date_groups.keys()))
        nearest_idx = np.argmin(np.abs(available_dates_int - current_date_int))
        nearest_date_int = available_dates_int[nearest_idx]
        
        # Use forward curve from nearest date
        group = self._date_groups[nearest_date_int]
        return self._interpolate_for_date(
            target_date_int,
            group['exp_dates_int'],
            group['prices']
        )
    
    def get_forward_rate(
        self, 
        current_date, 
        expiration_date=None, 
        tenor=None,
        interpolate=True
    ) -> float:
        """
        Get forward rate for a single date/tenor combination.
        
        Parameters:
        -----------
        current_date : date-like
            Current/valuation date
        expiration_date : date-like, optional
            Expiration date (provide either this or tenor)
        tenor : str, optional
            Tenor code (provide either this or expiration_date)
        interpolate : bool
            Whether to interpolate if exact match not found
            
        Returns:
        --------
        float: Forward rate (USD/CCY)
        """
        current_date_ts = pd.Timestamp(current_date)
        current_date_np = np.datetime64(current_date_ts.date())
        current_date_int = current_date_np.astype('int64')
        
        if tenor is not None:
            # Calculate exact expiration date
            tenor = str(tenor).upper()
            expiration_date_ts = self._calculate_expiration_date_exact(current_date_ts, tenor)
            expiration_date_np = np.datetime64(expiration_date_ts.date())
            
            # Check exact match
            exact_key = (current_date_int, tenor)
            if exact_key in self._exact_cache:
                return self._exact_cache[exact_key]
            
            if not interpolate:
                raise ValueError(f"No exact forward rate found for tenor {tenor} on {current_date}")
        elif expiration_date is not None:
            expiration_date_ts = pd.Timestamp(expiration_date)
            expiration_date_np = np.datetime64(expiration_date_ts.date())
        else:
            raise ValueError("Must provide either expiration_date or tenor")
        
        expiration_date_int = expiration_date_np.astype('int64')
        
        # Get rate with interpolation
        if current_date_int in self._date_groups:
            group = self._date_groups[current_date_int]
            return self._interpolate_for_date(
                expiration_date_int,
                group['exp_dates_int'],
                group['prices']
            )
        else:
            return self._get_rate_from_nearest_date(current_date_int, expiration_date_int)


class FXHelper:
    def __init__(self):
        self.converters: Dict[str, 'FXForwardConverter'] = {}
        self.configs: Dict[str, FXConverterConfig] = {}

    def add_converter(
        self, 
        df: pd.DataFrame, 
        fx_name: str = 'USD',
        interpolation_method: str = 'linear',
        force_update: bool = True):
     
        if fx_name in self.converters:
            if force_update:
                self.update_converter(df, fx_name, interpolation_method)
            return          
        converter = FXForwardConverter(df, interpolation_method, fx_name)
        self.converters[fx_name] = converter
        config = FXConverterConfig(
            interpolation_method=interpolation_method,
            fx_name=fx_name
        )
        self.configs[fx_name] = config

    def update_converter(
        self, 
        df: pd.DataFrame, 
        fx_name: str = 'USD',
        interpolation_method: Optional[str] = None
        ):
        if fx_name not in self.converters:
            raise KeyError(f"No converter found for '{fx_name}'. Use add_converter() first.")
        
        config = self.configs[fx_name]
        new_method = interpolation_method if interpolation_method is not None else config.interpolation_method
        converter = self.converters[fx_name]
        converter.update_data(df, new_method)
        
        if interpolation_method is not None:
            config.interpolation_method = interpolation_method

    def remove_converter(self, fx_name: str) -> None:
        if fx_name in self.converters:
            del self.converters[fx_name]
            del self.configs[fx_name]

    def get_converter(self, fx_name: str) -> 'FXForwardConverter':
        if fx_name not in self.converters:
            raise KeyError(f"No converter found for '{fx_name}'")
        return self.converters[fx_name]
    
    def list_converters(self) -> list:
        return list(self.converters.keys())
    
    def get_converter_info(self, fx_name: str) -> dict:
        if fx_name not in self.converters:
            raise KeyError(f"No converter found for '{fx_name}'")
        
        converter = self.converters[fx_name]
        config = self.configs[fx_name]
        
        return {
            'fx_name': fx_name,
            'interpolation_method': config.interpolation_method,
            'available_dates': converter.get_available_dates(),
            'available_tenors': converter.get_available_tenors(),
            'data_points': len(converter.df) if hasattr(converter, 'df') else 0
        }
    

    def convert_to_usd(
        self,
        fx_name: str,
        input_dates: Union[pd.DatetimeIndex, np.ndarray, list, pd.Series],
        future_prices_brl: Union[np.ndarray, list, pd.Series],
        current_dates: Optional[Union[pd.DatetimeIndex, np.ndarray, list, pd.Series]] = None,
        input_tenors: Optional[Union[np.ndarray, list, pd.Series]] = None
    ) -> np.ndarray:
        """
        Parameters:
        -----------
        fx_name : str
            Name of the FX pair to use
        input_dates : array-like
            Expiration dates OR current dates if tenors are provided
        future_prices_brl : array-like
            Future prices in BRL
        current_dates : array-like, optional
            Current/valuation dates
        input_tenors : array-like, optional
            Tenor codes
            
        Returns:
        --------
        np.ndarray of USD prices
        """
        converter = self.get_converter(fx_name)
        return converter.convert_to_usd(
            input_dates, future_prices_brl, current_dates, input_tenors
        )
    

    def batch_convert(
        self,
        fx_names: list,
        input_dates: list,
        future_prices_brl: list,
        current_dates: Optional[list] = None,
        input_tenors: Optional[list] = None
    ) -> Dict[str, np.ndarray]:
        """
        Convert prices using multiple FX converters.        
        Parameters:
        -----------
        fx_names : list
            List of FX pair names to use
        input_dates : list of array-like
            Input dates for each converter
        future_prices_brl : list of array-like
            Future prices for each converter
        current_dates : list of array-like, optional
            Current dates for each converter
        input_tenors : list of array-like, optional
            Tenor codes for each converter
            
        Returns:
        --------
        dict
            Dictionary mapping FX names to converted price arrays
        """
        if len(fx_names) != len(input_dates) or len(fx_names) != len(future_prices_brl):
            raise ValueError("All input lists must have the same length")
        
        results = {}
        for i, fx_name in enumerate(fx_names):
            converter = self.get_converter(fx_name)
            
            curr_dates = current_dates[i] if current_dates else None
            tenors = input_tenors[i] if input_tenors else None
            
            results[fx_name] = converter.convert_to_usd(
                input_dates[i], 
                future_prices_brl[i], 
                curr_dates, 
                tenors
            )
        
        return results
    
    def __contains__(self, fx_name: str) -> bool:
        """Check if a converter exists."""
        return fx_name in self.converters
    
    def __getitem__(self, fx_name: str) -> 'FXForwardConverter':
        """Get converter using dictionary-like syntax."""
        return self.get_converter(fx_name)
    
    def __len__(self) -> int:
        """Get number of converters."""
        return len(self.converters)
    
    def __repr__(self) -> str:
        """String representation of the helper."""
        return f"FXHelper with {len(self)} converters: {list(self.converters.keys())}"
    
fx_helper = FXHelper()