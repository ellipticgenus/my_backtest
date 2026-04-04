import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Callable, Optional, Union
from my_holiday.holiday_utils import apply_date_rule

RATES_CURVE = {
    'US':{
        'USGG1M': '28d',
        'USGG3M': '91d',
        'USGG6M': '182d',
        'USGG12M': '364d'
    }
}

class RatesCalculator:
    """
    Calculate funding costs based on treasury yield curves.
    Uses simple interest (discount rate) to calculate forward rates for piecewise periods.
    !!!!! Suitable for calculating funding costs for tenor less than or slightly greater than 1 year.
    !!!!! The method of simple interest we are using here are not accurate and propertate for long term funding cost calculation. 
    !!!!! For long term funding cost calculation, we should use compounding method to calculate forward rates.
    """
    
    def __init__(self, apply_date_rule: Callable = apply_date_rule, rates_curve: Dict[str, Dict[str, str]] = RATES_CURVE):
        self.rates_curve = rates_curve
        self.apply_date_rule = apply_date_rule
        self._tenor_cache = {}
        self._rate_periods_cache = {}
    

    def get_rates_symbols(self,country = 'US') -> List[str]:
        return list(self.rates_curve[country].keys())
    

    def calculate_funding_cost(
        self, 
        country: str, 
        start_date: str, 
        end_date: str,
        rates_data: Union[Dict[str, float], Dict[str, pd.Series]],
        observation_date: Optional[str] = None,
        calendar: str = 'US'
    ) -> Dict[str, Any]:
        """
        Calculate funding cost between two dates (includes start_date, excludes end_date).
        
        Args:
            country: Country code (e.g., 'US')
            start_date: Start date in 'YYYY-MM-DD' format (inclusive)
            end_date: End date in 'YYYY-MM-DD' format (exclusive)
            rates_data: Either:
                       - Dictionary mapping instrument names to annualized rates (as percentages)
                         e.g., {'USGG1M': 4.5, 'USGG3M': 4.6, ...}
                       - Dictionary mapping instrument names to pd.Series with dates as index
                         e.g., {'USGG1M': pd.Series([4.5, 4.6], index=[date1, date2]), ...}
            observation_date: Date to observe rates from time series (required if rates_data contains time series)
                             If not provided and rates_data is time series, defaults to start_date
            calendar: Calendar to use for date calculations (default: 'US')
            
        Returns:
            Dictionary containing:
                - total_cost: Total funding cost as a decimal (e.g., 0.05 for 5%)
                - annualized_rate: Effective annualized rate
                - days: Number of days in the period
                - breakdown: List of cost components by rate period
                - observation_date: The date used to observe rates (for time series)
                
        Raises:
            ValueError: If country not found or dates are invalid
        """
        if country not in self.rates_curve:
            raise ValueError(f"Country '{country}' not found in rates curve configuration")
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if end <= start:
            raise ValueError("End date must be after start date")
        
        is_time_series = self._is_time_series_data(rates_data)
        if observation_date is None:
            observation_date = start_date
        obs_date = datetime.strptime(observation_date, '%Y-%m-%d')

        if is_time_series:
            static_rates = self._extract_rates_at_date(rates_data, obs_date)
            result = self._calculate_funding_cost_static(
                country, start, end, static_rates, calendar, 
                observation_date=obs_date  
            )
            result['observation_date'] = observation_date
            return result
        else:
            result = self._calculate_funding_cost_static(
                country, start, end, rates_data, calendar,
                observation_date=obs_date  
            )
            result['observation_date'] = observation_date
            return result
    
    def calculate_funding_cost_batch(
        self,
        country: str,
        periods: List[tuple],  
        rates_data: Union[Dict[str, float], Dict[str, pd.Series]],
        observation_dates: Optional[List[str]] = None,
        calendar: str = 'US'
    ) -> List[Dict[str, Any]]:
        """
        Calculate funding costs for multiple periods efficiently.
        
        Args:
            country: Country code
            periods: List of (start_date, end_date) tuples
            rates_data: Rates data (static or time series)
            observation_dates: List of observation dates (one per period), optional
            calendar: Calendar name
            
        Returns:
            List of results, one per period
        """
        if observation_dates is None:
            observation_dates = [None] * len(periods)
        
        results = []
        
        is_time_series = self._is_time_series_data(rates_data)
        
        if is_time_series:
            preprocessed_series = self._preprocess_time_series(rates_data)
            
            for (start_date, end_date), obs_date in zip(periods, observation_dates):
                if obs_date is None:
                    obs_date = start_date
                
                obs_dt = datetime.strptime(obs_date, '%Y-%m-%d')

                static_rates = self._extract_rates_at_date_fast(
                    preprocessed_series, obs_dt
                )
                
                start = datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.strptime(end_date, '%Y-%m-%d')
                
                result = self._calculate_funding_cost_static(
                    country, start, end, static_rates, calendar,
                    observation_date=obs_dt  # Pass observation date
                )
                result['observation_date'] = obs_date
                results.append(result)
        else:
            for start_date, end_date in periods:
                result = self.calculate_funding_cost(
                    country, start_date, end_date, rates_data, 
                    observation_date=None, calendar=calendar
                )
                results.append(result)
        
        return results
    
    def _preprocess_time_series(
        self, 
        rates_data: Dict[str, pd.Series]
    ) -> Dict[str, pd.Series]:
        """
        Preprocess time series for faster lookups.
        Ensures datetime index, sorts, and forward fills.
        """
        preprocessed = {}
        
        for instrument, series in rates_data.items():
            if not isinstance(series.index, pd.DatetimeIndex):
                series = series.copy()
                series.index = pd.to_datetime(series.index)
            
            series = series.sort_index().ffill()
            preprocessed[instrument] = series
        
        return preprocessed
    
    def _extract_rates_at_date_fast(
        self,
        preprocessed_series: Dict[str, pd.Series],
        observation_date: datetime
    ) -> Dict[str, float]:
        """
        Fast extraction of rates using preprocessed series.
        """
        static_rates = {}
        target_ts = pd.Timestamp(observation_date)
        
        for instrument, series in preprocessed_series.items():
            idx = series.index.searchsorted(target_ts, side='right') - 1
            
            if idx < 0:
                static_rates[instrument] = series.iloc[0]
            else:
                static_rates[instrument] = series.iloc[idx]
        
        return static_rates
    
    def _is_time_series_data(self, rates_data: Union[Dict[str, float], Dict[str, pd.Series]]) -> bool:
        """
        Check if rates_data contains time series (pd.Series) or static values.
        """
        if not rates_data:
            return False
        
        first_value = next(iter(rates_data.values()))
        return isinstance(first_value, pd.Series)
    
    def _extract_rates_at_date(
        self, 
        rates_data: Dict[str, pd.Series],
        observation_date: datetime
    ) -> Dict[str, float]:
        """
        Extract rates from time series at a specific observation date.
        """
        static_rates = {}
        
        for instrument, series in rates_data.items():
            rate = self._get_rate_for_date(instrument, observation_date, series)
            static_rates[instrument] = rate
        
        return static_rates
    
    def _get_rate_for_date(
        self, 
        instrument: str, 
        target_date: datetime, 
        rates_series: pd.Series
    ) -> float:
        """
        Get the rate for a specific instrument and date from a time series.
        """
        if not isinstance(rates_series.index, pd.DatetimeIndex):
            rates_series.index = pd.to_datetime(rates_series.index)
        
        rates_series = rates_series.sort_index()
        target_ts = pd.Timestamp(target_date)
        
        valid_rates = rates_series[rates_series.index <= target_ts]
        
        if len(valid_rates) == 0:
            if len(rates_series) > 0:
                return rates_series.iloc[0]
            else:
                raise ValueError(f"No rate data available for instrument: {instrument}")
        
        return valid_rates.iloc[-1]
    
    def _calculate_funding_cost_static(
        self,
        country: str,
        start: datetime,
        end: datetime,
        rates_data: Dict[str, float],
        calendar: str,
        observation_date: datetime  # NEW: Required parameter
    ) -> Dict[str, Any]:
        """
        Calculate funding cost when rates_data contains static values.
        Optimized with caching.
        
        Args:
            country: Country code
            start: Start date of funding period
            end: End date of funding period
            rates_data: Dictionary mapping instruments to rates
            calendar: Calendar name
            observation_date: Date when rates were observed (used for tenor calculations)
            
        Returns:
            Dictionary with funding cost results
        """
        # Create cache key for rate periods - NOW INCLUDES OBSERVATION DATE
        rates_tuple = tuple(sorted(rates_data.items()))
        cache_key = (
            country, 
            observation_date.strftime('%Y-%m-%d'),  # Observation date, not start date!
            rates_tuple, 
            calendar
        )
        # Check cache
        if cache_key in self._rate_periods_cache:
            rate_periods = self._rate_periods_cache[cache_key]
        else:
            # Build piecewise rate structure with forward rates
            # Tenors are calculated from OBSERVATION DATE
            rate_periods = self._build_forward_rate_periods(
                country, 
                observation_date,  # Use observation date for tenor calculations
                rates_data, 
                calendar
            )
            
            # Store in cache (limit cache size)
            if len(self._rate_periods_cache) > 1000:
                keys_to_remove = list(self._rate_periods_cache.keys())[:500]
                for key in keys_to_remove:
                    del self._rate_periods_cache[key]
            
            self._rate_periods_cache[cache_key] = rate_periods

        # Calculate cost for each period
        total_cost = 0.0
        breakdown = []
        current_date = start
        
        # Optimized loop: pre-calculate conversion factor
        conversion_factor = 1.0 / 100.0 / 360.0
        
        while current_date < end:
            # Find applicable rate for current date
            rate_info = self._get_rate_at_date_optimized(rate_periods, current_date)
            
            if rate_info is None:
                break
            
            # Find the end of this rate period
            period_end = min(end, rate_info['end_date'])
            
            # Calculate days in this period (exclude end date)
            days_in_period = (period_end - current_date).days
            
            if days_in_period > 0:
                # Convert annualized rate to daily rate and calculate cost
                daily_rate = rate_info['forward_rate'] * conversion_factor
                period_cost = daily_rate * days_in_period
                
                total_cost += period_cost
                
                breakdown.append({
                    'start_date': current_date.strftime('%Y-%m-%d'),
                    'end_date': period_end.strftime('%Y-%m-%d'),
                    'days': days_in_period,
                    'forward_rate': rate_info['forward_rate'],
                    'daily_rate': round(daily_rate * 100, 8),
                    'period_cost': round(period_cost, 8),
                    'tenor_range': rate_info['tenor_range']
                })
            
            # Move to next period
            current_date = period_end
        
        # Calculate total days
        total_days = (end - start).days
        
        # Calculate effective annualized rate
        annualized_rate = (total_cost * 360.0 / total_days) * 100.0 if total_days > 0 else 0.0
        
        return {
            'country': country,
            'total_cost': round(total_cost, 8),
            'annualized_rate': round(annualized_rate, 6),
            'days': total_days,
            'start_date': start.strftime('%Y-%m-%d'),
            'end_date': end.strftime('%Y-%m-%d'),
            'breakdown': breakdown
        }
    
    def _build_forward_rate_periods(
        self, 
        country: str, 
        observation_date: datetime,  # RENAMED from start_date
        rates_data: Dict[str, float],
        calendar: str
    ) -> List[Dict[str, Any]]:
        """
        Build piecewise constant rate structure using forward rates.
        
        Tenors are calculated from the OBSERVATION DATE (when rates were observed),
        not from the funding start date.
        
        Args:
            country: Country code
            observation_date: Date when rates were observed
            rates_data: Dictionary of instrument rates
            calendar: Calendar for date calculations
            
        Returns:
            List of rate periods with forward rates
        """
        instruments = self.rates_curve[country]
        tenor_info = []
        
        # Collect all tenor information
        for instrument_name, tenor in instruments.items():
            if instrument_name not in rates_data:
                raise ValueError(f"Rate data missing for instrument: {instrument_name}")
            
            # Check tenor cache - keyed by OBSERVATION DATE
            tenor_cache_key = (observation_date.strftime('%Y-%m-%d'), tenor, calendar)
            
            if tenor_cache_key in self._tenor_cache:
                end_date = self._tenor_cache[tenor_cache_key]
            else:
                # Calculate the end date for this tenor from OBSERVATION DATE
                end_date = self.apply_date_rule(
                    observation_date.strftime('%Y-%m-%d'),
                    tenor,
                    calendar=calendar
                )
                
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')
                
                self._tenor_cache[tenor_cache_key] = end_date
            
            # Calculate days from observation date
            days_from_observation = (end_date - observation_date).days
            
            tenor_info.append({
                'instrument': instrument_name,
                'tenor': tenor,
                'end_date': end_date,
                'days': days_from_observation,
                'spot_rate': rates_data[instrument_name]
            })
        
        # Sort by days from observation
        tenor_info.sort(key=lambda x: x['days'])
        
        # Calculate forward rates for each period
        rate_periods = []
        days_to_years = 1.0 / 360.0
        
        for i, current_tenor in enumerate(tenor_info):
            if i == 0:
                # First period: use spot rate directly
                period_start_date = observation_date
                period_start_days = 0
                forward_rate = current_tenor['spot_rate']
                tenor_range = f"0-{current_tenor['tenor']}"
            else:
                # Subsequent periods: calculate forward rate
                prev_tenor = tenor_info[i - 1]
                period_start_date = prev_tenor['end_date']
                period_start_days = prev_tenor['days']
                
                t1 = prev_tenor['days'] * days_to_years
                t2 = current_tenor['days'] * days_to_years
                r1 = prev_tenor['spot_rate']
                r2 = current_tenor['spot_rate']
                
                if t2 - t1 <= 0:
                    forward_rate = r2
                else:
                    forward_rate = (r2 * t2 - r1 * t1) / (t2 - t1)
                
                tenor_range = f"{prev_tenor['tenor']}-{current_tenor['tenor']}"
            
            rate_periods.append({
                'start_date': period_start_date,
                'end_date': current_tenor['end_date'],
                'days': current_tenor['days'] - period_start_days,
                'forward_rate': forward_rate,
                'tenor_range': tenor_range,
                'instrument': current_tenor['instrument']
            })
        
        # Add a final period extending beyond the last tenor
        if rate_periods:
            last_tenor = tenor_info[-1]
            rate_periods.append({
                'start_date': last_tenor['end_date'],
                'end_date': datetime(2099, 12, 31),
                'days': None,
                'forward_rate': last_tenor['spot_rate'],
                'tenor_range': f"beyond {last_tenor['tenor']}",
                'instrument': last_tenor['instrument'] + ' (extended)'
            })
        
        return rate_periods
    
    def _get_rate_at_date_optimized(
        self, 
        rate_periods: List[Dict[str, Any]], 
        target_date: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Find the applicable rate period for a given date.
        """
        for period in rate_periods:
            if target_date < period['start_date']:
                return None
            if target_date < period['end_date']:
                return period
        
        return None
    
    def clear_cache(self):
        """Clear all internal caches."""
        self._tenor_cache.clear()
        self._rate_periods_cache.clear()