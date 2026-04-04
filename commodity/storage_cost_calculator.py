import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union, Tuple


COST_CURVE = {
    # https://www.cftc.gov/sites/default/files/filings/ptc/18/07/ptc071218cbotdcm001.pdf
    # https://www.cftc.gov/sites/default/files/stellent/groups/public/@rulesandproducts/documents/ifdocs/rul022608cbot001.pdf
    'S':{
        'type': 'fixed',
        'unit': '$/Bu',
        'curve': [
                {'date': '2000-01-01',
                        'value':  0.0015},
                {'date':'2008-11-01',
                        'value':  0.00165},
                {'date':'2019-11-19',
                        'value':  0.00265},
            ],
        'conversion_rate': 100 # convert to cents per bushel
        },
    'C':{
        'type': 'fixed',
        'unit': '$/Bu',
        'curve': [
                {'date': '2000-01-01',
                        'value':  0.0015},
                {'date':'2008-12-01',
                        'value':  0.00165},
                {'date':'2019-12-19',
                        'value':  0.00265},
            ],
        'conversion_rate': 100 # convert to cents per bushel
    },
    #https://www.cftc.gov/sites/default/files/filings/ptc/18/11/ptc111118cbotdcm001.pdf
    'SM':{
            'type': 'fixed',
            'unit': '$/ton',
            'curve': [
                    {'date': '2000-01-01',
                    'value': 0.07},
                    {'date':'2020-03-19',
                    'value':  0.12},
                ],
                'conversion_rate': 1.10231707 #convert to dollar  per short ton
        },
    'BO':{
        'type': 'fixed',
        'unit': '$/cwt', # cwt = 100 pbs
            'curve': [
                    {'date': '2000-01-01',
                    'value': 0.003},
                    {'date': '2020-03-19',
                    'value': 0.005},
                ],
        'conversion_rate': 1 #convert to cents per pounds
        },
}

class StorageCostCalculator:
    """
    Calculate storage costs for commodities based on configurable rate curves.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = COST_CURVE if config is None else config
        # Cache for parsed and sorted curves
        self._curve_cache = {}
        self._preprocess_curves()

    def _preprocess_curves(self):
        """Preprocess and cache sorted curves for each commodity."""
        for commodity, commodity_config in self.config.items():
            curve = commodity_config['curve']
            sorted_curve = sorted(
                curve, 
                key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d')
            )
            
            # Convert to arrays for faster access
            dates = np.array([
                datetime.strptime(entry['date'], '%Y-%m-%d') 
                for entry in sorted_curve
            ], dtype='datetime64[ns]')
            
            values = np.array([entry['value'] for entry in sorted_curve])
            conversion_rate = commodity_config.get('conversion_rate', 1)
            
            self._curve_cache[commodity] = {
                'dates': dates,
                'values': values * conversion_rate,
                'unit': commodity_config['unit']
            }

    def get_allowed_commodities(self) -> List[str]:
        """
        Get a list of allowed commodity symbols based on the configuration.
        
        Returns:
            List of commodity symbols (e.g., ['S', 'C', 'SM', 'BO'])
        """
        return list(self.config.keys())
    
    def calculate_storage_cost(self, commodity: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Calculate storage cost for a commodity between two dates.
        
        Args:
            commodity: Commodity symbol (e.g., 'S', 'SM', 'BO', 'C')
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Dictionary containing:
                - total_cost: Total storage cost
                - unit: Unit of the cost
                - breakdown: List of cost components by rate period
                
        Raises:
            ValueError: If commodity not found or dates are invalid
        """
        # Validate commodity exists
        if commodity not in self.config:
            raise ValueError(f"Commodity '{commodity}' not found in configuration")
        
        # Parse dates
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if end < start:
            raise ValueError("End date must be after start date")
        
        commodity_config = self.config[commodity]
        storage_type = commodity_config['type']
        unit = commodity_config['unit']
        curve = commodity_config['curve']
        conversion_rate = commodity_config.get('conversion_rate', 1)
        
        # Sort curve by date to ensure correct order
        sorted_curve = sorted(curve, key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))
        
        # Calculate cost
        total_cost = 0.0
        breakdown = []
        
        # Find applicable rate periods
        current_date = start
        
        while current_date < end:
            # Find the applicable rate for current_date
            rate_info = self._get_rate_at_date(sorted_curve, current_date)
            
            # Find when the next rate change occurs (if any)
            next_rate_date = self._get_next_rate_change_date(sorted_curve, current_date)
            
            # Determine the end of this rate period
            period_end = min(end, next_rate_date) if next_rate_date else end
            
            # Calculate cost for this period
            period_cost = self._calculate_period_cost(
                current_date, 
                period_end, 
                rate_info['value'] * conversion_rate, 
            )
            
            total_cost += period_cost
            
            breakdown.append({
                'start_date': current_date.strftime('%Y-%m-%d'),
                'end_date': period_end.strftime('%Y-%m-%d'),
                'rate': rate_info['value'] * conversion_rate,
                'cost': period_cost
            })
            
            # Move to next period
            current_date = period_end
        
        return {
            'commodity': commodity,
            'total_cost': round(total_cost, 6),
            'unit': unit,
            'start_date': start_date,
            'end_date': end_date,
            'breakdown': breakdown
        }
    
    def calculate_storage_cost_batch(
        self,
        commodity: Union[str, List[str]],
        periods: Union[List[Tuple[str, str]], pd.DataFrame],
        include_breakdown: bool = False
    ) -> pd.DataFrame:
        """
        Calculate storage costs for multiple periods efficiently.
        
        Args:
            commodity: Single commodity or list of commodities
                      If single, applies to all periods
                      If list, must match length of periods
            periods: Either:
                    - List of (start_date, end_date) tuples
                    - DataFrame with 'start_date' and 'end_date' columns
                      (and optionally 'commodity' column)
            include_breakdown: If True, include breakdown column in result
            
        Returns:
            DataFrame with columns:
                - commodity: Commodity symbol
                - start_date: Start date
                - end_date: End date
                - total_cost: Total storage cost
                - unit: Cost unit
                - breakdown: (optional) List of breakdown dicts
                
        Examples:
            # Single commodity, multiple periods
            periods = [('2024-01-01', '2024-06-01'), ('2024-02-01', '2024-07-01')]
            df = calculator.calculate_storage_cost_batch('S', periods)
            
            # Multiple commodities
            commodities = ['S', 'C', 'SM']
            df = calculator.calculate_storage_cost_batch(commodities, periods)
            
            # DataFrame input with commodity column
            input_df = pd.DataFrame({
                'commodity': ['S', 'C', 'SM'],
                'start_date': ['2024-01-01', '2024-01-01', '2024-01-01'],
                'end_date': ['2024-06-01', '2024-06-01', '2024-06-01']
            })
            df = calculator.calculate_storage_cost_batch(None, input_df)
        """
        # Parse input
        if isinstance(periods, pd.DataFrame):
            df_input = periods.copy()
            if 'commodity' not in df_input.columns:
                if isinstance(commodity, str):
                    df_input['commodity'] = commodity
                elif isinstance(commodity, list):
                    if len(commodity) != len(df_input):
                        raise ValueError("Length of commodity list must match DataFrame length")
                    df_input['commodity'] = commodity
                else:
                    raise ValueError("Commodity must be specified")
        else:
            # periods is a list of tuples
            if isinstance(commodity, str):
                commodities = [commodity] * len(periods)
            elif isinstance(commodity, list):
                if len(commodity) != len(periods):
                    raise ValueError("Length of commodity list must match periods length")
                commodities = commodity
            else:
                raise ValueError("Commodity must be specified")
            
            df_input = pd.DataFrame({
                'commodity': commodities,
                'start_date': [p[0] for p in periods],
                'end_date': [p[1] for p in periods]
            })
        
        # Calculate costs
        results = []
        
        for _, row in df_input.iterrows():
            commodity = row['commodity']
            start_date = row['start_date']
            end_date = row['end_date']
            
            result = self.calculate_storage_cost(commodity, start_date, end_date)
            
            result_row = {
                'commodity': result['commodity'],
                'start_date': result['start_date'],
                'end_date': result['end_date'],
                'total_cost': result['total_cost'],
                'unit': result['unit']
            }
            
            if include_breakdown:
                result_row['breakdown'] = result['breakdown']
            
            results.append(result_row)
        
        return pd.DataFrame(results)
    
    def calculate_storage_cost_batch_optimized(
        self,
        commodity: str,
        periods: Union[List[Tuple[str, str]], pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Optimized batch calculation for a single commodity.
        
        Args:
            commodity: Commodity symbol
            periods: List of (start_date, end_date) tuples or DataFrame
            
        Returns:
            DataFrame with storage cost results
        """
        if commodity not in self.config:
            raise ValueError(f"Commodity '{commodity}' not found in configuration")
        
        # Parse periods
        if isinstance(periods, pd.DataFrame):
            start_dates = pd.to_datetime(periods['start_date'])
            end_dates = pd.to_datetime(periods['end_date'])
        else:
            start_dates = pd.to_datetime([p[0] for p in periods])
            end_dates = pd.to_datetime([p[1] for p in periods])
        
        # Get commodity config
        commodity_config = self.config[commodity]
        curve = commodity_config['curve']
        conversion_rate = commodity_config.get('conversion_rate', 1)
        unit = commodity_config['unit']
        
        # Sort curve - use pandas Timestamps throughout
        sorted_curve = sorted(curve, key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))
        rate_dates = pd.to_datetime([entry['date'] for entry in sorted_curve])
        rate_values = [entry['value'] * conversion_rate for entry in sorted_curve]
        
        # Calculate costs
        costs = []
        
        for start, end in zip(start_dates, end_dates):
            if end <= start:
                costs.append(0.0)
                continue
            
            total_cost = 0.0
            current_date = start
            rate_idx = 0  # Start from first rate
            
            while current_date < end:
                # Move to appropriate rate (linear scan, but cached)
                while rate_idx < len(rate_dates) - 1 and rate_dates[rate_idx + 1] <= current_date:
                    rate_idx += 1
                
                rate = rate_values[rate_idx]
                
                # Find next rate change
                if rate_idx + 1 < len(rate_dates):
                    next_rate_date = rate_dates[rate_idx + 1]
                    period_end = min(end, next_rate_date)
                else:
                    period_end = end
                
                # Calculate period cost
                days = (period_end - current_date).days
                total_cost += days * rate
                
                current_date = period_end
            
            costs.append(round(total_cost, 6))
        
        # Create result DataFrame
        # start_dates and end_dates are already DatetimeIndex, use strftime directly
        result_df = pd.DataFrame({
            'commodity': commodity,
            'start_date': start_dates.strftime('%Y-%m-%d'),
            'end_date': end_dates.strftime('%Y-%m-%d'),
            'total_cost': costs,
            'unit': unit
        })
        
        return result_df
        
    def _get_rate_at_date(self, sorted_curve: List[Dict], target_date: datetime) -> Dict[str, Any]:
        """
        Find the applicable rate for a given date.
        
        Args:
            sorted_curve: List of rate entries sorted by date
            target_date: Date to find rate for
            
        Returns:
            Dictionary with 'value'
        """
        applicable_rate = sorted_curve[0]  # Default to first rate
        
        for rate_entry in sorted_curve:
            rate_date = datetime.strptime(rate_entry['date'], '%Y-%m-%d')
            if rate_date <= target_date:
                applicable_rate = rate_entry
            else:
                break
        
        return {
            'value': applicable_rate['value'],
        }
    
    def _get_next_rate_change_date(self, sorted_curve: List[Dict], current_date: datetime) -> datetime | None:
        """
        Find the next date when the rate changes.
        
        Args:
            sorted_curve: List of rate entries sorted by date
            current_date: Current date
            
        Returns:
            Next rate change date or None if no future changes
        """
        for rate_entry in sorted_curve:
            rate_date = datetime.strptime(rate_entry['date'], '%Y-%m-%d')
            if rate_date > current_date:
                return rate_date
        return None
    
    def _calculate_period_cost(self, start: datetime, end: datetime, rate: float) -> float:
        """
        Calculate cost for a specific period.
        
        Args:
            start: Period start date
            end: Period end date
            rate: Rate value            
        Returns:
            Cost for the period
        """
        days = (end - start).days
        return days * rate
