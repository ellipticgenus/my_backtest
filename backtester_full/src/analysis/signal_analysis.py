# The script is used for analyzing signals

from unittest import result
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
import matplotlib.pyplot as plt

def calculate_rank_ic(_df_return, _df_signal, forward_return_window=5):
    rank_ics = []
    dates = []
    return_signals = _df_return.shift(-forward_return_window-1)
    return_signals.dropna(inplace=True )
    for date in return_signals.index:
        signal_ranks = _df_signal.loc[date].rank()
        return_ranks = return_signals.loc[date].rank()
        ic,_ = spearmanr(signal_ranks, return_ranks)
        rank_ics.append(ic)
        dates.append(date)
    
    return pd.DataFrame({'date': dates, 'ic': rank_ics})

def calculate_ic(_df_return, _df_signal, forward_return_window=5):
    ics = []
    dates = []
    return_signals = _df_return.shift(-forward_return_window-1)
    return_signals.dropna(inplace=True )
    for date in return_signals.index:
        signals = _df_signal.loc[date]
        returns = return_signals.loc[date]
        ic,_ = spearmanr(signals, returns)
        ics.append(ic)
        dates.append(date)
    
    return pd.DataFrame({'date': dates, 'ic': ics})

def calculate_timeseries_ic(_df_return, _df_signal, forward_return_window=5):
    tc_ics = []
    return_signals = _df_return.shift(-forward_return_window-1)
    return_signals.dropna(inplace=True )
    _df_signal = _df_signal.loc[return_signals.index]
    for contract in _df_signal.columns:
        ic,_ = spearmanr(return_signals[contract], _df_signal[contract])
        tc_ics.append(ic)

    return pd.DataFrame({'contract': _df_signal.columns, 'ic': tc_ics})

def calculate_rolling_ic(_df_return, _df_signal, forward_return_window=5, rolling_window=60):
    """
    Calculates the rolling Spearman Rank Information Coefficient (IC) between signals and future returns.

    Parameters:
        _df_return (pd.DataFrame): DataFrame of price returns. Index should be datetime, columns are contracts.
        _df_signal (pd.DataFrame): DataFrame of signals. Index and columns should align with _df_return.
        forward_return_window (int): The number of periods ahead to calculate the return for. Default is 5.
        rolling_window (int): The number of periods to use for the rolling correlation calculation. Default is 60.

    Returns:
        pd.DataFrame: A DataFrame with the same index as the input (after shifting and window adjustments),
                      and columns for each contract's rolling IC. Also includes a 'mean_ic' column.
    """
    
    # 1. Align the Data: Ensure both DataFrames have the same index and columns
    # We use the intersection of indices and columns to avoid NaN issues
    common_index = _df_return.index.intersection(_df_signal.index).copy()
    common_cols = _df_return.columns.intersection(_df_signal.columns).copy()
    
    aligned_returns = _df_return.loc[common_index, common_cols]
    aligned_signals = _df_signal.loc[common_index, common_cols]
    
    # 2. Create the Forward Returns
    # Shift backwards so that the return at time T is for the period [T, T+forward_return_window]
    forward_returns = aligned_returns.shift(-forward_return_window-1)
    
    # 3. Pre-allocate a DataFrame to store the rolling ICs
    # The index will be the same as aligned_signals, but we will lose the first (rolling_window - 1) days
    rolling_ics = pd.DataFrame(index=aligned_signals.index, columns=common_cols)
    
    # 4. Loop through each day to calculate the rolling IC
    for i in range(rolling_window, len(aligned_signals)):
        # Current end date for the window
        current_date = aligned_signals.index[i]
        # Start of the window (rolling_window days back)
        start_index = i - rolling_window
        window_dates = aligned_signals.index[start_index:i]
        
        # Get the signal and forward return data for the current 60-day window
        signal_window = aligned_signals.loc[window_dates]
        return_window = forward_returns.loc[window_dates]
        
        # Calculate IC for each contract in this window
        for contract in common_cols:
            # Drop NaNs that appear in either series for this specific contract and window
            combined_data = pd.concat([signal_window[contract], return_window[contract]], axis=1).dropna()
            if len(combined_data) < 2: # Spearmanr requires at least 2 observations
                ic = np.nan
            else:
                ic, _ = spearmanr(combined_data.iloc[:, 0], combined_data.iloc[:, 1])
            rolling_ics.loc[current_date, contract] = ic
    
    # 5. Calculate the cross-sectional mean IC for each day
    rolling_ics['mean_ic'] = rolling_ics[common_cols].mean(axis=1, skipna=True)
    
    # 6. Drop the initial rows where the rolling window was not full
    rolling_ics = rolling_ics.iloc[rolling_window:]
    
    return rolling_ics

def calculate_binary_hit_ic(_df_return, _df_signal, forward_return_window=5, top_n=3):
    hit_rates = []
    return_contracts = _df_return.shift(-forward_return_window-1)
    return_contracts.dropna(inplace=True )
    dates = []

    for date in return_contracts.index:
        top_contracts = _df_signal.loc[date].nlargest(top_n).index
        bottom_contracts = _df_signal.loc[date].nsmallest(top_n).index
        top_mean_return = return_contracts.loc[date, top_contracts].mean()
        bottom_mean_return = return_contracts.loc[date, bottom_contracts].mean()
        hit_rates.append(int(top_mean_return > bottom_mean_return))
        dates.append(date)

    hit_ratess = pd.DataFrame({'date': dates, 'hit_rate': hit_rates})
    return hit_ratess



def basket_returns(signal_df: pd.DataFrame, return_df: pd.DataFrame, n_baskets: int = 5,freq = 1 ) -> pd.DataFrame:
   
    # Validate inputs
    if not signal_df.index.equals(return_df.index):
        raise ValueError("Signal and return DataFrames must have the same index (dates)")
    if not signal_df.columns.equals(return_df.columns):
        raise ValueError("Signal and return DataFrames must have the same columns (assets)")
    
    basket_returns = {}
    count = 0
    ret_quantiles= []
    for date in signal_df.index:
        temp = {}
        temp['date'] = date
        # Get signals and returns for current date
        signals = signal_df.loc[date]
        returns = return_df.loc[date]
        
        # Remove NaN values
        valid_mask = signals.notna() & returns.notna()
        signals_valid = signals[valid_mask]
        returns_valid = returns[valid_mask]
        
        if len(signals_valid) == 0:
            # Skip dates with no valid data
            continue
        
        # Create baskets based on signal quantiles
        if count % freq == 0:
            try:
                quantiles = pd.qcut(signals_valid, n_baskets, labels=False, duplicates='drop')
            except ValueError:
                # Handle cases where not enough unique values for quantiles
                continue
        # print((quantiles))

        temp = temp|quantiles.to_dict()
        # print(temp)
        # break
        # print(quantiles)
        # break
        # Calculate average return for each basket
        for basket in range(len(quantiles.categories) if hasattr(quantiles, 'categories') else n_baskets):
            basket_mask = (quantiles == basket)
            if basket_mask.any():
                basket_avg_return = returns_valid[basket_mask].mean()
                basket_key = f'basket_{basket+1}'  # Basket 1 is lowest signal, basket_n is highest
                
                if basket_key not in basket_returns:
                    basket_returns[basket_key] = {}
                basket_returns[basket_key][date] = basket_avg_return
        ret_quantiles.append( temp )
        count +=1
    # Convert to DataFrame
    result_df = pd.DataFrame(basket_returns)
    quantiles_df = pd.DataFrame(ret_quantiles)
    # Add spread return (basket_n - basket_1)
    if 'basket_1' in result_df.columns and f'basket_{n_baskets}' in result_df.columns:
        result_df['spread'] = result_df[f'basket_{n_baskets}'] - result_df['basket_1']
    
    return result_df ,quantiles_df




def basket_returns_with_costs(
    signal_df: pd.DataFrame, 
    return_df: pd.DataFrame, 
    n_baskets: int = 5,
    transaction_cost: float = 0.007  # 0.7% cost
) -> pd.DataFrame:
    
    # Validate inputs
    if not signal_df.index.equals(return_df.index):
        raise ValueError("Signal and return DataFrames must have the same index")
    if not signal_df.columns.equals(return_df.columns):
        raise ValueError("Signal and return DataFrames must have the same columns")
    
    # Initialize results and track previous basket assignments
    basket_results = {}
    prev_basket_assignments = None
    turnover_tracker = {f'basket_{i+1}': [] for i in range(n_baskets)}
    
    dates = sorted(signal_df.index)
    costs = {}
    for i, date in enumerate(dates):
        # Get signals and returns for current date
        signals = signal_df.loc[date]
        returns = return_df.loc[date]
        
        # Remove NaN values
        valid_mask = signals.notna() & returns.notna()
        signals_valid = signals[valid_mask]
        returns_valid = returns[valid_mask]
        
        if len(signals_valid) < n_baskets:
            # Skip dates with insufficient data
            basket_results[date] = {f'basket_{j+1}': np.nan for j in range(n_baskets)}
            continue
        
        try:
            # Create baskets based on signal quantiles
            quantiles = pd.qcut(signals_valid, n_baskets, labels=False, duplicates='drop')
            current_assignments = quantiles.to_dict()  # {asset: basket}
            
            # Calculate gross returns for each basket
            gross_returns = {}
            cost_temp  = {}
            for basket in range(n_baskets):
                basket_mask = (quantiles == basket)
                if basket_mask.any():
                    gross_returns[f'basket_{basket+1}'] = returns_valid[basket_mask].mean()
                else:
                    gross_returns[f'basket_{basket+1}'] = np.nan
            
            # Apply transaction costs if not first period
            if prev_basket_assignments is not None:
                # Calculate turnover and costs for each basket
                for basket_num in range(1, n_baskets + 1):
                    basket_key = f'basket_{basket_num}'
                    current_assets = [asset for asset, b in current_assignments.items() if b == basket_num - 1]
                    prev_assets = [asset for asset, b in prev_basket_assignments.items() if b == basket_num - 1]
                    
                    # Calculate turnover: assets that entered or left the basket
                    assets_changed = set(current_assets) ^ set(prev_assets)
                    turnover_fraction = len(assets_changed) / max(len(current_assets), 1)
                    # print(assets_changed,max(len(current_assets), 1) )
                    # Apply transaction cost
                    if not np.isnan(gross_returns[basket_key]):
                        cost_impact = turnover_fraction * transaction_cost
                        
                        # net_returns[basket_key] = gross_returns[basket_key] - cost_impact
                        cost_temp[basket_key] = cost_impact
                    else:
                        cost_temp[basket_key] = np.nan
                    turnover_tracker[basket_key].append(turnover_fraction)
            
            basket_results[date] = gross_returns
            costs[date] = cost_temp
            prev_basket_assignments = current_assignments
            
        except ValueError:
            basket_results[date] = {f'basket_{j+1}': np.nan for j in range(n_baskets)}
    
    # Convert to DataFrame
    result_df = pd.DataFrame.from_dict(basket_results, orient='index')
    cost_df= pd.DataFrame.from_dict(costs, orient='index')
    final_df = result_df-cost_df
    # Add spread return (basket_n - basket_1)
    if f'basket_{n_baskets}' in result_df.columns and 'basket_1' in result_df.columns:
        final_df['spread'] = result_df[f'basket_{n_baskets}'] - result_df['basket_1']-cost_df[f'basket_{n_baskets}']-cost_df['basket_1']
    
    # Calculate average turnover for each basket
    avg_turnover = {basket: np.mean(turnover) for basket, turnover in turnover_tracker.items() 
                   if len(turnover) > 0}
    
    return final_df, avg_turnover, cost_df


def analyze_signal_stability(signal_series: pd.Series, 
                           returns_series: pd.Series,
                           cost_rate: float = 0.007) -> dict:
    """
    Analyze signal persistence and optimal holding periods.
    """
    from statsmodels.tsa.stattools import acf
    
    # Autocorrelation analysis
    autocorr = acf(signal_series, nlags=20)
    print(autocorr)
    signal_half_life = np.argmax(autocorr < 0.5)  # Half-life in periods
    
    # Calculate optimal lookahead where alpha > cost
    ic_series = signal_series.shift(1).corr(returns_series.rolling(5).mean())
    forward_returns = []
    
    for horizon in range(1, 21):
        forward_ret = returns_series.shift(-horizon).rolling(horizon).mean()
        ic = signal_series.corr(forward_ret)
        forward_returns.append((horizon, ic))
    
    # Find horizon where expected return > 2*cost
    optimal_horizon = next((h for h, ic in forward_returns if abs(ic) > 2*cost_rate), 1)
    
    return {
        'autocorrelation_half_life': signal_half_life,
        'optimal_holding_period': optimal_horizon,
        'suggested_rebalance_frequency': f"{optimal_horizon} periods"
    }



def plot_baskets(df, basket_col, value_col, num_baskets=10, method='quantile',commodity = ''):
    """
    Divides `basket_col` into baskets and plots the distribution of `value_col` for each basket.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        basket_col (str): The name of the column to use for creating baskets (e.g., a factor or signal).
        value_col (str): The name of the column to analyze within each basket (e.g., returns).
        num_baskets (int): The number of baskets to create. Default is 10 (deciles).
        method (str): Method to create baskets. 'quantile' for equal-sized baskets, 'value' for equal value ranges.
    """
    
    # 1. Create the baskets
    if method == 'quantile':
        # Create labels for quantiles (e.g., 'Q1', 'Q2')
        labels = [f'Q{i+1}' for i in range(num_baskets)]
        # Use qcut to split into equal-sized buckets
        df['Basket'], sample_results = pd.qcut(df[basket_col], q=num_baskets, labels=labels, duplicates='drop', retbins=True)
    elif method == 'value':
        # Create labels for value bins (e.g., 'Bin1', 'Bin2')
        labels = [f'Bin{i+1}' for i in range(num_baskets)]
        # Use cut to split the range of the value into equal parts
        df['Basket'], sample_results = pd.cut(df[basket_col], bins=num_baskets, labels=labels, duplicates='drop', retbins=True)
    else:
        raise ValueError("Method must be 'quantile' or 'value'")

    print("Bin edges:", sample_results)

    # 2. Group by the basket and calculate statistics for the value column
    basket_stats = df.groupby('Basket')[value_col].agg(['mean', 'var', 'count'])
    basket_stats.rename(columns={'mean': 'Mean', 'var': 'Variance'}, inplace=True)
    
    # Prepare the data for plotting: Get the list of values for each basket
    basket_data = [df[df['Basket'] == basket][value_col].values for basket in basket_stats.index]

    # 3. Create the plot
    plt.figure(figsize=(10, 6))
    
    # Create a boxplot to show the distribution, mean, and outliers
    boxplot = plt.boxplot(basket_data, labels=basket_stats.index, patch_artist=True)
    
    # Customize boxplot colors
    for patch in boxplot['boxes']:
        patch.set_facecolor('lightblue')
    
    # Overlay the mean as a red dot
    plt.scatter(x=range(1, len(basket_stats) + 1), y=basket_stats['Mean'], color='red', s=10, zorder=3, label='Mean')
    
    # Annotate the mean and variance on the plot
    for i, (idx, row) in enumerate(basket_stats.iterrows(), start=1):
        # Position the text slightly above the mean
        plt.text(i, row['Mean'] + 0.05 * (plt.ylim()[1] - plt.ylim()[0]), 
                 f'Mean: {row["Mean"]:.3f}\nVar: {row["Variance"]:.3f}\nn: {row["count"]}',
                 ha='center', va='bottom', fontsize=9, 
                 bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))

    # 4. Format the plot
    plt.title(f'{commodity} Distribution of {value_col} by {num_baskets} Baskets of {basket_col} ({method.capitalize()}-based)')
    plt.xlabel(f'Basket (Grouped by {basket_col})')
    plt.ylabel(f'{value_col}')
    plt.axhline(y=0, color='black', linestyle='-', alpha=0.3) # Add a line at y=0 for reference
    plt.grid(axis='y', alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # 5. Return the statistics DataFrame for further inspection
    return basket_stats