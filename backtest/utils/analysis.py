"""
Analysis utilities for the backtest package.

Contains data loading, drawdown calculations, VaR calculations, and plotting functions.
"""

import os
import pickle
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import dates as mdates
from matplotlib.colors import LinearSegmentedColormap
from typing import Optional, Dict, Any, List

from backtest.utils.constants import CONTRACT_TYPE, CONTRACT_FACTOR, PREROLL, MONTH_TO_NUM
from backtest.utils.calendar import (
    load_business_days, contract_to_nearby, calculate_leftdays, load_nearby_series
)


def load_future_data(data_path: str = './data/S10TC', values: str = 'close') -> pd.DataFrame:
    """
    Load futures data from CSV files in a given directory and return a pivot table.
    
    Parameters
    ----------
    data_path : str, optional
        The path to the directory containing the CSV files.
    values : str, optional
        Which values to pivot. Default is 'close'.
        
    Returns
    -------
    pd.DataFrame
        Pivot table with date as index and contract as columns.
    """
    files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    dfs = (pd.read_csv(os.path.join(data_path, file)).assign(
        contract=file.replace('.csv', ''),
        date=lambda df: pd.to_datetime(df['date'])
    ) for file in files)

    big_df = pd.concat(dfs, ignore_index=True)
    pivot_df = big_df.pivot(index='date', columns='contract', values=values)
    return pivot_df


def load_and_process(index: str, path: Optional[str] = None) -> pd.DataFrame:
    """
    Load and process backtest results from pickle file.
    
    Args:
        index: Strategy index name
        path: Optional path to pickle file
        
    Returns:
        DataFrame containing processed backtest results
    """
    if path:
        with open(path, 'rb') as f:
            history = pickle.load(f)
    else:
        with open(f'C:/Users/yuhang.hou/projects/pipeline/data_pipeline/universal/strategy/backtest/{index}.pkl', 'rb') as f:
            history = pickle.load(f)

    curves = []
    for date, data in history.items():
        temp = {}
        temp['date'] = date
        temp['tc'] = data['tc']
        temp['level'] = data['level']
        curves.append(temp)
    final = pd.DataFrame(curves)
    final['tc_cumsum'] = final['tc'].cumsum()
    final.set_index('date', inplace=True)
    return final


def load_and_run_bt(index: str, strategy_type: str):
    """
    Load and run a backtest for a given index and strategy type.
    
    Args:
        index: Strategy index name
        strategy_type: Strategy type ('KF' for Kalman Filter, 'LS' for Long-Short)
    """
    from backtest.backtester import TrendBacktester, VolRollingBacktest
    
    if 'c5tc' in index:
        contract_df = load_future_data(f'C:/Users/yuhang.hou/projects/pipeline/data/C5TC', ['close'])
    elif 'p4tc' in index:
        contract_df = load_future_data(f'C:/Users/yuhang.hou/projects/pipeline/data/P4TC', ['close'])
    elif 's10tc' in index:
        contract_df = load_future_data(f'C:/Users/yuhang.hou/projects/pipeline/data/S10TC', ['close'])
    
    contract_df = contract_df.ffill()
    
    with open('last_trading_day.json', 'r') as f:
        last_trading_days = json.load(f)
        ltds = {k: pd.to_datetime(v) for k, v in last_trading_days.items()}
    
    with open('business_days.json', 'r') as f:
        business_days = json.load(f)
        business_days = [pd.to_datetime(i) for i in business_days]
        business_days = list(set(business_days))
        business_days = sorted(business_days)

    with open(f'C:/Users/yuhang.hou/projects/pipeline/data_pipeline/freight_price/strategy/config/{index}.json', 'r') as fi:
        config = json.load(fi)

    if strategy_type == 'KF':
        backtest = TrendBacktester(
            data=contract_df,
            config=config,
            trading_days=business_days,
            last_trading_day=ltds
        )
    elif strategy_type == 'LS':
        backtest = VolRollingBacktest(
            data=contract_df,
            config=config,
            trading_days=business_days,
            last_trading_day=ltds
        )
    else:
        raise NotImplementedError
    
    results = backtest.run_backtest()
    
    with open(f'C:/Users/yuhang.hou/projects/pipeline/data_pipeline/freight_price/strategy/backtest/{index}.pkl', 'wb') as f:
        pickle.dump(results, f)


def monthly_pnl_attribution(price_series, title: str = "Monthly Price Differences"):
    """
    Plot heatmap with monthly means (bottom) and yearly means (right).
    
    Parameters:
        price_series (pd.Series): Price time series with datetime index
        title (str): Plot title
        
    Returns:
        tuple: (monthly_mean, yearly_mean, pivot_table)
    """
    # Calculate differences
    diffs = price_series.diff().dropna()
    
    # Prepare DataFrame
    df = pd.DataFrame({
        'Diff': diffs,
        'Year': diffs.index.year,
        'Month': diffs.index.month_name(),
        'MonthNum': diffs.index.month
    })
    
    # Create pivot table
    month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    pivot = df.pivot_table(index='Year', columns='Month', values='Diff', aggfunc='mean')[month_order]
    
    # Calculate means
    monthly_mean = pivot.mean()
    yearly_mean = pivot.mean(axis=1).sort_index(ascending=False)
    
    # Create figure with gridspec
    fig = plt.figure(figsize=(14, 8))
    gs = fig.add_gridspec(2, 2, width_ratios=[10, 1], height_ratios=[10, 1],
                         hspace=0.3, wspace=0.1)
    
    # Heatmap (main)
    ax1 = fig.add_subplot(gs[0, 0])
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="RdYlGn", center=0,
                linewidths=0.5, cbar=False, ax=ax1)
    ax1.set_title(title, pad=20)
    ax1.set_xlabel('')
    ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
    
    # Yearly means (right)
    ax2 = fig.add_subplot(gs[0, 1])
    yearly_mean.plot(kind='barh', ax=ax2, color='firebrick')
    ax2.set_title('Avg by Year', pad=10)
    ax2.yaxis.tick_right()
    ax2.grid(axis='x', alpha=0.3)
    
    # Monthly means (bottom)
    ax3 = fig.add_subplot(gs[1, 0])
    monthly_mean.plot(kind='bar', ax=ax3, color='steelblue')
    ax3.set_title('Avg by Month', pad=10)
    ax3.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.show()
    
    return monthly_mean, yearly_mean, pivot


def calculate_drawdown(price_series, plot: bool = True) -> tuple:
    """
    Calculate drawdown statistics and plot both percentage and absolute drawdown information.

    Parameters:
        price_series (pd.Series): Price time series with datetime index
        plot (bool): Whether to plot the drawdown information

    Returns:
        tuple: (pct_table, abs_table)
    """
    if not isinstance(price_series, pd.Series):
        price_series = pd.Series(price_series)
    
    # 1. Calculate peaks
    previous_peaks = price_series.cummax()
    
    # 2. Calculate both types of drawdown series
    pct_drawdown = (price_series / previous_peaks) - 1  # Percentage drawdown
    abs_drawdown = price_series - previous_peaks       # Absolute drawdown
    
    # 3. Calculate max drawdown series (running max drawdown)
    max_pct_drawdown = pct_drawdown.cummin()
    max_abs_drawdown = abs_drawdown.cummin()
    
    # 4. Calculate drawdown statistics tables
    pct_table = calculate_drawdown_stats(pct_drawdown, price_series, is_pct=True)
    abs_table = calculate_drawdown_stats(abs_drawdown, price_series, is_pct=False)
    
    # 5. Plotting
    if plot:
        plot_drawdowns(price_series, pct_drawdown, abs_drawdown, 
                      max_pct_drawdown, max_abs_drawdown, pct_table)
    
    return pct_table, abs_table


def plot_drawdowns(price_series, pct_drawdown, abs_drawdown, 
                  max_pct_drawdown, max_abs_drawdown, pct_table):
    """
    Plot price series with both percentage and absolute drawdown information.
    """
    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(3, 1, height_ratios=[2, 1, 1])
    
    # Create axes
    ax1 = fig.add_subplot(gs[0])  # Price series
    ax2 = fig.add_subplot(gs[1])  # Percentage drawdown
    ax3 = fig.add_subplot(gs[2])  # Absolute drawdown
    
    # Custom colormap for drawdown periods
    cmap = LinearSegmentedColormap.from_list('dd_cmap', ['lightcoral', 'darkred'])
    
    # Plot price series
    ax1.plot(price_series.index, price_series, label='Price', color='black', linewidth=1.5)
    
    # Highlight max drawdown period if any
    if not pct_table.empty:
        max_dd = pct_table.iloc[0]  # Most severe drawdown (already sorted)
        max_dd_mask = (price_series.index >= max_dd['Start']) & (price_series.index <= max_dd['End'])
        ax1.plot(price_series.index[max_dd_mask], price_series[max_dd_mask], 
                color='red', linewidth=2.5, label='Max Drawdown Period')
        
        # Add annotation for max drawdown
        ax1.annotate(f"Max DD: {-max_dd['Depth']:.1f}%",
                    xy=(max_dd['Trough'], price_series[max_dd['Trough']]),
                    xytext=(10, 10), textcoords='offset points',
                    bbox=dict(boxstyle='round,pad=0.5', fc='red', alpha=0.3),
                    arrowprops=dict(arrowstyle='->'))
    
    ax1.set_ylabel('Price')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle=':', alpha=0.7)
    
    # Plot percentage drawdown
    ax2.fill_between(pct_drawdown.index, pct_drawdown*100, 0, 
                    where=pct_drawdown < 0, color='red', alpha=0.2)
    ax2.plot(pct_drawdown.index, pct_drawdown*100, color='darkred', label='Drawdown (%)', linewidth=1)
    ax2.plot(max_pct_drawdown.index, max_pct_drawdown*100, color='black', linestyle='--', label='Max Drawdown')
    
    ax2.set_ylabel('Drawdown (%)')
    ax2.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax2.legend(loc='lower left')
    ax2.grid(True, linestyle=':', alpha=0.7)
    
    # Plot absolute drawdown
    ax3.fill_between(abs_drawdown.index, abs_drawdown, 0, 
                    where=abs_drawdown < 0, color='blue', alpha=0.2)
    ax3.plot(abs_drawdown.index, abs_drawdown, color='darkblue', label='Drawdown (Abs)', linewidth=1)
    ax3.plot(max_abs_drawdown.index, max_abs_drawdown, color='black', linestyle='--', label='Max Drawdown')
    
    ax3.set_ylabel('Drawdown (Absolute)')
    ax3.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax3.legend(loc='lower left')
    ax3.grid(True, linestyle=':', alpha=0.7)
    
    # Format x-axis for dates
    for ax in [ax1, ax2, ax3]:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


def calculate_drawdown_stats(drawdown_series, price_series, is_pct: bool = True) -> pd.DataFrame:
    """
    Calculate drawdown statistics including duration and recovery.
    """
    dates = price_series.index
    in_drawdown = drawdown_series < 0
    drawdown_start = (in_drawdown & ~in_drawdown.shift(1).fillna(False)).infer_objects(copy=False)
    drawdown_end = (in_drawdown & ~in_drawdown.shift(-1).fillna(False)).infer_objects(copy=False)
    
    start_dates = dates[drawdown_start]
    end_dates = dates[drawdown_end]
    
    stats = []
    for start in start_dates:
        # Get the drawdown period
        drawdown_period = (dates >= start) & (dates <= end_dates[end_dates >= start].min())
        current_drawdown = drawdown_series[drawdown_period]
        
        # Find trough (max drawdown point)
        trough_date = current_drawdown.idxmin()
        trough_value = current_drawdown.min()
        trough_price = price_series[trough_date]
        peak_price = price_series[price_series.index <= start].max()
        
        # Find recovery date (when back to previous peak)
        recovery_mask = (dates > trough_date) & (price_series >= peak_price)
        recovery_date = dates[recovery_mask].min() if recovery_mask.any() else pd.NaT
        
        # Calculate durations
        time_to_trough = (trough_date - start).days
        recovery_time = (recovery_date - trough_date).days if pd.notna(recovery_date) else np.nan
        
        stats.append({
            'Start': start,
            'Trough': trough_date,
            'End': recovery_date if pd.notna(recovery_date) else end_dates[end_dates >= start].min(),
            'Peak Value': peak_price,
            'Trough Value': trough_price,
            'Depth': trough_value * 100 if is_pct else trough_value,
            'Time to Trough (days)': time_to_trough,
            'Recovery Time (days)': recovery_time,
            'Total Duration (days)': (recovery_date - start).days if pd.notna(recovery_date) else np.nan
        })
    
    # Sort by depth (most severe first)
    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        stats_df = stats_df.sort_values('Depth', ascending=True if is_pct else False)
    
    return stats_df


def plot_var(price_series, indices: Dict, max_roll_date: int = 10):
    """
    Plot VaR analysis for a portfolio of strategies.
    
    Args:
        price_series: Price time series
        indices: Dictionary mapping strategy index to position size
        max_roll_date: Maximum roll date for nearby calculation
    """
    var_series = calculate_var(indices, max_roll_date)
    fig = plt.figure(figsize=(14, 12))
    gs = fig.add_gridspec(6, 1, height_ratios=[2, 1, 1, 1, 1, 1])
    
    # Create axes
    ax1 = fig.add_subplot(gs[0])  # Price series
    ax2 = fig.add_subplot(gs[1])  # VaR
    ax3 = fig.add_subplot(gs[2])  # Abs Total
    ax4 = fig.add_subplot(gs[3])  # Total
    ax5 = fig.add_subplot(gs[4])  # Long
    ax6 = fig.add_subplot(gs[5])  # Short
    
    # Custom colormap for drawdown periods
    cmap = LinearSegmentedColormap.from_list('dd_cmap', ['lightcoral', 'darkred'])
    
    # Plot price series
    ax1.plot(price_series.index, price_series, label='Price', color='black', linewidth=1.5)
    
    ax1.set_ylabel('Price')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle=':', alpha=0.7)
    
    # Plot var
    ax2.plot(var_series.index, var_series['var'], color='darkred', label='var', linewidth=1)
    ax2.set_ylabel('Var')
    ax2.legend(loc='lower left')
    ax2.grid(True, linestyle=':', alpha=0.7)
    
    ax3.plot(var_series.index, var_series['abs_total'], color='darkred', label='abs_total', linewidth=1)
    ax3.set_ylabel('Abs Total Notional')
    ax3.legend(loc='lower left')
    ax3.grid(True, linestyle=':', alpha=0.7)

    ax4.plot(var_series.index, var_series['total'], color='darkred', label='total', linewidth=1)
    ax4.set_ylabel('Total Notional')
    ax4.legend(loc='lower left')
    ax4.grid(True, linestyle=':', alpha=0.7)

    ax5.plot(var_series.index, var_series['long'], color='darkred', label='long', linewidth=1)
    ax5.set_ylabel('Long Notional')
    ax5.legend(loc='lower left')
    ax5.grid(True, linestyle=':', alpha=0.7)

    ax6.plot(var_series.index, var_series['short'], color='darkred', label='short', linewidth=1)
    ax6.set_ylabel('Short Notional')
    ax6.legend(loc='lower left')
    ax6.grid(True, linestyle=':', alpha=0.7)
    
    # Format x-axis for dates
    for ax in [ax1, ax2, ax3, ax4, ax5, ax6]:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


def calculate_var(indices: Dict, max_roll_date: int = 10) -> pd.DataFrame:
    """
    Calculate VaR for a portfolio of strategies.
    
    Args:
        indices: Dictionary mapping strategy index to position size
        max_roll_date: Maximum roll date for nearby calculation
        
    Returns:
        DataFrame with VaR results
    """
    holding_curve = {}
    tickers = set()
    
    # Step 1: Aggregate positions across all indices
    for index, size in indices.items():
        if 'c5tc' in index:
            contract_df = load_future_data(f'C:/Users/yuhang.hou/projects/pipeline/data/C5TC', ['close'])
        elif 'p4tc' in index:
            contract_df = load_future_data(f'C:/Users/yuhang.hou/projects/pipeline/data/P4TC', ['close'])
        elif 's10tc' in index:
            contract_df = load_future_data(f'C:/Users/yuhang.hou/projects/pipeline/data/S10TC', ['close'])
        try:
            with open(f'C:/Users/yuhang.hou/projects/pipeline/data_pipeline/freight_price/strategy/backtest/{index}.pkl', 'rb') as f:
                history = pickle.load(f)
            for date, data in history.items():
                positions = data.get('positions', {})
                
                if date not in holding_curve:
                    holding_curve[date] = {}
                
                for ticker, qty in positions.items():
                    if ticker == 'USD':
                        continue

                    base_ticker = (index.split('_')[0]).upper()
                    if index.split('_')[1] == 'q':
                        ticker_symbol = base_ticker + index.split('_')[1]
                    else:
                        ticker_symbol = base_ticker
                    ticker_symbol = ticker_symbol.upper()
                    price = contract_df.loc[date, ('close', ticker)]
                    holding_curve[date][base_ticker + ticker] = holding_curve[date].get(base_ticker + ticker, 0) + qty * price * size
                    tickers.add(ticker_symbol)
        except FileNotFoundError:
            print(f"Warning: File for index {index} not found")
            continue
        except Exception as e:
            print(f"Error processing index {index}: {str(e)}")
            continue

    tickers = list(tickers)
    
    # Step 2: Load nearby series
    nearby_series = load_nearby_series(tickers, max_roll_date)

    # Step 3: Calculate VaR for each date
    with open('C:/users/yuhang.hou/projects/pipeline/data_pipeline/freight_price/last_trading_day.json', 'r') as f:
        last_trading_days = json.load(f)
    business_days = load_business_days()
    var_results = []
    for date in sorted(holding_curve.keys()):
        position = holding_curve[date]
        if not position:
            continue
            
        nearby_contracts = []
        position_values = []
        var_position_values = []
        for ticker, value in position.items():
            ticker_symbol = ticker[:-3]
            contract_month = ticker[-3:]
            try:
                contract_type = CONTRACT_TYPE.get(ticker_symbol[-1], 'monthly')
                nearby = contract_to_nearby(
                    date,
                    contract_month,
                    max_roll_date,
                    contract_type=contract_type,
                    trading_days=last_trading_days,
                    buz_days=business_days
                )
                nearby = max(nearby, 0)
                nearby_contracts.append(f'{ticker_symbol}_{nearby}_{max_roll_date}')
                factor = 1
                if contract_type == 'monthly' and nearby == 0:
                    factor = calculate_leftdays(date)
                position_values.append(value)
                var_position_values.append(value * factor)
            except Exception as e:
                print(f"Error getting nearby contract for {ticker} on {date}: {str(e)}")
                continue
        # Calculate VaR if we have data
        if nearby_contracts:
            try:
                returns = nearby_series[nearby_contracts].loc[:date].iloc[-252:]
                if len(returns) > 10:
                    abs_total = np.sum(np.abs(position_values))
                    total = np.sum(position_values)
                    long = np.sum([x for x in position_values if x > 0])
                    short = np.sum([x for x in position_values if x < 0])
                    portfolio_returns = returns @ var_position_values
                    var = np.percentile(portfolio_returns, 5)
                    var_results.append({
                        'date': date,
                        'var': var,
                        'abs_total': abs_total,
                        'total': total,
                        'long': long,
                        'short': short
                    })
            except Exception as e:
                print(f"Error calculating VaR for {date}: {str(e)}")
    res = pd.DataFrame(var_results).sort_values('date')
    res.set_index('date', inplace=True)
    return res