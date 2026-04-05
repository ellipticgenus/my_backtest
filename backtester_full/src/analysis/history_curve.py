import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
from matplotlib import dates as mdates
from matplotlib.colors import LinearSegmentedColormap
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from my_holiday.holiday_utils import *
from typing import Dict, List
PREROLL = {
    'Q':[0,1],
    'Y':[0]
}
CONTRACT_TYPE ={
    "Q": "quarterly",
    'Y': "yearly",
}


def monthly_pnl_attribution(price_series, title="Monthly Price Differences"):
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
    
    # return monthly_mean, yearly_mean, pivot

def calculate_sharpe_ratios(returns_series, risk_free_rate=0.0, freq='D'):
    """
    Calculates the 1-year, 3-year, and 5-year annualized Sharpe ratios from a pandas Series of returns.

    Parameters:
    returns_series (pd.Series): A pandas Series with a DatetimeIndex and the return values.
    risk_free_rate (float): The annualized risk-free rate (e.g., 0.02 for 2%). Default is 0.
    freq (str): The frequency of the data. Used for annualization.
                Common options: 'D' (daily), 'M' (monthly), 'Q' (quarterly).

    Returns:
    dict: A dictionary containing the Sharpe ratios for the available periods.
    """

    # Validate input
    if not isinstance(returns_series, pd.Series):
        raise TypeError("Input must be a pandas Series.")
    if returns_series.empty:
        raise ValueError("Return series is empty.")

    # Make sure the index is datetime and sort it
    returns_series = returns_series.copy()
    returns_series.index = pd.to_datetime(returns_series.index)
    returns_series = returns_series.sort_index()

    # Define the periods and their required minimum days (approx)
    periods = {
        '1_year': 252,   # Common trading days in a year
        '3_year': 252 * 3,
        '5_year': 252 * 5,
        'Lifetime': len(returns_series)
    }
    
    # Adjust min data points based on frequency
    freq_map = {'D': 1, 'M': 21, 'Q': 63, 'A': 252} # Approx conversions
    min_data_points = freq_map.get(freq, 1)
    
    # Adjust periods for frequency
    for key in periods:
        periods[key] = periods[key] // min_data_points

    # Get the end date of the series (most recent date)
    end_date = returns_series.index[-1]
    
    sharpe_results = {}
    
    # Calculate Sharpe for each period
    for period_name, min_obs in periods.items():
        # Calculate the start date for the period
        if '1_year' in period_name:
            start_date = end_date - pd.DateOffset(years=1)
        elif '3_year' in period_name:
            start_date = end_date - pd.DateOffset(years=3)
        elif '5_year' in period_name:
            start_date = end_date - pd.DateOffset(years=5)
        elif 'Lifetime' in period_name:
            start_date = returns_series.index[0]
        else:
            continue
        
        # Slice the returns for the period
        period_returns = returns_series.loc[start_date:end_date]
        
        # Check if we have enough data
        if len(period_returns) < min_obs:
            sharpe_results[period_name] = np.nan
            print(f"Warning: Not enough data to calculate {period_name} Sharpe. Required: ~{min_obs} observations, Found: {len(period_returns)}.")
            continue
        
        # Calculate excess returns
        # Adjust risk-free rate to the period frequency
        if freq == 'D':
            period_rf = risk_free_rate / 252
        elif freq == 'M':
            period_rf = risk_free_rate / 12
        elif freq == 'Q':
            period_rf = risk_free_rate / 4
        else:
            period_rf = risk_free_rate / 252 # Default to daily
        
        excess_returns = period_returns - period_rf
        
        # Calculate annualization factor
        if freq == 'D':
            ann_factor = np.sqrt(252)
        elif freq == 'M':
            ann_factor = np.sqrt(12)
        elif freq == 'Q':
            ann_factor = np.sqrt(4)
        else:
            ann_factor = np.sqrt(252) # Default to daily
        
        # Calculate Sharpe Ratio
        mean_excess_return = np.mean(excess_returns)
        std_excess_return = np.std(excess_returns, ddof=1) # Sample standard deviation
        
        if std_excess_return == 0:
            sharpe_ratio = np.nan # Avoid division by zero
        else:
            sharpe_ratio = (mean_excess_return / std_excess_return) * ann_factor
        
        sharpe_results[period_name] = sharpe_ratio
    
    return sharpe_results



def historical_stats(price_series, indices,max_roll_date=10):
    var_series = calculate_var(indices, max_roll_date)
    # print(var_series)
    res = {}
    res['var'] = var_series['var'].min()
    res['var95'] = var_series['var'].quantile(0.05)
    res['var99'] = var_series['var'].quantile(0.01)
    res['notional'] = max(var_series['long'].max(), var_series['short'].max())
    # print(res)
    year_end = price_series.resample('YE').last()
    yoy_pnl = year_end.diff()
    yoy_pnl.iloc[0] = year_end.iloc[0]
    res['max_pnl_risk'] = (yoy_pnl/abs(res['var99'])).max()
    res['min_pnl_risk'] = (yoy_pnl/abs(res['var99'])).min()
    res['mean_pnl_risk'] = (yoy_pnl/abs(res['var99'])).mean()
    # print(res)
    price_series += res['notional']
    return_df = price_series.pct_change().dropna()
    res = res|(calculate_sharpe_ratios(return_df, risk_free_rate=0.0, freq='D'))
    # print(res)
    previous_peak = price_series.cummax()

    drawdown = (price_series - previous_peak)
    abs_drawdown = (drawdown.abs()-drawdown)/2
    res['max_drawdown'] = abs_drawdown.max()
    res['mean_drawdown'] = abs_drawdown.mean()
    # print(res)
    return res

def plot_var(price_series, indices, max_roll_date=10):
    var_series = calculate_var(indices, max_roll_date)
    fig = plt.figure(figsize=(14, 12))
    gs = fig.add_gridspec(6, 1, height_ratios=[2,1,1,1,1,1])
    
    # Create axes
    ax1 = fig.add_subplot(gs[0])  # Price series
    ax2 = fig.add_subplot(gs[1])  # Percentage 
    ax3 = fig.add_subplot(gs[2])  # Percentage 
    ax4 = fig.add_subplot(gs[3])  # Percentage 
    ax5 = fig.add_subplot(gs[4])  # Percentage 
    ax6 = fig.add_subplot(gs[5])  # Percentage 
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
    for ax in [ax1, ax2,ax3,ax4,ax5,ax6]:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


def calculate_var(indices, max_roll_date=10):
    holding_curve = {}
    tickers = set()  # Using set to automatically handle duplicates
    
    # Step 1: Aggregate positions across all indices
    for index, size in indices.items():
        try:
            with open(f'../backtester/strategy/backtest/{index}.pkl', 'rb') as f:
                history = pickle.load(f)
            for date, data in history.items():
                positions = data.get('positions', {})  # Safe get with default empty list
                risks = data.get('risk_curve', {})
                
                if date not in holding_curve:
                    holding_curve[date] = {}
                
                # Handle both (ticker,qty) pairs and more complex position formats
                for ticker, qty in positions.items():
                    if ticker == 'USD':
                        continue
                    ticker_symbol = ticker[:-3]  # Remove contract suffix
                    price = risks.get(ticker, {}).get('close', 0)
                    holding_curve[date][ticker] = holding_curve[date].get(ticker, 0) + qty * price * size
                    tickers.add(ticker_symbol)
                # print(holding_curve)
        except FileNotFoundError:
            print(f"Warning: File for index {index} not found")
            continue
        except Exception as e:
            print(f"Error processing index {index}: {str(e)}")
            continue
    
    # Convert to list after collecting all unique tickers
    tickers = list(tickers)
    
    # Step 2: Load nearby series (assuming this function exists)
    nearby_series = load_nearby_series(tickers, max_roll_date)

    # Step 3: Calculate VaR for each date

    var_results = []
    for date in sorted(holding_curve.keys()):  # Process dates in order
        position = holding_curve[date]
        if not position:
            continue
            
        # Get nearby contracts and position values
        nearby_contracts = []
        position_values = []
        var_position_values = []
        for ticker, value in position.items():
            ticker_symbol = ticker[:-3]
            contract_month = ticker[-3:]
            try:
                contract_type=CONTRACT_TYPE.get(ticker_symbol[-1], 'monthly')
                nearby = contract_to_nearby(
                    date, 
                    contract_month, 
                    max_roll_date,
                    contract_type=contract_type
                )
                nearby = max(nearby,0)
                nearby_contracts.append(f'{ticker_symbol}_{nearby}_{max_roll_date}')
                factor = 1 
                if contract_type == 'monthly' and nearby == 0:
                    factor = calculate_leftdays(date)
                position_values.append(value)
                var_position_values.append(value*factor)
            except Exception as e:
                print(f"Error getting nearby contract for {ticker} on {date}: {str(e)}")
                continue
        # Calculate VaR if we have data
        if nearby_contracts:
            try:
                returns = nearby_series[nearby_contracts].loc[:date].iloc[-252:]
                if len(returns) > 10:  # Minimum data requirement
                    abs_total =np.sum(np.abs(position_values))
                    total = np.sum(position_values)
                    long =np.sum([x for x in position_values if x>0])
                    short=np.sum([x for x in position_values if x<0])
                    portfolio_returns = returns @ var_position_values  # Matrix multiplication
                    var = np.percentile(portfolio_returns, 5)
                    var_results.append({'date': date, 'var': var,'abs_total':abs_total,'total':total,'long':long, 'short':short})
            except Exception as e:
                print(f"Error calculating VaR for {date}: {str(e)}")
    res =  pd.DataFrame(var_results).sort_values('date')
    res.set_index('date',inplace=True)
    return res
    

def calculate_leftdays(_date):
    if _date:
        _date = pd.to_datetime(_date)
    else:
        _date = pd.Timestamp.now()
    
    first_day = _date.replace(day=1)
    last_day = (first_day + relativedelta(months=1)) - timedelta(days=1)
    all_days = pd.date_range(start=first_day, end=last_day, freq='D')    
    remaining_days = pd.date_range(start=_date, end=last_day, freq='D')

    return len(remaining_days[remaining_days.dayofweek < 5])/len(all_days[all_days.dayofweek < 5])

def load_nearby_series(tickers, max_roll_date = 10):
    ts_list = []
    for ticker in tickers:
        if ticker[-1] in ['Y','Q']:
            temp = ticker[:-1]
        else: 
            temp = ticker
        roll_nearbys = PREROLL.get(ticker[-1],[0,1,2])
        for roll_nearby in roll_nearbys:
            symbol = f'{ticker}_{roll_nearby}_{max_roll_date}'
            df = pd.read_csv(f'../data/series/{temp}/{symbol}.csv')
            df['date'] = pd.to_datetime(df['date'])
            df = df.reset_index(drop=True).set_index('date')
            df.rename(columns={'return':symbol},inplace=True)
            ts_list.append(df[symbol])
    merged_df = pd.concat(ts_list,axis=1)
    return merged_df


def calculate_drawdown(price_series,plot=True):
    
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
                    where=pct_drawdown<0, color='red', alpha=0.2)
    ax2.plot(pct_drawdown.index, pct_drawdown*100, color='darkred', label='Drawdown (%)', linewidth=1)
    ax2.plot(max_pct_drawdown.index, max_pct_drawdown*100, color='black', linestyle='--', label='Max Drawdown')
    
    ax2.set_ylabel('Drawdown (%)')
    ax2.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax2.legend(loc='lower left')
    ax2.grid(True, linestyle=':', alpha=0.7)
    
    # Plot absolute drawdown
    ax3.fill_between(abs_drawdown.index, abs_drawdown, 0, 
                    where=abs_drawdown<0, color='blue', alpha=0.2)
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
    
def calculate_drawdown_stats(drawdown_series, price_series, is_pct=True):
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

def cal_sharpe_vol_ret(df, initial_notional, final_date):
    final_date = pd.to_datetime(final_date)
    _df = df.copy()
    _df += initial_notional
    if not isinstance(_df.index, pd.DatetimeIndex):
        _df.index = pd.to_datetime(_df.index)
    historical_data = _df[_df.index <= final_date]
    live_data = _df[_df.index > final_date]
    
    results = {}
    
    for column in _df.columns:
        # Get column data
        col_data = _df[column].dropna()
        hist_data = historical_data[column].dropna()
        live_col_data = live_data[column].dropna()
        
        if len(hist_data) < 2:  # Need at least 2 data points for calculations
            results[column] = {
                '1Y_Return': np.nan,
                '1Y_Vol': np.nan,
                '1Y_Sharpe': np.nan,
                '3Y_Return': np.nan,
                '3Y_Vol': np.nan,
                '3Y_Sharpe': np.nan,
                'Lifetime_Return': np.nan,
                'Lifetime_Vol': np.nan,
                'Lifetime_Sharpe': np.nan,
                'Live_Return': np.nan,
                'Live_Vol': np.nan,
                'Live_Sharpe': np.nan
            }
            continue
        
        # Calculate daily returns
        daily_returns = col_data.pct_change().dropna()
        hist_daily_returns = hist_data.pct_change().dropna()
        live_daily_returns = live_col_data.pct_change().dropna()
        
        # Helper function to calculate metrics for a given return series
        def calculate_metrics(returns, risk_free_rate=0.0):
            if len(returns) == 0:
                return np.nan, np.nan, np.nan
            
            # Annualized return
            annualized_return = (1 + returns.mean()) ** 252 - 1
            
            # Annualized volatility
            annualized_vol = returns.std() * np.sqrt(252)
            
            # Sharpe ratio (annualized)
            if annualized_vol > 0:
                sharpe_ratio = (annualized_return - risk_free_rate) / annualized_vol
            else:
                sharpe_ratio = np.nan
            
            return annualized_return, annualized_vol, sharpe_ratio
        
        # 1-year metrics (from final_date backwards)
        one_year_ago = final_date - pd.DateOffset(years=1)
        returns_1y = hist_daily_returns[hist_daily_returns.index >= one_year_ago]
        ret_1y, vol_1y, sharpe_1y = calculate_metrics(returns_1y)
        
        # 3-year metrics (from final_date backwards)
        three_years_ago = final_date - pd.DateOffset(years=3)
        returns_3y = hist_daily_returns[hist_daily_returns.index >= three_years_ago]
        ret_3y, vol_3y, sharpe_3y = calculate_metrics(returns_3y)
        
        # Lifetime metrics (all historical data)
        ret_lt, vol_lt, sharpe_lt = calculate_metrics(hist_daily_returns)
        
        # Live metrics (data after final_date)
        ret_live, vol_live, sharpe_live = calculate_metrics(live_daily_returns)
        
        results[column] = {
            '1Y_Return': ret_1y,
            '1Y_Vol': vol_1y,
            '1Y_Sharpe': sharpe_1y,
            '3Y_Return': ret_3y,
            '3Y_Vol': vol_3y,
            '3Y_Sharpe': sharpe_3y,
            'Lifetime_Return': ret_lt,
            'Lifetime_Vol': vol_lt,
            'Lifetime_Sharpe': sharpe_lt,
            'Live_Return': ret_live,
            'Live_Vol': vol_live,
            'Live_Sharpe': sharpe_live,
            # 'Initial_Notional': initial_notional,
            # 'Final_Date': final_date
        }
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results).T
    naked_df = results_df.copy()
    # Format percentages for better readability
    percent_columns = [col for col in results_df.columns if 'Return' in col or 'Vol' in col]
    for col in percent_columns:
        results_df[col] = results_df[col].apply(lambda x: f"{x:.2%}" if not pd.isna(x) else "N/A")
    
    return naked_df, results_df