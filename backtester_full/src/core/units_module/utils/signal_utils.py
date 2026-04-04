import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from backtester.src.core.units_module.utils.kalman import KalmanTrendEstimator
def bollinger_bands(series, window=5, num_std=2):
    """
    Calculate Bollinger Bands for a given time series.
    
    Parameters:
        series (pd.Series): Time series data (e.g., closing prices).
        window (int): Rolling window size (default=20).
        num_std (int): Number of standard deviations for bands (default=2).
        plot (bool): If True, generates a plot (default=False).
    
    Returns:
        pd.DataFrame: Columns ['Middle', 'Upper', 'Lower', '%B', 'Bandwidth'].
    """
    # Calculate rolling mean and standard deviation
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    
    # Calculate bands
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)
    
    # Additional metrics
    percent_b = (series - lower_band) / (upper_band - lower_band)  # %B indicator
    bandwidth = (upper_band - lower_band) / rolling_mean  # Bandwidth
    
    # Create DataFrame
    bands = pd.DataFrame({
        'Middle': rolling_mean,
        'Upper': upper_band,
        'Lower': lower_band,
        '%B': percent_b,
        'Bandwidth': bandwidth
    })

    return bands

def trend_kalman(data_to_use, return_lookback):
    kf = KalmanTrendEstimator(
            process_noise=0.01,
            observation_noise=1.0,
            initial_level= data_to_use[0],
            initial_trend=0
        )
    estimates = []
        # predicts = []
    for measurement in data_to_use:
        estimates.append(kf.update(measurement))
        # predicts.append(kf.predict())

    trend_signal = (estimates[-1]['level'] - estimates[-return_lookback]['level'])/estimates[-return_lookback]['level']
    
    diff = [estimates[i]['level'] - data_to_use[i] for i in range(len(estimates))]
    # print(diff)
    noise_std = np.std(diff)/estimates[-1]['level']
    return trend_signal, noise_std


def RSI(data_to_use, lookback):
    delta = data_to_use.diff().dropna()
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    roll_up = up.rolling(window=lookback).mean()
    roll_down = down.abs().rolling(window=lookback).mean()
    rs = roll_up / roll_down
    rsi = 100 - 100 / (1 + rs)
    return rsi