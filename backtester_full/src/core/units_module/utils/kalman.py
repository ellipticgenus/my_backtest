"""
Kalman Filter for Trend Estimation.

Contains the KalmanTrendEstimator class for trend detection in price series.
"""

import numpy as np
from typing import Dict, List, Optional, Any


class KalmanTrendEstimator:
    """
    Kalman Filter for trend estimation in financial time series.
    
    Uses a 2-state model (level + trend) to estimate the underlying trend
    in noisy price data.
    """
    
    def __init__(
        self,
        process_noise: float = 0.1,
        observation_noise: float = 1.0,
        initial_level: float = 0,
        initial_trend: float = 0,
        initial_uncertainty: float = 1.0
    ):
        """
        Initialize the Kalman Trend Estimator.
        
        Args:
            process_noise: Process noise variance (default: 0.1)
            observation_noise: Observation noise variance (default: 1.0)
            initial_level: Initial level estimate (default: 0)
            initial_trend: Initial trend estimate (default: 0)
            initial_uncertainty: Initial state uncertainty (default: 1.0)
        """
        self.q = np.array([[process_noise, 0], [0, process_noise]])
        self.r = observation_noise
        self.F = np.array([[1, 1], [0, 1]])
        self.H = np.array([[1, 0]])
        self.state = np.array([[initial_level], [initial_trend]])
        self.P = np.eye(2) * initial_uncertainty
        self.history: Dict[str, List] = {'levels': [], 'trends': [], 'timesteps': []}
    
    def update(self, measurement: float, timestep: Optional[int] = None) -> Dict[str, Any]:
        """Update the filter with a new measurement."""
        self.state = self.F @ self.state
        self.P = self.F @ self.P @ self.F.T + self.q
        y = measurement - self.H @ self.state
        S = self.H @ self.P @ self.H.T + self.r
        K = self.P @ self.H.T / S
        self.state = self.state + K * y
        self.P = (np.eye(2) - K @ self.H) @ self.P
        self.history['levels'].append(float(self.state[0][0]))
        self.history['trends'].append(float(self.state[1][0]))
        self.history['timesteps'].append(timestep if timestep is not None else len(self.history['timesteps']))
        return self.get_estimate()
    
    def get_estimate(self) -> Dict[str, Any]:
        """Get the current state estimate."""
        return {
            'level': float(self.state[0][0]),
            'trend': float(self.state[1][0]),
            'uncertainty': self.P.tolist()
        }
    
    def predict(self, steps_ahead: int = 1) -> float:
        """Predict the level steps_ahead into the future."""
        predicted_state = np.linalg.matrix_power(self.F, steps_ahead) @ self.state
        return float(predicted_state[0])
    
    def reset(self, initial_level: float = 0, initial_trend: float = 0, initial_uncertainty: float = 1.0):
        """Reset the filter to initial state."""
        self.state = np.array([[initial_level], [initial_trend]])
        self.P = np.eye(2) * initial_uncertainty
        self.history = {'levels': [], 'trends': [], 'timesteps': []}
    
    @staticmethod
    def calculate_trend_signal(data: List[float], lookback: int, noise: float, process_noise_scale: float = 0.05):
        """Calculate trend signal from a series of data points."""
        if len(data) < lookback:
            return 0, 0
        
        kf = KalmanTrendEstimator(
            process_noise=process_noise_scale * noise,
            observation_noise=noise,
            initial_level=data[0],
            initial_trend=0
        )
        
        estimates = [kf.update(m) for m in data]
        levels = [e['level'] for e in estimates]
        diff = [levels[i] - data[i] for i in range(len(estimates))]
        noise_std = np.std(diff) / levels[-1] if levels[-1] != 0 else 0
        trend_signal = (levels[-1] - levels[-lookback]) / levels[-lookback] if len(levels) > lookback else 0
        
        return trend_signal, noise_std