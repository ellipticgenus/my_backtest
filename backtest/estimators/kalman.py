"""
Kalman Filter Trend Estimator.

Provides a Kalman filter implementation for trend estimation in price series.
"""

import numpy as np
from typing import Dict, List, Optional, Any


class KalmanTrendEstimator:
    """
    Kalman Filter for trend estimation.
    
    Uses a 2-state Kalman filter (level and trend) to estimate
    the underlying trend in a price series.
    
    Attributes:
        q: Process noise covariance matrix
        r: Observation noise variance
        F: State transition matrix
        H: Observation matrix
        state: Current state estimate [level, trend]
        P: State covariance matrix
        history: Dictionary containing historical estimates
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
            process_noise: Process noise standard deviation
            observation_noise: Observation noise standard deviation
            initial_level: Initial level estimate
            initial_trend: Initial trend estimate
            initial_uncertainty: Initial state uncertainty
        """
        self.q = np.array([[process_noise, 0], 
                          [0, process_noise]])
        self.r = observation_noise
        self.F = np.array([[1, 1], 
                          [0, 1]])
        self.H = np.array([[1, 0]])
        self.state = np.array([[initial_level], 
                             [initial_trend]])
        self.P = np.eye(2) * initial_uncertainty
        self.history: Dict[str, List] = {
            'levels': [],
            'trends': [],
            'timesteps': []
        }
    
    def update(self, measurement: float, timestep: Optional[int] = None) -> Dict[str, Any]:
        """
        Update the filter with a new measurement.
        
        Args:
            measurement: New observation value
            timestep: Optional timestep index
            
        Returns:
            Dictionary containing current estimate
        """
        # Predict
        self.state = self.F @ self.state
        self.P = self.F @ self.P @ self.F.T + self.q
        
        # Update
        y = measurement - self.H @ self.state
        S = self.H @ self.P @ self.H.T + self.r
        K = self.P @ self.H.T / S
        
        self.state = self.state + K * y
        self.P = (np.eye(2) - K @ self.H) @ self.P
        
        # Store history
        self.history['levels'].append(float(self.state[0][0]))
        self.history['trends'].append(float(self.state[1][0]))
        self.history['timesteps'].append(
            timestep if timestep is not None else len(self.history['timesteps'])
        )
        
        return self.get_estimate()
    
    def get_estimate(self) -> Dict[str, Any]:
        """
        Get the current state estimate.
        
        Returns:
            Dictionary with 'level', 'trend', and 'uncertainty'
        """
        return {
            'level': float(self.state[0][0]),
            'trend': float(self.state[1][0]),
            'uncertainty': self.P.tolist()
        }
    
    def predict(self, steps_ahead: int = 1) -> float:
        """
        Predict the level multiple steps ahead.
        
        Args:
            steps_ahead: Number of steps to predict
            
        Returns:
            Predicted level value
        """
        predicted_state = np.linalg.matrix_power(self.F, steps_ahead) @ self.state
        return float(predicted_state[0])
    
    def reset(
        self, 
        initial_level: float = 0, 
        initial_trend: float = 0,
        initial_uncertainty: float = 1.0
    ) -> None:
        """
        Reset the filter to initial state.
        
        Args:
            initial_level: Initial level estimate
            initial_trend: Initial trend estimate
            initial_uncertainty: Initial state uncertainty
        """
        self.state = np.array([[initial_level], [initial_trend]])
        self.P = np.eye(2) * initial_uncertainty
        self.history = {
            'levels': [],
            'trends': [],
            'timesteps': []
        }