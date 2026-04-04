import numpy as np

class KalmanTrendEstimator:
    """
    A trend estimator using Kalman Filter with a local linear trend model.
    
    The state vector is [level, trend] where:
    - level is the current value
    - trend is the current slope
    
    Parameters:
    - process_noise: Controls how much the trend can change
    - observation_noise: Measurement noise variance
    - initial_level: Initial value estimate
    - initial_trend: Initial slope estimate
    - initial_uncertainty: Initial uncertainty of estimates
    """
    
    def __init__(self, process_noise=0.1, observation_noise=1.0, 
                 initial_level=0, initial_trend=0, initial_uncertainty=1.0):
        # Process noise covariance
        self.q = np.array([[process_noise, 0], 
                          [0, process_noise]])
        
        # Observation noise
        self.r = observation_noise
        
        # State transition matrix (assumes constant trend model)
        self.F = np.array([[1, 1], 
                          [0, 1]])
        
        # Observation matrix (we only observe the level)
        self.H = np.array([[1, 0]])
        
        # Initial state
        self.state = np.array([[initial_level], 
                             [initial_trend]])
        
        # Initial covariance
        self.P = np.eye(2) * initial_uncertainty
        
        # Store history for plotting/analysis
        self.history = {
            'levels': [],
            'trends': [],
            'timesteps': []
        }
    
    def update(self, measurement, timestep=None):
        """
        Update the filter with a new measurement.
        
        Args:
            measurement: The observed value
            timestep: Optional timestamp for tracking
        """
        # Prediction step
        self.state = self.F @ self.state
        self.P = self.F @ self.P @ self.F.T + self.q
        
        # Update step
        y = measurement - self.H @ self.state
        S = self.H @ self.P @ self.H.T + self.r
        K = self.P @ self.H.T / S
        
        self.state = self.state + K * y
        self.P = (np.eye(2) - K @ self.H) @ self.P
        
        # Store results
        self.history['levels'].append(float(self.state[0]))
        self.history['trends'].append(float(self.state[1]))
        self.history['timesteps'].append(timestep if timestep is not None else len(self.history['timesteps']))
        
        return self.get_estimate()
    
    def get_estimate(self):
        """Return current level and trend estimates"""
        return {
            'level': float(self.state[0]),
            'trend': float(self.state[1]),
            'uncertainty': self.P.tolist()
        }
    
    def predict(self, steps_ahead=1):
        """Predict future values based on current trend"""
        predicted_state = np.linalg.matrix_power(self.F, steps_ahead) @ self.state
        return float(predicted_state[0])