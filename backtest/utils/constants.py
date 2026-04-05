"""
Constants and mappings for the backtest package.

Contains month codes, contract types, and other trading-related constants.
"""

# Month code to number mapping (futures contract codes)
MONTH_CODES = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
}

# Number to month code mapping (reverse of MONTH_CODES)
MONTH_TO_CODE = {v: k for k, v in MONTH_CODES.items()}

# Month letter to number mapping
MONTH_TO_NUM = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
}

# Number to month letter mapping
NUM_TO_MONTH = {
    1: 'F',   # January
    2: 'G',   # February
    3: 'H',   # March
    4: 'J',   # April
    5: 'K',   # May
    6: 'M',   # June
    7: 'N',   # July
    8: 'Q',   # August
    9: 'U',   # September
    10: 'V',  # October
    11: 'X',  # November
    12: 'Z'   # December
}

# Contract type descriptions
CONTRACT_TYPE = {
    "Q": "quarterly",
}

# Contract type to factor mapping
CONTRACT_FACTOR = {
    'monthly': 1,
    'quarterly': 3,
}

# Pre-roll configurations by contract type
PREROLL = {
    'Q': [0, 1],
}