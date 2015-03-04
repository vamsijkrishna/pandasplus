import numpy as np

def safe_float(x):
    if not x:
        return np.nan
    return np.float64(x)