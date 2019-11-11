"""
Detects and extracts anomalies in input netflow data
"""

import pandas as pd
import numpy as np

"""
Parameters:
"""

N_SEEDS = 16
N_BINS  = 128

START_TIME = None
END_TIME = START_TIME # + minutes
STEP_LENGTH = 5

DIV_THRESHOLD = 0.1 #Tune this by feature
SEED_THRESHOLD = N_SEEDS - 4 #Tune this by feature

features = []

#Moving average / trailing distribution?
#MAV_STEPS = 6
#TRAIL_LEN = 6
