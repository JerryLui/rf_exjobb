"""
Detects and extracts anomalies in input netflow data
"""

import pandas as pd
import numpy as np
from helper_functions import KL_divergence

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

class DetectorPool():
    '''
    A detector pool container which feeds the input dataframes forward to
    its detectors. When all data is consumed, the DetectorPool outputs
    the final detections.
    '''

    def __init__():
        pass

class Detector():
    '''
    A detector which takes a dataframe and filters it, then runs the
    detection algorithm on the subset.
    '''

    def __init__(self):
        self.name         = None
        self.n_seeds      = 1
        self.n_bins       = 1
        self.mav_steps    = 1
        self.features     = []
        self.aggregations = []
        self.filter       = {}

        #Parameter-dependent initializations
        self.seeds = np.random.choice(100000, size=self.n_seeds, replace=False)
        self.bucket_limits = [i for i in range(2**32//self.n_bins, 2**32, 2**32//self.n_bins)]

        #Non-parameter initializations
        self.step = 0
        self.divs = np.zeros((1, ))
        self.mav  = np.zeros((1, ))

        #Bool to signify if detector is ready for detection
        # (~mav steps have passed)
        self.operational = False

    def run_next_timestep(self, frame):
        '''
        Consumes all new data in frame
        '''
        frame = self.applyfilter(frame)

        for f, feat in enumerate(self.features):
            #AGGREGATIONS ARE NOT IMPLEMENTED YET

            feat_series = frame[feat]

            for s, seed in enumerate(self.seeds):
                unique, counts = np.unique(feat_series, return_counts=True)

                for uni, cnt in zip(unique, counts):
                    #Hash features (And save?)
                    #Add to histogram (Vectorize?)
                    pass

            #Detection per seed, use mav method
            for s, seed in enumerate(self.seeds):
                pass

            #Extraction must also be done on this level
            #Check number of detected seeds

        step += 1

    def applyfilter(self, frame):
        pass

