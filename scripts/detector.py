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

    def __init__(self):
        self.detectors = []

    def add_detector(self, new_det):
        self.detectors.append(new_det)

    def next_timestep(self, frame):
        new_detections = []
        for det in self.detectors:
            new = det.run_next_timestep(frame)
            new_detections.append(new)
        print(new_detections) #<- or some version of this
        

    def get_results():
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
        self.aggregations = [] #Feature/aggregation should probably be a pair
        self.filter       = {}
        self.thresh       = 1

        #Parameter-dependent initializations
        self.seeds = np.random.choice(100000, size=self.n_seeds, replace=False)
        self.bucket_limits = [i for i in range(2**32//self.n_bins, 2**32, 2**32//self.n_bins)]

        #Non-parameter initializations
        self.step = 0
        self.divs = np.zeros((len(self.features), self.n_seeds))
        #TODO: Some mav frame
        self.mav  = np.zeros((len(self.features))) #Use np.roll
        self.last_histograms = None

        #Bool to signify if detector is ready for detection
        # (~mav steps have passed)
        self.operational = False

    def run_next_timestep(self, frame):
        '''
        Consumes all new data in frame
        '''
        frame = self.applyfilter(frame)

        histograms = np.zeros((len(self.features), self.n_seeds, self.n_bins))
        divs       = np.zeros((len(self.features), self.n_seeds)))

        for f, feat in enumerate(self.features):
            #AGGREGATIONS ARE NOT IMPLEMENTED YET

            feat_series = frame[feat]

            for s, seed in enumerate(self.seeds):
                unique, counts = np.unique(feat_series, return_counts=True)

                bins = hash_to_buckets(unique, self.bucket_limits, seed)

                for b, cnt in zip(bins, counts):
                    histograms[f, s, b] += cnt

                #Calculate divergence, will be used in detection step
                if self.last_histograms is not None:
                    div = KL_divergence(histograms[f, s, :],
                            self.last_histograms[f, s, :])
                    divs[f, s] = div

            #Detection per seed, use mav method
            #What do we need?
            # - Mav array             -> rolling numpy array
            # - Last iteration's bins -> last_histograms
            for s, seed in enumerate(self.seeds):
                pass

            #Extraction must also be done on this level
            #Check number of detected seeds

        if step < self.mav_steps:
            self.mav[f] = None
        else:
            self.mav[f] = None

        step += 1
        self.last_histograms = histograms

    def applyfilter(self, frame):
        pass

