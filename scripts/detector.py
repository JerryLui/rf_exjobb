"""
Detects and extracts anomalies in input netflow data
"""

import pandas as pd
import numpy as np
from helper_functions import KL_divergence


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
        self.name         = None #Name used in logging
        self.n_seeds      = 1    #Number of seeds for detector to use
        self.n_bins       = 1    #Number of bins in histogram
        self.mav_steps    = 1    #Number of steps to take for the moving average
        self.features     = []   #Which features to run detection on
        #self.aggregations = []   #Feature/aggregation should probably be a pair
        self.filter       = None #Filter function to apply to data before detect
        self.thresh       = 1    #KL Threshold for a detection

        #Parameter-dependent initializations
        self.seeds = np.random.choice(100000, size=self.n_seeds, replace=False)
        self.bucket_limits = [i for i in range(2**32//self.n_bins, 2**32, 2**32//self.n_bins)]
        self.triggers = self.n_seeds - 2 #How many triggers to include extracted

        #Non-parameter initializations
        self.step = 0
        self.divs = np.zeros((len(self.features), self.n_seeds, self.mav_steps))
        #divs is used to calculate mav for every timestep
        self.mav  = np.zeros((len(self.features)))
        self.last_histograms = None
        #Need a way to save IP/bin pairs
        self.last_bin_set = None

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
        bin_set    = [[[set() for _ in range(self.n_bins)]
                    for __ in range(self.n_seeds)] for ___ in self.features]
        flags      = [[[]
                    for __ in range(self.n_seeds)] for ___ in self.features]

        for f, feat in enumerate(self.features):
            #AGGREGATIONS ARE NOT IMPLEMENTED YET

            feat_series = frame[feat]

            for s, seed in enumerate(self.seeds):
                unique, counts = np.unique(feat_series, return_counts=True)
                bins = hash_to_buckets(unique, self.bucket_limits, seed)

                for u, b, cnt in zip(unique, bins, counts):
                    histograms[f, s, b] += cnt
                    bin_set[f][s][b].add(u) #Bin set to be used for extraction

                #Calculate divergence, will be used in detection step
                if self.last_histograms is not None:
                    div = KL_divergence(histograms[f, s, :],
                            self.last_histograms[f, s, :])

                    #This overwrites the old elements of rolling div array
                    self.divs[f, s, 0] = div

            #Detection per seed, use mav method
            #What do we need?
            # - Mav array             -> rolling numpy array
            # - Last iteration's bins -> last_histograms
            if self.step > 1
                for s, seed in enumerate(self.seeds):
                    last = np.copy(self.last_histograms[f, s, :])
                    current = np.copy(histograms[f, s, :])
                    new_div = div
                    n = 0
                    while (new_div - mav[f]) > self.thresh and n < self.n_bins:
                        b = np.argmax(np.abs(last - current))
                        flags[f][s].append(b) #Flag bin b
                        current[b] = last[b]
                        new_div = KL_divergence(current, last)
                        n += 1

            #Extraction must also be done on this level

            #Check number of detected seeds
            n_triggers = sum([1 for s in flags[f] if s])
            total_dict = {}
            if n_triggers >= self.triggers:
                for s, seed n enumerate(self.seeds):
                    flag_bins = flags[f][s]
                    for b in flag_bins:
                        bin_set_union = bin_set[f][s][b].union(
                                self.last_bin_set[f][s][b])
                        for value in bin_set_union:
                            if value in total_dict.keys():
                                total_dict[value] = 1
                            else:
                                total_dict[value] += 1

            #Save to some results list ish

            if self.step < self.mav_steps:
                self.mav[f] = np.sum(self.divs[f, :, :]) / ((self.step + 1) * self.n_seeds)
            else:
                self.mav[f] = np.sum(self.divs[f, :, :]) / ((self.mav_steps + 1) * self.n_seeds)
                self.operational = True


        self.divs = np.roll(self.divs, 1, axis=2)

        self.step += 1
        self.last_histograms = histograms
        self.last_bin_set    = bin_set

    def applyfilter(self, frame):
        if self.filter is None:
            return frame
        else:
            return self.filter(frame)

