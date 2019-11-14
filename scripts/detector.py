"""
Detects and extracts anomalies in input netflow data
"""

import pandas as pd
import numpy as np
from helper_functions import KL_divergence, hash_to_buckets

class Detection():
    '''
    Small wrapper for the information of a single detection.
    A list of detections is returned by the detector(pool) for every iteration
    '''

    def __init__(self, detector, operational, feature, value, number, timestep):
        self.detector    = detector
        self.operational = operational
        self.feature     = feature
        self.value       = value
        self.number      = number
        self.timestep    = timestep

    def __repr__(self):
        return '\n%s:\t%s:\t%s:' % (self.detector, self.feature, self.value)

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
            for d in new:
                new_detections.append(d)
        print(new_detections) #<- or some version of this
        
    def get_results():
        pass

    def get_detector_divs(self):
        divs = {}
        for det in self.detectors:
            divs[det.name] = det.get_divs()
        return divs

    def get_detector_mavs(self):
        mavs = {}
        for det in self.detectors:
            mavs[det.name] = det.get_mav()
        return mavs

class Detector():
    '''
    A detector which takes a dataframe and filters it, then runs the
    detection algorithm on the subset.
    '''

    def __init__(self, name, n_seeds, n_bins,
            mav_steps, features, filt, thresh):
        self.name         = name      #Name used in logging
        self.n_seeds      = n_seeds   #Number of seeds for detector to use
        self.n_bins       = n_bins    #Number of bins in histogram
        self.mav_steps    = mav_steps    #Number of steps to take for the moving average
        self.features     = features  #Which features to run detection on
        #self.aggregations = []       #Feature/aggregation should probably be a pair
        self.filt         = filt      #Filter function to apply to data before detect
        self.thresh       = thresh    #KL Threshold for a detection

        #Parameter-dependent initializations
        self.seeds = np.random.choice(100000, size=self.n_seeds, replace=False)
        self.bucket_limits = [i for i in range(2**32//self.n_bins, 2**32, 2**32//self.n_bins)]
        self.flag_th = self.n_seeds - 2 #How many flags to include extracted

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
        bin_set    = [[[set() for _ in range(self.n_bins)]
                    for __ in range(self.n_seeds)] for ___ in self.features]
        flags      = [[[]
                    for __ in range(self.n_seeds)] for ___ in self.features]


        detections = []

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
            # - Last iteration's bins -> last_histograms
            if self.step > 1:
                for s, seed in enumerate(self.seeds):
                    last = np.copy(self.last_histograms[f, s, :])
                    current = np.copy(histograms[f, s, :])
                    new_div = div
                    n = 0
                    while (new_div - self.mav[f]) > self.thresh and n < self.n_bins:
                        b = np.argmax(np.abs(last - current))
                        flags[f][s].append(b) #Flag bin b
                        current[b] = last[b]
                        new_div = KL_divergence(current, last)
                        n += 1

            #Extraction must also be done on this level

            #Check number of detected seeds
            n_flags = sum([1 for s in flags[f] if s])
            total_dict = {}
            if n_flags >= self.flag_th:
                for s, seed in enumerate(self.seeds):
                    flag_bins = flags[f][s]
                    for b in flag_bins:
                        bin_set_union = bin_set[f][s][b].union(
                                self.last_bin_set[f][s][b])
                        for value in bin_set_union:
                            if value in total_dict.keys():
                                total_dict[value] += 1
                            else:
                                total_dict[value] = 1
                for (k, v) in total_dict.items():
                    if v >= self.flag_th:
                        detection = Detection(
                                detector=self.name,
                                operational=self.operational,
                                feature=feat,
                                value=k,
                                number=v,
                                timestep=self.step
                                )
                        detections.append(detection)

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

        return detections

    def applyfilter(self, frame):
        '''
        Apply the filter function self.filt to an input dataframe
        '''
        if self.filt is None:
            return frame
        else:
            return self.filt(frame)

    def get_divs(self):
        '''
        Returns divs for plotting
        The 1 index is needed, as roll happens at the end of every iteration
        '''
        return self.divs[:, :, 1]

    def get_mav(self):
        '''
        Return the current moving average for plotting
        '''
        return self.mav

