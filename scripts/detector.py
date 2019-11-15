"""
Detects and extracts anomalies in input netflow data
"""
import pandas as pd
import numpy as np
import logging
from helper_functions import KL_divergence, hash_to_buckets

# Logging
logger = logging.getLogger('rf_exjobb')

class Detection():
    '''
    Small wrapper for the information of a single detection.
    '''

    def __init__(self, detector, operational, feature, value, number, timestep):
        '''
        Detection object corresponding to a single detection from one Detector

        :param detector: Name of detector which found this value
        :param operational: Bool whether the detector is in an operational state or not
        :param feature: Which of the detectors aggregation features triggered a detection
        :param value: The value of the triggered feature
        :param number: The number of hash functions in which this value was contained in a flagged bin
        :param timestep: How many steps the detector has processed
        '''
        self.detector    = detector
        self.operational = operational
        self.feature     = feature
        self.value       = value
        self.number      = number
        self.timestep    = timestep

    def __repr__(self):
        return '\n%s\t%s\t%s\t%s' % (self.detector, self.feature, self.value, self.number)

class DetectorPool():
    '''
    A detector pool container which feeds the input dataframes forward to
    its detectors. When all data is consumed, the DetectorPool outputs
    the final detections.
    '''

    def __init__(self):
        '''
        DetectorPool is a thin wrapper for running multiple detectors on the same data
        '''
        self.detectors = []

    def add_detector(self, new_det):
        '''
        Add a detector object to the pool

        :param new_det: New detector object
        '''
        self.detectors.append(new_det)

    def run_next_timestep(self, frame):
        '''
        Runs the next timestep. Every detector is fed the same dataframe, and all detections are returned in a list

        :param frame: Pandas dataframe to feed into detectors
        :return: Tuple like (List of Detection objects, DataFrame containing relevant flows)
        '''
        new_detections = []
        detection_frames = []
        for det in self.detectors:
            (dets, det_frame) = det.run_next_timestep(frame)
            for d in dets:
                new_detections.append(d)
            detection_frames.append(det_frame)
        detection_frame = pd.concat(detection_frames, axis=0, ignore_index=True)
        return (new_detections, detection_frame)

    def get_detector_divs(self):
        '''
        Get the KL-divergences from the last timestep from every detector

        :return: Dictionary keyed by detector name with numpy arrays of KL-divergence
        '''
        divs = {}
        for det in self.detectors:
            divs[det.name] = det.get_divs()
        return divs

    def get_detector_mavs(self):
        '''
        Get the moving average of the KL-divergences from the last few (detector dependent) steps

        :return: Dcitionary keyed by detector name with numpy arrays of moving averages
        '''
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
        '''
        The Detector analyzes some feature using DESCRIPTION

        :param name: Detector name
        :param n_seeds: Number of unique hash functions to use in detector
        :param n_bins: The number of possible outcomes of every hash function
        :param mav_steps: Number of steps to calculate moving average of, to use for detection
        :param features: List of features to aggregate and analyze
        :param filt: Filter function which is applied to every datafram befor analysis
        :param thresh: Threshold value for detection of KL-divergences compared to moving average 
        '''
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
        Runs the given dataframe as the next timestep

        :param frame: Pandas dataframe to analyze
        :returns: Tuple like (List of Detection objects, Dataframe containing all flows related to detection)
        '''
        frame = self.applyfilter(frame)

        histograms = np.zeros((len(self.features), self.n_seeds, self.n_bins))
        bin_set    = [[[set() for _ in range(self.n_bins)]
                    for __ in range(self.n_seeds)] for ___ in self.features]
        flags      = [[None
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
                    last = np.copy(self.last_histograms[f, s, :])
                    current = np.copy(histograms[f, s, :])
                    bins = self.mav_detection(self.mav[f], div, current, last)
                    flags[f][s] = bins

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

        sub_frames = []
        for det in detections:
            feat = det.feature
            val = det.value
            sub = frame.loc[
                    frame[feat] == val
                    ]
            sub_frames.append(sub)
        if sub_frames:
            detection_frame = pd.concat(sub_frames, axis=0)
        else:
            detection_frame = pd.DataFrame()

        return (detections, detection_frame)

    def mav_detection(self, mav, div, current, last):
        '''
        Run moving average detection

        :param mav: The current moving average to compare to
        :param div: KL-divergence for current timestep (only supplied to not recalculate)
        :param current: Current histogram to compare
        :param last: Last histogram to compare
        :return: Bins which trigger the moving average detectiuon rule
        '''
        new_div = div
        n = 0
        bins = []
        while (new_div - mav) > self.thresh and n < self.n_bins:
            b = np.argmax(np.abs(last - current))
            bins.append(b) #Flag bin b
            current[b] = last[b]
            new_div = KL_divergence(current, last)
            n += 1
        return bins


    def applyfilter(self, frame):
        '''
        Apply the filter function self.filt to an input dataframe

        :param frame: Frame to run through filter
        :return: Filtered frame
        '''
        if self.filt is None:
            return frame
        else:
            return self.filt(frame)

    def get_divs(self):
        '''
        Returns divs for plotting
        The 1 index is needed, as roll happens at the end of every iteration

        :return: KL-divergences of last timestep
        '''
        return self.divs[:, :, 1]

    def get_mav(self):
        '''
        Return the current moving average for plotting

        :return: Moving average of KL-divergence as of the last timestep
        '''
        return self.mav

