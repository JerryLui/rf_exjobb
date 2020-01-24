from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pickle
import numpy as np
import logging
import sys
import gc

sys.path.append("/home/jliu/rf_exjobb/scripts/")  # Configure

from elasticquery import ElasticQuery
from detector import Detector, DetectorPool, Detection
from settings import server, index, username, password
from helper_functions import int_ext_filter, protocol_filter, detection_list_to_df
import gc

# Logging initialization
fp_log = datetime.now().strftime('logs/l%d%H%M.log')  # Configure
logging.basicConfig(filename=fp_log,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger('rf_exjobb')

es_logger = logging.getLogger('elasticsearch')
es_logger.propagate = False

ul_logger = logging.getLogger('urllib3.connectionpool')
ul_logger.propagate = False


def run(start_time: datetime, end_time: datetime, window_size: timedelta):
    current_time = start_time
    eq = ElasticQuery(server, index, username, password)
    dp = DetectorPool()

    detectors = [
        Detector(
            name='ext_4_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.36,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='int_4_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['internal'],
            filt=int_ext_filter,
            thresh=0.44,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='src_4_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['src_addr'],
            filt=None,
            thresh=0.32,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='dst_4_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['dst_addr'],
            filt=None,
            thresh=0.32,
            flag_th=6,
            detection_rule='two_step'
        )
    ]

    name_list = []
    all_divs = {}

    # Add all detectors to detection pool for concurrency
    for detector in detectors:
        dp.add_detector(detector)
        name_list.append(detector.name)
        all_divs[detector.name] = []

    # Threading
    detections = []
    detection_frames = []

    divs_detector = detectors[0]  # Only need the divs from one detector
    ext_divs = []

    # Main Operation Loop
    while current_time < end_time:
        # Load the data from local drive/ElasticSearch
        df = eq.query_time(current_time, window_size)
        current_time += window_size

        # Run detectors
        results = dp.run_next_timestep(df)

        # Result processing
        detections.append(results[0])
        detection_frames.append(results[1])
        logger.debug(' '.join([str(len(_)) for _ in results]))

        for det in detectors:
            all_divs[det.name].append(det.get_divs())

        ext_divs.append(divs_detector.get_divs())

    full_detections = pd.concat(detection_frames)
    window_size_fmt = int(window_size.total_seconds() / 60)
    pd.to_pickle(full_detections, 'output/detection_frame_{}-{}_{}.pkl'.format(start_time.day,
                                                                               start_time.month,
                                                                               window_size_fmt))
    pd.to_pickle(detection_list_to_df(detections), 'output/detections_{}-{}_{}.pkl'.format(start_time.day,
                                                                                           start_time.month,
                                                                                           window_size_fmt))
    with open('output/ext_divs_{}-{}_{}.pkl'.format(start_time.day, start_time.month, window_size_fmt), 'wb') as fp:
        pickle.dump(ext_divs, fp, protocol=pickle.HIGHEST_PROTOCOL)
    for det in detectors:
        with open('output/divs_{}_{}-{}_{}.pkl'.format(det.name, start_time.day, start_time.month, window_size_fmt), 'wb') as fp:
            pickle.dump(all_divs[det.name], fp, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    try:
        window_size = timedelta(minutes=5)
        # Earliest 30 days before today
        for i in [9, 10, 11, 12, 13]:
            logger.debug('Starting run for day %i' % i)
            run(datetime(2019, 12, i, 0, 0), datetime(2019, 12, i+1, 0, 0), window_size)
    except Exception as e:
        logger.fatal(e, exc_info=True)
    logger.debug('Finished')
