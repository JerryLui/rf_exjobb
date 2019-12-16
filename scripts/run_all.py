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
            name='2.5_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.166,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='2.75_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.182,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='3_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.2,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='3.25_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.216,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='3.5_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.232,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='3.75_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.249,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='4_sigma',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.265,
            flag_th=6,
            detection_rule='two_step'
        ),
        # ICMP Detectors
        Detector(
            name='ICMP_64_2.5_sigma',
            n_seeds=8,
            n_bins=64,
            features=['external'],
            filt=protocol_filter('ICMP'),
            thresh=0.166,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='ICMP_64_3_sigma',
            n_seeds=8,
            n_bins=64,
            features=['external'],
            filt=protocol_filter('ICMP'),
            thresh=0.2,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='ICMP_32_2.5_sigma',
            n_seeds=8,
            n_bins=32,
            features=['external'],
            filt=protocol_filter('ICMP'),
            thresh=0.166,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='ICMP_32_3_sigma',
            n_seeds=8,
            n_bins=32,
            features=['external'],
            filt=protocol_filter('ICMP'),
            thresh=0.2,
            flag_th=6,
            detection_rule='two_step'
        ),
        # UDP Detectors
        Detector(
            name='UDP_2.5_sigma',
            n_seeds=8,
            n_bins=128,
            features=['external'],
            filt=protocol_filter('UDP'),
            thresh=0.166,
            flag_th=6,
            detection_rule='two_step'
        ),
        Detector(
            name='UDP_3_sigma',
            n_seeds=8,
            n_bins=128,
            features=['external'],
            filt=protocol_filter('UDP'),
            thresh=0.2,
            flag_th=6,
            detection_rule='two_step'
        )
    ]

    name_list = []
    for detector in detectors:
        dp.add_detector(detector)
        name_list.append(detector.name)

    max_dets = {}
    for n in name_list:
        max_dets[n] = []

    # Threading
    futures = []
    thread_pool = ThreadPoolExecutor(1)
    while current_time < end_time:
        futures.append(thread_pool.submit(eq.query_time, current_time, window_size))
        current_time += window_size

    detections = []
    detection_frames = []

    divs_detector = detectors[-1] #Only need the divs from one detector
    ext_divs = []

    for i, future in enumerate(as_completed(futures)):
        results = dp.run_next_timestep(future.result())
        detections.append(results[0])
        detection_frames.append(results[1])
        logger.debug(' '.join([str(len(_)) for _ in results]))

        futures[i] = None

        for detector in detectors:
            max_dets[detector.name].append(detector.get_max_det())

        ext_divs.append(divs_detector.get_divs())

    full_detections = pd.concat(detection_frames)
    window_size_fmt = int(window_size.total_seconds() / 60)
    pd.to_pickle(full_detections, 'output/detection_frame_{}-{}_{}.pkl'.format(start_time.day,
                                                                               start_time.month,
                                                                               window_size_fmt))
    pd.to_pickle(detection_list_to_df(detections), 'output/detections_{}-{}_{}.pkl'.format(start_time.day,
                                                                                           start_time.month,
                                                                                           window_size_fmt))
    with open('output/max_dets_{}-{}_{}.pkl'.format(start_time.day, start_time.month, window_size_fmt), 'wb') as fp:
        pickle.dump(max_dets, fp, protocol=pickle.HIGHEST_PROTOCOL)
    with open('output/ext_divs_{}-{}_{}.pkl'.format(start_time.day, start_time.month, window_size_fmt), 'wb') as fp:
        pickle.dump(ext_divs, fp, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    try:
        window_size = timedelta(minutes=5)
        # Earliest 30 days before today
        for i in range(3, 7):
            run(datetime(2019, 12, i, 0, 0), datetime(2019, 12, i+1, 0, 0), window_size)
    except Exception as e:
        logger.fatal(e, exc_info=True)
    logger.debug('Finished')
