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


@profile
def run(start_time: datetime, end_time: datetime, window_size: timedelta):
    current_time = start_time
    eq = ElasticQuery(server, index, username, password)
    dp = DetectorPool()

    '''
    one = Detector(
            name='one',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.31,
            flag_th=6,
            detection_rule='two_step'
            )
            '''

    one_half = Detector(
            name='one_half',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.46,
            flag_th=6,
            detection_rule='two_step'
            )

    two = Detector(
            name='two',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.62,
            flag_th=6,
            detection_rule='two_step'
    )

    two_half = Detector(
            name='two_half',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.77,
            flag_th=6,
            detection_rule='two_step'
    )

    three = Detector(
            name='three',
            n_seeds=8,
            n_bins=1024,
            features=['external'],
            filt=int_ext_filter,
            thresh=0.93,
            flag_th=6,
            detection_rule='two_step'
            )

    # dp.add_detector(one)
    dp.add_detector(one_half)
    dp.add_detector(two)
    dp.add_detector(two_half)
    dp.add_detector(three)

    # name_list = ['one', 'two', 'three', 'one_half', 'two_half']
    name_list = ['two', 'three', 'one_half', 'two_half']
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

    for i, future in enumerate(as_completed(futures)):
        results = dp.run_next_timestep(future.result())
        detections.append(results[0])
        detection_frames.append(results[1])
        logger.debug(' '.join([str(len(_)) for _ in results]))

        futures[i] = None

        # Ye this is shit
        # max_dets['one'].append(one.get_max_det())
        max_dets['one_half'].append(one_half.get_max_det())
        max_dets['two'].append(two.get_max_det())
        max_dets['two_half'].append(two_half.get_max_det())
        max_dets['three'].append(three.get_max_det())

    full_detections = pd.concat(detection_frames)
    pd.to_pickle(full_detections, 'output/detection_frame.pkl')
    pd.to_pickle(detection_list_to_df(detections), 'output/detections.pkl')
    with open('output/max_dets.pkl', 'wb') as fp:
        pickle.dump(max_dets, fp, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    try:
        window_size = timedelta(minutes=15)
        run(datetime(2019, 11, 4, 0, 0), datetime(2019, 11, 5, 0, 0), window_size)
    except Exception as e:
        logger.fatal(e, exc_info=True)
    logger.debug('Finished')





