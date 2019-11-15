from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import logging
import sys
sys.path.append("/home/jliu/rf_exjobb/scripts/")  # Configure

from elasticquery import ElasticQuery
from detector import Detector, DetectorPool, Detection
from settings import server, index, username, password
from helper_functions import int_ext_filter, protocol_filter


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

    tcp_det = Detector(
        name='TCP',
        n_seeds=8,
        n_bins=256,
        mav_steps=5,
        features=['src_addr', 'dst_addr'],
        filt=protocol_filter('TCP'),
        thresh=0.1
    )
    udp_det = Detector(
        name='UDP',
        n_seeds=8,
        n_bins=64,
        mav_steps=5,
        features=['src_addr', 'dst_addr'],
        filt=protocol_filter('UDP'),
        thresh=0.1
    )
    icmp_det = Detector(
        name='ICMP',
        n_seeds=8,
        n_bins=16,
        mav_steps=5,
        features=['src_addr', 'dst_addr'],
        filt=protocol_filter('ICMP'),
        thresh=0.1
    )
    dp.add_detector(tcp_det)
    dp.add_detector(udp_det)
    dp.add_detector(icmp_det)

    # Threading
    futures = []
    thread_pool = ThreadPoolExecutor(1)
    while current_time < end_time:
        futures.append(thread_pool.submit(eq.query_time, current_time, window_size))
        current_time += window_size

    for future in as_completed(futures):
        results = dp.run_next_timestep(future.result())
        logger.debug(results)


if __name__ == '__main__':
    window_size = timedelta(minutes=5)
    run(datetime(2019, 10, 28, 4, 0), datetime(2019, 10, 29, 4, 5), window_size)





