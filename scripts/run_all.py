from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import sys
sys.path.append("/home/jliu/rf_exjobb/scripts/")  # Configure

from elasticquery import ElasticQuery
from detector import Detector, DetectorPool
from settings import server, index, username, password


# Logging initialization
fp_log = 'elastic_query.log'  # Configure
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
    det = Detector(
        name='Detector',
        n_seeds=8,
        n_bins=256,
        mav_steps=5,
        features=['src_addr', 'dst_addr'],
        filt=None,
        thresh=0.1
    )

    while current_time < end_time:
        data = eq.query_time(start_time, window_size)
        results = det.run_next_timestep(data)
        print(results)
        current_time += window_size


if __name__ == '__main__':
    time_window = timedelta(minutes=5)
    run(datetime(2019, 10, 28, 4, 0), datetime(2019, 10, 28, 4, 5), time_window)





