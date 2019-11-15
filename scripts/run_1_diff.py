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
fp_log = datetime.now().strftime('l%d%H%M.log')  # Configure
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
        name='Dummy',
        n_seeds=1,
        n_bins=1,
        mav_steps=1,
        features=['src_addr', 'dst_addr', 'internal', 'external'],
        filt=int_ext_filter,
        thresh=10
    )

    divs = []

    while current_time < end_time:
        frame = eq.query_time(current_time, window_size)
        #Do not care about results
        det.run_next_timestep(frame)
        divs.append(det.get_divs())
        current_time += window_size

    #Merge all divs?
    final_divs = np.concatenate(divs)
    np.save('output/1_diff_1_seed', divs)
    #Save as a pickle

if __name__ == '__main__':
    window_size = timedelta(minutes=5)
    run(datetime(2019, 10, 28, 4, 0), datetime(2019, 10, 29, 4, 5))

