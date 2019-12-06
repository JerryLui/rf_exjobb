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
    
    src_dst = Detector(
        name='src_dst',
        n_seeds=1,
        n_bins=1024,
        features=['src_addr', 'dst_addr'],
        filt=None,
        thresh=10,
        flag_th=1
    )
    int_ext = Detector(
        name='int_ext',
        n_seeds=1,
        n_bins=1024,
        features=['internal', 'external'],
        filt=int_ext_filter,
        thresh=10,
        flag_th=1
        )

    dp.add_detector(src_dst)
    dp.add_detector(int_ext)

    src_dst_divs = []
    int_ext_divs = []

    while current_time < end_time:
        frame = eq.query_time(current_time, window_size)
        #Do not care about results
        dp.run_next_timestep(frame)
        
        src_dst_divs.append(src_dst.get_divs())
        int_ext_divs.append(int_ext.get_divs())
        
        current_time += window_size

    #Merge all divs?
    src_dst_divs = np.concatenate(src_dst_divs)
    int_ext_divs = np.concatenate(int_ext_divs) 
    np.save('output/src_dst_divs_15_1024', src_dst_divs)
    np.save('output/int_ext_divs_15_1024', int_ext_divs)
    #Save as a pickle

if __name__ == '__main__':
    window_size = timedelta(minutes=15)
    run(datetime(2019, 11, 5, 0, 0), datetime(2019, 11, 6, 0, 0), window_size)

