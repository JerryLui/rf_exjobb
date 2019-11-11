"""
Download data from Elastic Search server.
Requires: settings.py with username and password parameters
"""
import sys                                  # Used for local imports
sys.path.append("/home/jerry/Dropbox/Kurser/Master Thesis/rf_exjobb/scripts")   # Configure

from settings import username, password     # Import from settings.py
from elasticsearch import Elasticsearch, exceptions
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

import logging
import os


# Logging parameters
fp_log = 'elastic_query.log'                    # Configure
logging.basicConfig(filename=fp_log,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


def query_btime(start_time, window_time=5, save_data=False):
    '''

    :return:
    '''
    # Local parameters
    data_folder = '/home/jliu/rf_exjobb/'           # Configure
    fp_data = os.path.join(data_folder, 'data.csv')
    # fp_output = os.path.join(data_folder, 'output.pickle')

    server_addr = 'http://localhost:9200'           # Configure

    # Time parameters
    time_current = start_time
    time_change = timedelta(minutes=window_time)    # Configure

    # Query parameters
    QUERY_SIZE = 10000
    QUERY_IDX = 'elastiflow*'                      # Configure

    columns1 = ['src_addr', 'src_port', 'dst_addr', 'dst_port', 'ip_protocol', 'packets', 'bytes']
    columns2 = ['first_switched', 'last_switched']
    columns3 = ['ipaddr']
    columns = columns1 + columns2 + columns3

    r_columns = ['hits.hits._source.flow.' + _ for _ in columns1] + \
                ['hits.hits._source.netflow.' + _ for _ in columns2] + \
                ['hits.hits._source.node.' + columns3[0]]
    r_filter = ['_scroll_id', 'hits.total.value', 'hits.hits._source.@timestamp'] + r_columns

    try:
        logger.info('Initializing connection.')
        client = Elasticsearch(server_addr, http_auth=(username, password), timeout=600)
        client.info()
    except exceptions.AuthenticationException:
        logger.error('Client Authorization Failed.')
        sys.exit('Client Authorization Failed.')

    logger.info('Querying time %s' % time_current.isoformat())
    query_filter = \
        {'query':
             {'bool':
                  {'filter':
                       {'range':
                            {'@timestamp':
                                 {'gte': time_current.isoformat(),
                                  'lte': (time_current + time_change).isoformat()}
                             }
                        }
                   }
              }
         }

    response = client.search(index=QUERY_IDX, body=query_filter, size=QUERY_SIZE, scroll='2m', filter_path=r_filter)
    scroll_id = response['_scroll_id']

    # Process batches
    logger.info('Processing %i flows.' % response['hits']['total']['value'])

    response_batch = 1
    df_tmp = pd.DataFrame(columns=columns)
    while True:
        rows = []
        for hit in response['hits']['hits']:
            row = hit['_source']['flow']
            row.update(hit['_source']['netflow'])
            row.update(hit['_source']['node'])
            rows.append(row)

        df_tmp = df_tmp.append(pd.DataFrame.from_dict(rows), sort=False)

        # Exit condition
        if len(response['hits']['hits']) < QUERY_SIZE:
            logger.info('Processed %i batches.' % response_batch)
            break

        # Get next set
        response = client.scroll(scroll_id=scroll_id, scroll='2m', filter_path=r_filter)
        response_batch += 1

    if save_data:
        df_tmp.to_csv(fp_data)

    return df_tmp


if __name__ == '__main__':
    df = query_btime(datetime(2019, 9, 2, 9, 0))

