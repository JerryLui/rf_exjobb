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

import operator
import logging
import os


# Local parameters
save_data = False                               # Configure
data_file = None
output_file = None

data_folder = '/home/jliu/rf_exjobb/'           # Configure
fp_data = os.path.join(data_folder, 'data.csv')
fp_output = os.path.join(data_folder, 'output.pickle')
fp_log = 'elastic_query.log'                    # Configure

# Logging parameters
logging.basicConfig(filename=fp_log,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

# Time parameters
time_start = datetime(2019, 10, 28, 4, 0)       # Configure
time_end = datetime(2019, 11, 4, 4, 0)          # Configure
time_format = '%Y-%m-%d %H:%M:%S'

time_change = timedelta(minutes=5)              # Configure
time_current = time_start

# Query parameters
QUERY_SIZE = 10000
QUERY_MAX = 65536
QUERY_IDX = 'flow_index.*'                      # Configure

columns1 = ['src_addr', 'src_port', 'dst_addr', 'dst_port', 'ip_protocol', 'packets', 'bytes']
columns2 = ['first_switched', 'last_switched']

r_columns = ['hits.hits._source.flow.' + _ for _ in columns1] + ['hits.hits._source.netflow.' + _ for _ in columns2]
r_filter = ['_scroll_id', 'hits.total.value', 'hits.hits._source.@timestamp'] + r_columns

get_cols1 = operator.itemgetter(*columns1)
get_cols2 = operator.itemgetter(*columns2)

# Save file parameters
if save_data:
    data_file = open(fp_data, 'w')
    data_file.write(','.join(columns1 + columns2) + '\n')

try:
    logger.info('Initializing connection.')
    client = Elasticsearch('https://insikt-netflow.recfut.com', http_auth=(username, password), timeout=600)
    client.info()
except exceptions.AuthenticationException:
    logger.error('Client Authorization Failed.')
    sys.exit('Client Authorization Failed.')

while time_current < time_end:
    logger.info('Querying time %s' % time_current.strftime(time_format))
    query_filter = \
        {'query':
             {'bool':
                  {'filter':
                       {'range':
                            {'@timestamp':
                                 {'gte':time_current.strftime(time_format),
                                  'lte':(time_current + time_change).strftime(time_format)}
                             }
                        }
                   }
              }
         }

    response = client.search(index=QUERY_IDX, body=query_filter, size=QUERY_SIZE, scroll='2m', filter_path=r_filter)
    scroll_id = response['_scroll_id']

    logger.info('Processing %i flows.' % response['hits']['total'])

    # Process batches
    logger.info('Processing batches.')

    response_batch = 1
    while True:
        # Save data
        for hit in response['hits']['hits']:
            row = []
            row += list(get_cols1(hit['_source']['flow']))
            row += list(get_cols2(hit['_source']['netflow']))
            if save_data:
                data_file.write(','.join([str(_) for _ in row]) + '\n')

        # Exit condition
        if len(response['hits']['hits']) < QUERY_SIZE:
            logger.info('Processed %i batches.' % response_batch)
            break

        # Get next set
        response = client.scroll(scroll_id=scroll_id, scroll='2m')
        response_batch += 1

    if save_data:
        data_file.close()
