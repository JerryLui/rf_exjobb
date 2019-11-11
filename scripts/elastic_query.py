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


# Local parameters
save_data = False                               # Configure
output_file = None

data_folder = '/home/jliu/rf_exjobb/'           # Configure
fp_data = os.path.join(data_folder, 'data.csv')
fp_output = os.path.join(data_folder, 'output.pickle')
fp_log = 'elastic_query.log'                    # Configure

server_addr = 'http://localhost:9200'           # Configure

# Logging parameters
logging.basicConfig(filename=fp_log,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger('elastic_query')

# Time parameters
time_start = datetime(2019, 9, 2, 9, 0)       # Configure
time_end = datetime(2019, 9, 2, 13, 0)          # Configure

time_change = timedelta(minutes=5)              # Configure
time_current = time_start

# Query parameters
QUERY_SIZE = 10000
QUERY_MAX = 65536
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

df_flows = pd.DataFrame(columns=columns)
while time_current < time_end:
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
        # Save data
        rows = []
        for hit in response['hits']['hits']:
            row = hit['_source']['flow']
            row.update(hit['_source']['netflow'])
            row.update(hit['_source']['node'])
            rows.append(row)

        df_tmp = df_tmp.append(pd.DataFrame.from_dict(rows))

        # Exit condition
        if len(response['hits']['hits']) < QUERY_SIZE:
            logger.info('Processed %i batches.' % response_batch)
            break

        # Get next set
        response = client.scroll(scroll_id=scroll_id, scroll='2m', filter_path=r_filter)
        response_batch += 1

    if save_data:
        df_tmp.to_csv(fp_data)

    df_flows = df_flows.append(df_tmp)


