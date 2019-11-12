"""
Download data from Elastic Search server.
Requires: settings.py with server, username and password parameters

@Author Jerry Liu jerry.liu@recordedfuture.com
"""
import sys  # Used for local imports

sys.path.append("/home/jerry/Dropbox/Kurser/Master Thesis/rf_exjobb/scripts")  # Configure

from settings import username, password, server, index  # Import from settings.py
from elasticsearch import Elasticsearch, exceptions
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

import logging
import os


# Configuration parameters
fp_log = 'elastic_query.log'  # Configure

# Logging initialization
logging.basicConfig(filename=fp_log,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

es_logger = logging.getLogger('elasticsearch')
es_logger.propagate = False

ul_logger = logging.getLogger('urllib3.connectionpool')
ul_logger.propagate = False


class ElasticQuery(object):
    def __init__(self, server_address, index, username, password):
        """
        ElasticQuery object for querying dataframes from server

        :param server_address: es server addr
        :param index: es index on server
        """
        self.index = index
        try:
            logger.debug('Initializing connection.')
            self.client = Elasticsearch(server_address, http_auth=(username, password), timeout=30000)
            self.client.info()
        except exceptions.AuthenticationException as e:
            logger.error('Client Authorization Failed.')
            raise e

    def query_time(self, start_time: datetime, window_time=5, save_data=''):
        """
        Queries ElasticSearch server starting at start_time

        :param start_time: datetime to start search at
        :param window_time: time window size of search
        :param save_data: file name to save data

        :return: dataframe containing data in the time window
        """
        # Query parameters
        query_index = self.index
        query_size = 10000

        # save_file parameters
        if save_data:
            data_folder = os.getcwd()  # Configure
            fp_data = os.path.join(data_folder, save_data)

        # Time parameters
        time_current = start_time
        time_change = timedelta(minutes=window_time)

        # Query parameters
        columns1 = ['src_addr', 'src_port', 'dst_addr', 'dst_port', 'ip_protocol', 'packets', 'bytes']
        columns2 = ['first_switched', 'last_switched']
        columns3 = ['ipaddr']
        columns = columns1 + columns2 + columns3

        r_columns = ['hits.hits._source.flow.' + _ for _ in columns1] + \
                    ['hits.hits._source.netflow.' + _ for _ in columns2] + \
                    ['hits.hits._source.node.' + _ for _ in columns3]
        r_filter = ['_scroll_id', 'hits.total.value', 'hits.hits._source.@timestamp'] + r_columns

        query_filter = \
            {'query':
                 {'bool':
                      {'filter':
                           {'range':
                                {'@timestamp':
                                     {'gte': time_current.isoformat(),
                                      'lt': (time_current + time_change).isoformat()}
                                 }
                            }
                       }
                  }
             }

        logger.debug('Querying time %s' % time_current.isoformat())
        response = self.client.search(index=query_index,
                                      body=query_filter,
                                      size=query_size,
                                      scroll='2m',
                                      filter_path=r_filter)

        scroll_id = response['_scroll_id']

        # Process batches
        logger.debug('Processing %i flows.' % response['hits']['total']['value'])

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
            if len(response['hits']['hits']) < query_size:
                logger.debug('Processed %i batches.' % response_batch)
                break

            # Get next set
            response = self.client.scroll(scroll_id=scroll_id, scroll='2m', filter_path=r_filter)
            response_batch += 1

        if save_data:
            df_tmp.to_csv(fp_data)

        return df_tmp


if __name__ == '__main__':
    eq = ElasticQuery(server, index, username, password)
    # df = eq.query_time(datetime(2019, 9, 2, 9, 0))
    df = eq.query_time(datetime(2019, 10, 28, 9, 0))

