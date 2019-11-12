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
    def __init__(self, es_server, es_index, username, password):
        """
        ElasticQuery object for querying dataframes from server

        :param es_server: es server addr
        :param index: es index on server
        """
        self.QUERY_SIZE = 10000
        self.es_index = es_index
        self.es_server = es_server

        try:
            logger.debug('Initializing connection.')
            self.client = Elasticsearch(self.es_server, http_auth=(username, password), timeout=30000)
            self.client.info()
        except exceptions.AuthenticationException as e:
            logger.error('Client Authorization Failed.')
            raise e

        # Columns of interest
        self.col_flow = ['src_addr', 'src_port', 'dst_addr', 'dst_port', 'ip_protocol', 'packets', 'bytes']
        self.col_netflow = ['first_switched', 'last_switched']
        self.col_node = ['ipaddr']
        self.columns = self.col_flow + self.col_netflow + self.col_node

        self.response_columns = ['hits.hits._source.flow.' + _ for _ in self.col_flow] + \
                                ['hits.hits._source.netflow.' + _ for _ in self.col_netflow] + \
                                ['hits.hits._source.node.' + _ for _ in self.col_node]
        self.response_filter = ['_scroll_id', 'hits.total.value', 'hits.hits._source.@timestamp'] + self.response_columns

    def query_time(self, start_time: datetime, window_time=5):
        """
        Queries ElasticSearch server starting at start_time

        :param start_time: datetime to start search at
        :param window_time: time window size of search
        :param save_data: file path to save data

        :return: dataframe containing data in the time window if any
        """
        # Time parameters
        time_current = start_time
        time_change = timedelta(minutes=window_time)

        query = \
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
        return self._run_query(query)

    def _run_query(self, query):
        """
        Queries ElasticSearch server with given query body, saves result to file if a path is given

        :param query: query body given to elastic search
        :return: results as a dataframe
        """
        response = self.client.search(index=self.es_index,
                                      body=query,
                                      size=self.QUERY_SIZE,
                                      scroll='2m',
                                      filter_path=self.response_filter)
        scroll_id = response['_scroll_id']
        n_flows = response['hits']['total']['value']
        if n_flows == 0:
            raise exceptions.NotFoundError()

        # Process batches
        logger.debug('Processing %i flows.' % n_flows)

        df_tmp = pd.DataFrame(columns=self.columns)

        response_batch = 1
        while True:
            rows = []
            for hit in response['hits']['hits']:
                row = hit['_source']['flow']
                row.update(hit['_source']['netflow'])
                row.update(hit['_source']['node'])
                rows.append(row)

            df_tmp = df_tmp.append(pd.DataFrame.from_dict(rows), sort=False)

            # Exit condition
            if len(response['hits']['hits']) < self.QUERY_SIZE:
                logger.debug('Processed %i batches.' % response_batch)
                break

            # Get next set
            response = self.client.scroll(scroll_id=scroll_id, scroll='2m', filter_path=self.response_filter)
            response_batch += 1

        return df_tmp


if __name__ == '__main__':
    eq = ElasticQuery(server, index, username, password)
    df = eq.query_time(datetime(2019, 9, 2, 9, 0))
    # df = eq.query_time(datetime(2019, 10, 28, 9, 0))

