"""
Download data from Elastic Search server.
Requires: settings.py with server, username and password parameters

@Author Jerry Liu jerry.liu@recordedfuture.com
"""
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, exceptions
import pandas as pd
import numpy as np
import logging


# Logging
logger = logging.getLogger('rf_exjobb')


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
            self.client = Elasticsearch(self.es_server, http_auth=(username, password), timeout=600)
        except exceptions.AuthenticationException as e:
            logger.error('Client Authorization Failed.')
            raise e
        logger.debug('Connection established.')

        # Columns of interest
        self.col_time = ['timestamp']
        self.col_flow = ['src_addr', 'src_port', 'dst_addr', 'dst_port', 'ip_protocol', 'packets', 'bytes']
        self.col_node = ['ipaddr']
        self.columns = self.col_time + self.col_flow + self.col_node

        self.response_columns = ['hits.hits._source.flow.' + _ for _ in self.col_flow] + \
                                ['hits.hits._source.node.' + _ for _ in self.col_node]
        self.response_filter = ['_scroll_id', 'hits.total.value', 'hits.hits._source.@timestamp'] + self.response_columns

    def query_unique(self, field):
        """
        Queries ElasticSearch for all unique entries in given field
        :param field: field following 'hits.hits._source' [examples: flow.ip_protocol, node.hostname]
        :return:
        """
        query = \
            {
                'aggs': {
                    'nodes': {
                        'terms': {
                            'field': field,
                        }
                    }
                }
            }

        logger.debug('Querying uniques for field %s' % field)
        response = self._search(query, filter_response=False)

        df_tmp = pd.DataFrame(columns=[field, 'count'])
        if response['timed_out']:
            logger.warning('Query timed out')
        else:
            logger.debug('%i flows processed in %.2f seconds' %
                         (response['hits']['total']['value'], response['took']/1000))
            df_tmp = df_tmp.from_dict(response['aggregations']['nodes']['buckets'])
        return df_tmp

    def query_time(self, start_time: datetime, window_size: timedelta):
        """
        Queries ElasticSearch server starting at start_time

        :param start_time: datetime to start search at
        :param window_size: lookup window size in timedelta
        :return: dataframe containing data in the time window if any
        """
        # Time parameters
        time_current = start_time
        time_change = window_size

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
        return self._query_data(query)

    def _query_data(self, query):
        """
        Queries ElasticSearch server with given query body, saves result to file if a path is given

        :param query: query body given to elastic search
        :return: results as a dataframe
        """
        df_lst = []
        df_tmp = pd.DataFrame(columns=self.columns)

        response = self._search(query)
        scroll_id = response['_scroll_id']
        n_flows = response['hits']['total']['value']
        if n_flows == 0:
            logger.debug('No entries found.\n%s' % response)
            return df_tmp

        lines_skipped = 0
        batches = int(np.ceil(n_flows/self.QUERY_SIZE))
        logger.debug('Processing %i flows.' % n_flows)
        for batch in range(batches-1):
            rows = []
            for hit in response['hits']['hits']:
                row = hit['_source'].get('flow', None)
                if not row:
                    lines_skipped += 1
                    continue
                row.update(hit['_source']['node'])
                row.update({'timestamp': hit['_source']['@timestamp']})
                rows.append(row)
            df_lst.append(df_tmp.from_dict(rows))
            response = self._scroll(scroll_id)

        logger.debug('Processed %i batches, skipped %i lines.' % (batches, lines_skipped))
        return pd.concat(df_lst, sort=False)

    def _search(self, query, filter_response=True):
        """
        Wrapper for ElasticSearch search function

        :param query: query body
        :param filter_response:
        :return:
        """
        response_filter = self.response_filter if filter_response else None
        return self.client.search(index=self.es_index,
                                  body=query,
                                  size=self.QUERY_SIZE,
                                  scroll='2m',
                                  filter_path=response_filter)

    def _scroll(self, scroll_id, filter_response=True):
        """
        Wrapper for ElasticSearch scroll function

        :param scroll_id:
        :param filter_response:
        :return:
        """
        response_filter = self.response_filter if filter_response else None
        return self.client.scroll(scroll_id=scroll_id,
                                  scroll='2m',
                                  filter_path=response_filter)


if __name__ == '__main__':
    import time
    from settings import *

    # Configuration parameters
    fp_log = 'elastic_query.log'  # Configure

    # Logging initialization
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

    t0 = time.time()
    eq = ElasticQuery(server, 'elastiflow-3.5.1-2019*', username, password)
    eq.query_unique('flow.ip_protocol')
    t1 = time.time() - t0

    print('Time Elapsed %.2f' % t1)


