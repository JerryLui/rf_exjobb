"""
Download data from Elastic Search server.
Requires: settings.py with server, username and password parameters

@Author Jerry Liu jerry.liu@recordedfuture.com
"""
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, exceptions
import pandas as pd
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

        response = self.client.search(index=self.es_index,
                                      body=query,
                                      size=self.QUERY_SIZE,
                                      scroll='2m',
                                      filter_path=self.response_filter)
        scroll_id = response['_scroll_id']
        n_flows = response['hits']['total']['value']
        if n_flows == 0:
            logger.debug('No entries found.\n%s' % response)
            return df_tmp

        # Process batches
        logger.debug('Processing %i flows.' % n_flows)

        response_batch = 1
        lines_skipped = 0
        while True:
            rows = []
            for i, hit in enumerate(response['hits']['hits']):
                row = hit['_source'].get('flow', None)
                if not row:
                    lines_skipped += 1
                    continue
                row.update(hit['_source']['node'])
                row.update({'timestamp': hit['_source']['@timestamp']})
                rows.append(row)
            df_lst.append(df_tmp.from_dict(rows))

            # Exit condition
            if len(response['hits']['hits']) < self.QUERY_SIZE:
                logger.debug('Processed %i batches, skipped %i lines.' % (response_batch, lines_skipped))
                break

            # Get next set
            response = self.client.scroll(scroll_id=scroll_id, scroll='2m', filter_path=self.response_filter)
            response_batch += 1

        return pd.concat(df_lst)


if __name__ == '__main__':
    import time
    # import sys  # Used for local imports

    # sys.path.append("/home/jliu/rf_exjobb/scripts/")  # Configure
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
    eq = ElasticQuery(server, index, username, password)
    df = eq.query_time(datetime(2019, 9, 2, 9, 0), timedelta(minutes=5))
    t1 = time.time() - t0

    print('Time Elapsed %.2f' % t1)


