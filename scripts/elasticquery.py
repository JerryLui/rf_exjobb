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
import os


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
        self.disk_path = '/media/jerry/RecordedFuture/Data'

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

        if response['timed_out']:
            logger.warning('Query timed out')
            return pd.DataFrame()
        logger.debug('%i flows processed in %.2f seconds' % (response['hits']['total']['value'], response['took']/1000))
        return pd.DataFrame().from_dict(response['aggregations']['nodes']['buckets'])

    def get_first_last(self):
        """
        :return: (date_last, date_first, total_hits)
        """
        dates = []
        for order in ['desc', 'asc']:
            query = \
                {
                    "query": {
                        "match_all": {}
                    },
                    "sort": [
                        {
                            "@timestamp": {
                                "order": order
                            }
                        }
                    ]
                }
            response = self._search(query, filter_response=False, size=1)
            dates.append(datetime.strptime(response['hits']['hits'][0]['_source']['@timestamp'],
                                           '%Y-%m-%dT%H:%M:%S.%fZ'))
            total_hits = response['hits']['total']['value']
            # (dates[0] - dates[1]).total_seconds()/(60*60*24)
        return dates[0], dates[1], total_hits

    def query_time(self, start_time: datetime, window_size: timedelta, from_disk: bool = True):
        """
        Queries ElasticSearch server starting at start_time

        :param start_time: datetime to start search at
        :param window_size: lookup window size in timedelta
        :param from_disk: check if file exists on disk and load it
        :return: dataframe containing data in the time window if any
        """
        # Time parameters
        time_current = start_time
        time_change = window_size

        logger.debug('Querying time %s' % time_current.isoformat())
        if from_disk and os.path.exists(self._get_pp(time_current)):
            return self.load_pickle(current_date, time_change)

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

        return self._query_data(query)

    def query_ip(self, ip, start_time: datetime, end_time: datetime, src=True):
        """
        Queries ElasticSearch server for src/dst ip/cidr in given time range

        :param ip: ip, cidr notation acceptable [ex. 192.168.1.1/16]
        :param start_time: start time in range
        :param end_time: end time in range
        :param src: lookup in src_addr/dst_addr
        :return: dataframe with results
        """
        time_start = start_time
        time_end = end_time

        flow_feature = 'flow.src_addr' if src else 'flow.dst_addr'

        query = \
            {'query':
                 {'bool':
                      {'filter':
                           [{'term':
                                {flow_feature: ip}
                           },
                               {'range':
                                    {'@timestamp':
                                         {'gte': time_start.isoformat(),
                                          'lt': time_end.isoformat()}
                                     }
                                }
                           ]
                      }
                  }
             }

        logger.debug('Querying ip %s in time %s' % (ip, time_start.isoformat()))
        return self._query_data(query)

    def load_pickle(self, start_time: datetime, window_size: timedelta):
        """
        Load saved pickle files from disk instead of query.
        """
        windows = int(window_size.total_seconds()/(60 * 5))     # Number of windows
        file_window = timedelta(minutes=5)

        logger.debug('Loading time %s ' % start_time.isoformat())
        df_lst = []
        for _ in range(windows):
            pp = self._get_pp(start_time)
            df_lst.append(pd.read_pickle(pp))
            start_time += file_window
        return pd.concat(df_lst, sort=False, ignore_index=True)

    def _get_pp(self, current_date):
        """
        :return: pickle file path for given date
        """
        pickle_path = os.path.join(self.disk_path,
                                   str(current_date.month),
                                   str(current_date.day),
                                   '%02d%02d.pickle' % (current_date.hour, current_date.minute))
        return pickle_path

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
            logger.warning('Entries not found.\n')
            return df_tmp

        lines_skipped = 0
        batches = int(np.ceil(n_flows/self.QUERY_SIZE))
        logger.debug('Processing %i flows.' % n_flows)
        for batch in range(max(batches-1, 1)):
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
        self.client.clear_scroll(scroll_id)     # Clear Scroll after Finish

        logger.debug('Processed %i batches, skipped %i lines.' % (batches, lines_skipped))
        return pd.concat(df_lst, sort=False, ignore_index=True)

    def _search(self, query, filter_response=True, size=None):
        """
        Wrapper for ElasticSearch search function

        :param query: query body
        :param filter_response:
        :return:
        """
        response_filter = self.response_filter if filter_response else None
        size = self.QUERY_SIZE if not size else size
        return self.client.search(index=self.es_index,
                                  body=query,
                                  size=size,
                                  scroll='1m',
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
                                  scroll='1m',
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

    eq = ElasticQuery(server, 'elastiflow-3.5.1-2019*', username, password)
    # df1 = eq.query_ip('5.254.66.131', datetime(2019, 12, 6, 2, 30), datetime(2019, 12, 6, 3, 0))
    # df2 = eq.query_ip('5.254.66.131', datetime(2019, 12, 6, 2, 30), datetime(2019, 12, 6, 3, 0), src=False)
    # df = eq.query_time(datetime(2019, 12, 2, 0), timedelta(minutes=120))

    disk_path = '/media/jerry/RecordedFuture/Data/'
    current_date = datetime(2019, 12, 14, 22, 10)
    window = timedelta(minutes=5)
    while current_date < datetime(2019, 12, 15, 0, 0):
        print(current_date)
        df = eq.query_time(current_date, window, from_disk=False)

        pickle_path = os.path.join(disk_path,
                                   str(current_date.month),
                                   str(current_date.day),
                                   '%02d%02d.pickle' % (current_date.hour, current_date.minute))
        if not os.path.exists(os.path.dirname(pickle_path)):
            os.makedirs(os.path.dirname(pickle_path))
        df.to_pickle(pickle_path)
        current_date += window





