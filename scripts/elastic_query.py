"""
Download data from Insikt ElasticSearch

Notes: Commented columns include fields with comma which breaks csv file
"""
import sys
sys.path.append("/home/jerry/Dropbox/Kurser/Master Thesis/rf_exjobb/scripts")

from settings import username, password
from elasticsearch import Elasticsearch

import operator
import os

case = 'NK.csv'

disk_path = '/media/jerry/RecordedFuture/insikt_cases'
save_file = open(os.path.join(disk_path, case), 'w')
client = Elasticsearch('https://insikt-netflow.recfut.com', http_auth=(username, password), timeout=600)
query_size = 10000
query_body = \
{'query':
    {'match':
        {
            'input_filename': os.path.splitext(case)[0]
        }
    }
}

columns = ['@timestamp', 'dst_ip_addr', 'dst_port', 'dest_asn', 'dest_host', 'num_octets', 'num_pkts', 'proto',
           'sample_algo', 'sample_interval', 'source_asn', 'src_host', 'src_host_score', 'src_ip_addr', 'src_ip_score',
           'src_name', 'src_port', 'start_time', 'tcp_flags', 'input_filename']
f_columns = columns + ['src_cont', 'src_long', 'src_lat', 'dst_cont', 'dst_long', 'dst_lat']
save_file.write(','.join(f_columns) + ',\n')

get_items = operator.itemgetter(*columns)

# Download
response = client.search('main_netflow_index*', size=query_size, body=query_body, scroll='2m')
print('Getting %i data points...' % response['hits']['total'])
scroll_id = response['_scroll_id']

batch = 0
while True:
    print('Processing batch %i...' % batch)
    batch += 1
    for hit in response['hits']['hits']:
        row = list(get_items(hit['_source']))
        if hit['_source']['src_geoip']:
            row.append(hit['_source']['src_geoip']['continent_code'])
            row.append(hit['_source']['src_geoip']['longitude'])
            row.append(hit['_source']['src_geoip']['latitude'])
        else:
            row += ['', '', '']
        if hit['_source']['dst_geoip']:
            row.append(hit['_source']['dst_geoip']['continent_code'])
            row.append(hit['_source']['dst_geoip']['longitude'])
            row.append(hit['_source']['dst_geoip']['latitude'])
        else:
            row += ['', '', '']

        save_file.write(','.join([str(_) for _ in row]) + ',\n')

        # data.append(row)

    if len(response['hits']['hits']) < query_size:
        print('Download finished.')
        break

    # Get next set
    response = client.scroll(scroll_id=scroll_id, scroll='2m')

save_file.close()