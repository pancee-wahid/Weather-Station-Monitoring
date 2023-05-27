from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import connections
from elasticsearch_dsl import Search

import pandas as pd

loc = r'/home/elastic/s1/s1__2023-05-20__11-26.parquet'
data = pd.read_parquet(loc, engine='auto')

es_client = connections.create_connection(hosts=["http://localhost:9200/"])

def doc_generator(pandas_data):
    for index, document in pandas_data.iterrows():
        yield {
            "_index": 'status',
            "_source": document.to_dict(),
        }

helpers.bulk(es_client, doc_generator(data))