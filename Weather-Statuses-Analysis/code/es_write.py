from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import connections
from elasticsearch_dsl import Search

import os
import pandas as pd

done = []
empty_list = []
for i in range(10):
    done.append(empty_list) 

folders_list = os.listdir('/home/elastic/parquet_files')
# folders_list = os.listdir('F:\\term-2\Data intensive\labs\Weather-Station-Monitoring\Archiving - Python\parquet-files')


while True:
    for i in range(10):
        folder_files = os.listdir("/home/elastic/parquet_files/" + folders_list[i])
        # folder_files = os.listdir("F:\\term-2\Data intensive\labs\Weather-Station-Monitoring\Archiving - Python\parquet-files\\" + folders_list[i])
        for file in folder_files:
            if not (file in done[i]):
                loc = "/home/elastic/parquet_files/" + folders_list[i] + "/" + file
                # loc = "F:\\term-2\Data intensive\labs\Weather-Station-Monitoring\Archiving - Python\parquet-files\\" + folders_list[i] + "\\" + file
                data = pd.read_parquet(loc, engine='auto')
                es_client = connections.create_connection(hosts=["http://localhost:9200/"])
                def doc_generator(pandas_data):
                    for index, document in pandas_data.iterrows():
                        yield {
                            "_index": 'status',
                            "_source": document.to_dict(),
                        }
                helpers.bulk(es_client, doc_generator(data))
                print(file)
                done[i].append(file)