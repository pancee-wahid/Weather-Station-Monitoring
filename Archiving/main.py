from kafka import KafkaConsumer
from json import loads
import pandas as pd
import fastparquet as fp
import datetime
import pyarrow as pa
import pyarrow.parquet as pq

parquetFilesPath = 'D:\Projects\Weather-Station-Monitoring\Archiving\parquet-files'


def write_to_parquet():
    for i, station_records in enumerate(temp_store):
        timestamp = loads(station_records[0])['status_timestamp']
        print(timestamp)
        formatted_time = datetime.date.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d__%H-%M')
        parquet_file = parquetFilesPath + "\\" +  str(i+1) + "\\" +  str(i+1)   + "__" + formatted_time +  '.parquet'
        station_df = pd.DataFrame.from_records(station_records)
        pqwriter = None
        table = pa.Table.from_pandas(station_df)
        # create a parquet write object giving it an output file
        pqwriter = pq.ParquetWriter(parquet_file, table.schema)            
        pqwriter.write_table(table)
        pqwriter.close()



if __name__ == "__main__":

    consumer = KafkaConsumer(
        'weather-messages',
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group')

    temp_store = [[],[], [], [], [], [], [], [], [], []]

    count = 0
    for msg in consumer:
        if msg is None or msg == '':
            continue
        if 'error' in msg:
            print('Error: {}'.format(msg['error']))
            continue
        
        record_json = msg.value.decode('utf-8')
        print(record_json)
        record = loads(record_json)
        temp_store[int(record['station_id']) - 1].append(record_json)
        count += 1
        if (count >= 1000):
            write_to_parquet()
            temp_store = [[],[], [], [], [], [], [], [], [], []]
            count = 0

