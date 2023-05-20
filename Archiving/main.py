import os
from datetime import datetime as dt
from json import loads

import fastparquet as fp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

parquetFilesPath = "D:\Projects\Weather-Station-Monitoring\Archiving\parquet-files\s"


def write_to_parquet():
    for i, station_records in enumerate(temp_store):
        station_df = pd.DataFrame.from_records(station_records)
        table = pa.Table.from_pandas(station_df)
        # station_df = station_df.sort_values('status_timestamp')
        # for rec in station_df:
        station_df['t_timestamp'] = pd.to_datetime(station_df['status_timestamp'], unit='ms')
        print(station_df)
        grouped = station_df.groupby(pd.Grouper(key='t_timestamp', freq='5min'))

        for name, group in grouped:
            if (group is None or len(group) == 0):
                continue
            print(group)
            group = group.drop('t_timestamp', axis=1)
            timestamp = group.iloc[0, 3]
            formatted_time = dt.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d__%H-%M")
            parquet_file = parquetFilesPath + str(i + 1) + "/s" + str(i + 1) + "__" + formatted_time + ".parquet"

            # create a parquet write object giving it an output file
            pqwriter = pq.ParquetWriter(parquet_file, table.schema)
            pqwriter.write_table(table)
            pqwriter.close()


if __name__ == "__main__":
    consumer = KafkaConsumer(
        "weather-messages",
        bootstrap_servers=["127.0.0.1:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="my-group",
    )

    temp_store = [[], [], [], [], [], [], [], [], [], []]

    count = 0
    for msg in consumer:
        if msg is None or msg == "":
            continue
        if "error" in msg:
            print("Error: {}".format(msg["error"]))
            continue

        record_json = msg.value.decode("utf-8")
        print(record_json)
        record = loads(record_json)
        # Flatten the nested "weather" dictionary
        for k, v in record["weather"].items():
            record[f"weather_{k}"] = v
        del record["weather"]
        temp_store[int(record["station_id"]) - 1].append(record)
        count += 1
        if count >= 10000:
            write_to_parquet()
            temp_store = [[], [], [], [], [], [], [], [], [], []]
            count = 0