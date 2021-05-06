from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,TimestampType,DecimalType,StringType,DateType,StructField
from typing import List
import config


common_event = StructType([
    StructField("col1_val", DateType()),
    StructField("col2_val", StringType()),
    StructField("col3_val", StringType()),
    StructField("col4_val", StringType()),
    StructField("col5_val", TimestampType()),
    StructField("col6_val", IntegerType()),
    StructField("col7_val", TimestampType()),
    StructField("col8_val", DecimalType()),
    StructField("col9_val", DecimalType()),
    StructField("col10_val", IntegerType()),
    StructField("col11_val", DecimalType()),
    StructField("col12_val", IntegerType()),
    StructField("col13_val", StringType())
])


def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    try:
    # [logic to parse records]
        if record[record_type_pos] == "T":
            event = [record[0], record[1], record[2], record[3],
                     record[4], record[5], record[6], record[7], record[8],
                     record[9], record[10]]
            return event
        elif record[record_type_pos] == "Q":
            event = [record[0], record[1], record[2], record[3], record[4],
                     record[5], record[6], record[7], record[8],
                     record[9], record[10]]
            return event
    except record[record_type_pos] == "":
        event = [record[0], record[1], record[2], record[3],
                 record[4], record[5], record[6], record[7], record[8],
                 "B", record[10]]
    return event


def apply_latest(line):
    return line.groupBy()


spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set("fs.azure.account.key.springboardstoragejt.blob.core.windows.net", config.access_token)
raw = spark.textFile("wasbs://blob-container@springboardstoragejt.blob.core.windows.net/data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
parsed = raw.map(lambda line: parse_csv(line))
data = spark.createDataFrame(parsed)
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

trade_common = spark.read.parquet("output_dir/partition=T")
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'file_tm', 'trade_pr')
trade_corrected = apply_latest(trade)
trade_date = '2020-07-29'
trade.write.parquet("data/csv/2020-08-05/NYSE/trade/trade_dt={}".format(trade_date))

