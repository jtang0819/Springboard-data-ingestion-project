import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,TimestampType,DecimalType,StringType,DateType,StructField
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


def parse_json(line:str):
    record_type = line['event_type']
    record = json.load(line)
    try:
        # [logic to parse records]
        if record_type == "T":
            # [Get the applicable field values from json]
            event = record
            if record_type == "":  # [some key fields empty]
                record['event type'] = "T"
            else:
                return event
        elif record_type == "Q":
            event = record
            # [Get the applicable field values from json]
            if record_type == "":  # [some key fields empty]:
                record['event type'] = "Q"
            else:
                return event
    except None:
        event = record
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        record['event type'] = "B"
        return event


def apply_latest(line):
    return line.groupBy()


spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set("fs.azure.account.key.springboardstoragejt.blob.core.windows.net", config.access_token)
raw = spark.textFile("wasbs://blob-container@springboardstoragejt.blob.core.windows.net/data")
parsed = raw.map(lambda line: parse_json(line))
data = spark.createDataFrame(parsed)
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")
trade_common = spark.read.parquet("output_dir/partition=T")
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'file_tm', 'trade_pr')
trade_corrected = apply_latest(trade)
trade_date = '2020-07-29'
trade.write.parquet("data/json/2020-08-05/NYSE/trade/trade_dt={}".format(trade_date))
