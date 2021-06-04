from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType, DecimalType, StringType, DateType, StructField
from typing import List
import config


common_event = StructType([
    StructField("trade_dt", DateType()),
    StructField("arrival_tm", TimestampType()),
    StructField("rec_type", StringType()),
    StructField("symbol", StringType()),
    StructField("event_tm", TimestampType()),
    StructField("event_seq_nb", IntegerType()),
    StructField("exchange", StringType()),
    StructField("trade_pr", DecimalType()),
    StructField("bid_pr", DecimalType()),
    StructField("bid_size", IntegerType()),
    StructField("ask_pr", DecimalType()),
    StructField("ask_size", IntegerType()),
    StructField("partition", StringType())
])

# 2020-08-06,2020-08-06 09:30:00.0,Q,SYMA,2020-08-06 18:08:58.256,69,NYSE,75.54449587635618,100,77.16118144376208,100
# 2020-08-06,2020-08-06 09:30:00.0,T,SYMA,2020-08-06 18:14:28.899,70,NYSE,78.23471404145394,125


def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(",")
    try:
    # [logic to parse records]
        if record[record_type_pos] == "T":
            event = [record[0], record[1], record[2], record[3],
                     record[4], record[5], record[6], record[7], "", "", "", "", "T"]
            return event
        elif record[record_type_pos] == "Q":
            event = [record[0], record[1], record[2], record[3], record[4],
                     record[5], record[6], "", record[7], record[8],
                     record[9], record[10], "Q"]
            return event
    finally:
        event = ["", "", "", "", "", "", "", "", "", "", "", "", "B"]
    return event


def apply_latest(line):
    return line.groupBy()


appName = 'Springboard data ingestion project'
spark = SparkSession.builder.master('local').appName(appName).getOrCreate()
raw = spark.sparkContext.textFile("file:///C:/Users/jordan/Python-Projects/springboard-capstone2/Springboard-data-ingestion-project/2020-08-06_trade_data.txt")
# print(raw.top(10))
parsed = raw.map(lambda line: parse_csv(line))
# print(parsed.top(10))
data = spark.createDataFrame(parsed, common_event)
# print(data.show(10))
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir/data.parquet")

# trade_common = spark.read.parquet("output_dir/partition=T")
# trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'file_tm', 'trade_pr')
# trade_corrected = apply_latest(trade)
# trade_date = '2020-07-29'
# trade.write.parquet("data/csv/2020-08-05/NYSE/trade/trade_dt={}".format(trade_date))
#
#
# df = spark.read.parquet('data/')
