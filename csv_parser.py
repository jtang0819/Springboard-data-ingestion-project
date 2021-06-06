import decimal
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType, DecimalType, StringType, DateType, StructField
from pyspark.sql import functions

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
        # parse textfile into groups
        if record[record_type_pos] == "T":
            event = [datetime.strptime(record[0], "%Y-%m-%d"),
                     datetime.strptime(record[1], "%Y-%m-%d %H:%M:%S.%f"), record[2], record[3],
                     datetime.strptime(record[4], "%Y-%m-%d %H:%M:%S.%f"), int(record[5]),
                     record[6], decimal.Decimal(record[7]), None, None, None, None, "T"]
            #event_type = [type(element) for element in event]
            #print(event_type)
            return event
        elif record[record_type_pos] == "Q":
            event = [datetime.strptime(record[0], "%Y-%m-%d"),
                     datetime.strptime(record[1], "%Y-%m-%d %H:%M:%S.%f"), record[2], record[3],
                     datetime.strptime(record[4], "%Y-%m-%d %H:%M:%S.%f"), int(record[5]), record[6],
                     None, decimal.Decimal(record[7]), int(record[8]),
                     decimal.Decimal(record[9]), int(record[10]), "Q"]
            #event_type = [type(element) for element in event]
            #print(event_type)
            return event
    finally:
        event = [None, None, None, None, None, None, None, None, None, None, None, None, "B"]
    return event


def apply_latest(line):
    return line.groupBy()


appName = 'Springboard data ingestion project'
spark = SparkSession.builder.master('local').appName(appName).getOrCreate()
raw = spark \
    .sparkContext \
    .textFile(
    "file:///C:/Users/jordan/Python-Projects/springboard-capstone2/Springboard-data-ingestion-project/2020-08-06_trade_data.txt")
# print(raw.top(10))
parsed = raw.map(lambda line: parse_csv(line))
# print(parsed.dtypes)
# print(parsed.head(10))
data = spark.createDataFrame(parsed, common_event)
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir/data.parquet")
trade_common = spark.read.parquet("output_dir/data.parquet/partition=T/*")
# print(trade_common.schema)
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'file_tm', 'trade_pr').show(10)
# trade_corrected = apply_latest(trade)
# trade_date = '2020-07-29'
# trade.write.parquet("output_dir/trade/trade_dt={}".format(trade_date))
#
#
# df = spark.read.parquet('data/')
