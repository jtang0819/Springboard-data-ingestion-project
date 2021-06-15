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


# STEP TWO
appName = 'Springboard data ingestion project'
spark = SparkSession.builder.master('local').appName(appName).getOrCreate()
raw = spark \
    .sparkContext \
    .textFile(
    "file:///C:/Users/jordan/Python-Projects/springboard-capstone2/Springboard-data-ingestion-project/2020-08-06_trade_data.txt")
parsed = raw.map(lambda line: parse_csv(line))
data = spark.createDataFrame(parsed, common_event)
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir/data.parquet")
# STEP THREE
trade_common = spark.read.parquet("output_dir/data.parquet/partition=T/*")
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'trade_pr')
trade_corrected = apply_latest(trade)
trade_date = '2020-07-29'
trade.write.parquet("output_dir/trade/trade_dt={}".format(trade_date))
df = spark.read.parquet(('output_dir/trade/trade_dt={}'.format('2020-07-29')))
df = spark.sql("select symbol, event_tm, event_seq_nb, trade_pr from trades where trade_dt = '2020-07-20'")
mov_avg_df = spark.sql(
    """
    select symbol, exchange, event_tm, event_seq_nb, trade_pr, 
    avg(trade_pr) over(partition by symbol order by event_tm) as mov_avg_pr
    from tmp_trade_moving_avg
    """
)
mov_avg_df.write.saveAsTable("temp_trade_moving_avg")

date = datetime.datetime.strptime('2020-07-29', '%Y-%m-%d')
prev_date_str = datetime.datetime.today() - datetime.timedelta(days=1)
df = spark.sql("select symbol, event_tm, event_seq_nb, trade_pr from trades where trade_dt = '{}'".format(prev_date_str))
df.createOrReplaceTempView("tmp_last_trade")
last_pr_df = spark.sql(
    """select symbol, exchange, last_pr from (select
symbol, exchange, event_tm, event_seq_nb, trade_pr,
avg(trade_pr) over(partition by symbol order by event_tm range between interval '30' MINUTES PRECEDING AND CURRENT ROW) 
as last_pr
FROM tmp_trade_moving_avg) a
""")
last_pr_df.write.saveAsTable("temp_last_trade")
quote_union = spark.sql(
    """
    SELECT a.trade_dt, b.rec_type, a.symbol, a.event_tm, b.event_seq_nb,
    a.exchange, b.bid_pr, b.bid_size, b.ask_size, b.trade_pr, b.mov_avg_pr
    from
    quotes a
    join temp_trade_moving_avg b on a.arrival_tm = b.event_tm 
    """
)
quote_union.createOrReplaceTempView("quote_union")
quote_union_update = spark.sql(
    """
    select *,
    max(trade_pr) as last_trade_pr,
    max(mov_avg_pr) as last_move_avg_pr
    from quote_union
    """
)
quote_union_update.createOrReplaceTempView("quote_union_update")
quote_update = spark.sql(
    """
    select trade_dt, symbol, event_tm, event_seq_nb, exchange,
    bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
    from quote_union_update
    where rec_type = 'Q'
    """
)
quote_update.createOrReplaceTempView("quote_update")
quote_final = spark.sql(
    """
    select a.trade_dt, a.symbol, a.event_tm, a.event_seq_nb, a.exchange,
    a.bid_pr, a.bid_size, a.ask_pr, a.ask_size, a.last_trade_pr, a.last_mov_avg_pr,
    a.bid_pr - b.close_pr as bid_pr_mv, a.ask_pr - b.close_pr as ask_pr_mv
    from 
    quote_update a left outer join tmp_last_trade b on 
    # [Broadcast temp_last_trade table. Use quote_update to left outer join
    temp_last_trade]
    """
)
quote_final.write.parquet('/quote-trade-analytical/date={}'.format(trade_date))


def run_reporter_etl(my_config):
    trade_date = my_config.get('production', 'processing_date')
    reporter = Reporter(spark, my_config)
    tracker = Tracker('analytical_etl', my_config)
    try:
        reporter.report(spark, trade_date, eod_dir)
        tracker.update_job_status("success")
    except Exception as e:
        print(e)
        tracker.update_job_status("failed")
    return

#
#
# df = spark.read.parquet('data/')
