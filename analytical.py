from pyspark.sql import SparkSession
import datetime

appName = 'Springboard data ingestion project'
spark = SparkSession.builder.master('local').appName(appName).getOrCreate()
#STEP FOUR
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
FROM tmp_trade_moving_avg) a
""")
last_pr_df.write.saveAsTable("temp_last_trade")
quote_union = spark.sql(
    """
    create view quote_union AS
    
    """
)

