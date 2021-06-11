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