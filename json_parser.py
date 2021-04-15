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
    record_type = record['event_type']
    try:
        # [logic to parse records]
        if record_type == "T":
    # [Get the applicable field values from json]
        if:  # [some key fields empty]
        event = common_event(col1_val, col2_val, ..., "T", "")
        else:
        event = common_event(,, , ....,, , , , "B", line)
        return event
    elif record_type == "Q":
    # [Get the applicable field values from json]
        if:  # [some key fields empty]:
            event = common_event(col1_val, col2_val, …, "Q", "")
        else:
            event = common_event(,, , ....,, , , , "B", line)
        return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        return common_event(,, , ....,, , , , "B", line)

spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set("fs.azure.account.key.springboardstoragejt.blob.core.windows.net", config.access_token)
raw = spark.textFile("wasbs://blob-container@springboardstoragejt.blob.core.windows.net/data")
parsed = raw.map(lambda line: parse_json(line))
data = spark.createDataFrame(parsed)
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")