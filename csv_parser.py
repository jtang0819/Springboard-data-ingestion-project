from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,TimestampType,DecimalType,StringType,DateType,StructField
from typing import List

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
            event = common_event(col1_val, col2_val, ..., "T","")
            #ask mento what is going on here??
            return event
        elif record[record_type_pos] == "Q":
            event = common_event(col1_val, col2_val, … , "Q","")
            return event
    except Exception as e:
    # [save record to dummy event in bad partition]
    # [fill in the fields as None or empty string]
    return common_event(,,,....,,,,,"B", line)



spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set("fs.azure.account.key.springboardstoragejt.blob.core.windows.net", "Jr02WEmRh7kNBzrNzzyyYbZYWPEdPCP883k/bAvQvagHtK1tgCqv8czhMDTT4ATwFPSo+tJiu2TVoRql5XJH8A==")
raw = spark.textFile("wasbs://blob-container@springboardstoragejt.blob.core.windows.net/data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
parsed = raw.map(lambda line: parse_csv(line))
data = spark.createDataFrame(parsed)
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

