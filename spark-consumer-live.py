from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

kafka_topic_name = "pkttest"
kafka_bootstrap_servers = "localhost:9092"

spark = SparkSession \
        .builder \
        .appName("Structured Streaming Pkt") \
        .master("local[*]") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
pkt_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .load()


pkt_df1 = pkt_df.selectExpr("CAST(value AS STRING)", "timestamp")

pkt_schema_string = "Count INT, src_mac STRING, dst_mac STRING" 

pkt_df2 = pkt_df1 \
        .select(from_csv(col("value"), pkt_schema_string) \
                .alias("pkt"), "timestamp")

pkt_df3 = pkt_df2.select("pkt.*", "timestamp")

query = pkt_df3 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .format("console") \
        .start()

query.awaitTermination()
