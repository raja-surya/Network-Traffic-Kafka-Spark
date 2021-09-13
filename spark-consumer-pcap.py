from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

kafka_topic_name = "pkttest_pcap"
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

pkt_schema_string = "count INT,src_ip STRING,dst_ip STRING, \
                     proto INT, sport STRING, dport STRING" 

pkt_df2 = pkt_df1 \
        .select(from_csv(col("value"), pkt_schema_string) \
                .alias("pkt"), "timestamp")

pkt_df3 = pkt_df2.select("pkt.*", "timestamp")
#pkt_df3 = pkt_df2.select("pkt.*")

#summary = pkt_df3 \
#         .groupBy("proto") \
#         .count()

#query = summary \
query = pkt_df3 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .format("console") \
        .start()

### If you want to try storing the data frames in files.
#query = pkt_df3 \
#        .writeStream \
#        .trigger(processingTime='5 seconds') \
#        .outputMode("append") \
#        .format("csv") \
#        .option("checkpointLocation", "/home/raja/kafka/pkt-example") \
#        .option("path", "/home/raja/kafka/pkt-example") \
#        .start()


query.awaitTermination()


