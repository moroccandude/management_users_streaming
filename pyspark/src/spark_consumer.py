from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("consumer") \
    .master("spark://0.0.0.0:7077") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
    .option("subscribe", "users") \
    .option("startingOffsets", "earliest") \
    .load()

# Select key and value columns (Kafka messages are binary by default, so cast them to strings)
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Write the stream to the console for inspection
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Keep the stream running
query.awaitTermination()