from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ✅ Create Spark Session with Kafka Integration
spark = SparkSession.builder \
    .appName("KafkaUserStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# ✅ Define Schema for JSON Messages
user_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("title", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("country", StringType(), True)
    ]), True),
    StructField("dob", StructType([
        StructField("age", IntegerType(), True)
    ]), True),
])

# ✅ Read Stream from Kafka Topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users") \
    .option("startingOffsets", "earliest") \
    .load()

# ✅ Parse JSON Messages
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), user_schema).alias("userData")) \
    .select("userData.*")

# ✅ Aggregate Data (Count Users per Country & Compute Avg Age)
aggregated_df = parsed_df \
    .groupBy("location.country") \
    .agg(
        count("*").alias("user_count"),
        avg("dob.age").alias("avg_age")
    ) \
    .orderBy(col("user_count").desc())

# ✅ Write Stream to Console
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()  # ✅ Removed the incorrect `\` here!

query.awaitTermination()
