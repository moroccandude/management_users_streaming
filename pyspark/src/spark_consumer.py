from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType

# Create Spark Session with Kafka and MongoDB Integration
spark = SparkSession.builder \
    .appName("KafkaUserStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .getOrCreate()

# MongoDB connection details
mongo_uri = "mongodb://user:admin@127.0.0.1:27017/admin?authSource=admin"
database = "admin"
collection = "users"

# Define Schema for JSON Messages
user_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("title", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("street",StructField([
            StructField("number",IntegerType()),
            StructField("name",StringType())
        ])),
         StructField("city",StringType()),
        StructField("country", StringType(), True),

    ]), True),
    StructField("dob", StructType([
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("email", StringType(), True),
     StructField("login", StructType([
        StructField("username", StringType(), True),
       StructField("password", StringType(), True)
    ]), True),
     StructField("phone", IntegerType(), True),
      StructField("registered", StructType([
       StructField("date", StringType(), True),
       StructField("age", IntegerType(), True)
      ])),
       StructField("coordinates",StructType([
    StructField("latitude", DoubleType(), True),  # Latitude as a double
    StructField("longitude", DoubleType(), True)  # Longitude as a double
]),True)     


])

# Read Stream from Kafka Topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON Messages
parsed_df = kafka_df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), user_schema).alias("userData")) \
    .select("userData.*")

# Aggregate Data (Count Users per Country & Compute Avg Age)
aggregated_df = parsed_df \
    .groupBy("location.country") \
    .agg(
        count("*").alias("user_count"),
        avg("dob.age").alias("avg_age")
    ) \
    .orderBy(col("user_count").desc())

# Write Parsed Data to MongoDB (Streaming)
parsed_query = parsed_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_parsed") \
    .option("uri", mongo_uri) \
    .option("database", database) \
    .option("collection", collection) \
    .outputMode("append") \
    .start()

print("Parsed data streaming to MongoDB started!")

# Write Aggregated Data to MongoDB (Streaming)
aggregated_query = aggregated_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_aggregated") \
    .option("uri", mongo_uri) \
    .option("database", database) \
    .option("collection", "aggregated_data") \
    .outputMode("complete") \
    .start()

print("Aggregated data streaming to MongoDB started!")

# Await termination for both queries
parsed_query.awaitTermination()
aggregated_query.awaitTermination()