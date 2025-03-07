from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count,expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging
import yaml
import uuid
import os
from typing import Dict, Any

def load_config(config_path: str) -> Dict[str, Any]:
    """Load application configuration from YAML file"""
    try:
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    except Exception as e:
        raise RuntimeError(f"Failed to load configuration: {str(e)}")

def setup_logging(config: Dict[str, Any]) -> None:
    """Set up logging based on configuration"""
    log_config = config.get("logging", {})
    logging.basicConfig(
        level=getattr(logging, log_config.get("level", "INFO")),
        format=log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        filename=log_config.get("file")
    )

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Initialize and configure the Spark session based on configuration"""
    logger = logging.getLogger(__name__)
    logger.info("Initializing Spark session")
    
    # Extract configuration
    spark_config = config.get("spark", {})
    app_name = spark_config.get("app_name", "KafkaUserStreamProcessor")
    packages = ",".join(spark_config.get("packages", []))
    
    # Create builder with packages
    builder = SparkSession.builder.appName(app_name)
    
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    
    # Add all other spark configurations
    for key, value in spark_config.get("conf", {}).items():
        builder = builder.config(key, value)
    
    # Create session
    spark = builder.getOrCreate()
    
    # Configure Cassandra connection
    cassandra_config = config.get("cassandra", {})
    spark.conf.set("spark.cassandra.connection.host", cassandra_config.get("host", "127.0.0.1"))
    spark.conf.set("spark.cassandra.connection.port", cassandra_config.get("port", "9042"))
    
    return spark

def define_schema():
    """Define the schema for the incoming user data"""
    return StructType([
        StructField("gender", StringType(), True),
        StructField("name", StructType([
            StructField("title", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True)
        ]), True),
        StructField("location", StructType([
            StructField("street", StructType([
                StructField("number", IntegerType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("city", StringType(), True),
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
        ]), True),
        StructField("coordinates", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True)
    ])

def read_from_kafka(spark, config):
    """Read streaming data from Kafka based on configuration"""
    logger = logging.getLogger(__name__)
    
    kafka_config = config.get("kafka", {})
    bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")
    topic = kafka_config.get("topic", "users")
    starting_offsets = kafka_config.get("starting_offsets", "earliest")
    
    logger.info(f"Reading data from Kafka topic: {topic}")
    
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .load()

def parse_kafka_messages(messages, schema):
    """Parse Kafka messages according to the provided schema"""
    logger = logging.getLogger(__name__)
    logger.info("Parsing Kafka messages")
    
    return messages \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("userData")) \
        .select("userData.*")

def aggregate_user_data(parsed_df):
    """Aggregate user data by country: count users and compute average age"""
    logger = logging.getLogger(__name__)
    logger.info("Aggregating user data by country")
    
    return parsed_df \
        .groupBy("location.country") \
        .agg(
            count("*").alias("user_count"),
            avg("dob.age").alias("avg_age")
        ) \
        .orderBy(col("user_count").desc())

def write_to_mongodb(df, config, collection_key, checkpoint_key, output_mode):
    """Write data to MongoDB based on configuration"""
    logger = logging.getLogger(__name__)
    
    mongodb_config = config.get("mongodb", {})
    checkpoints = config.get("checkpoints", {})
    
    # Get the specific collection for this write operation
    collections = mongodb_config.get("collections", {})
    collection = collections.get(collection_key, collection_key)
    
    # Get the checkpoint path
    checkpoint_paths = checkpoints.get("paths", {})
    checkpoint_path = checkpoint_paths.get(checkpoint_key, f"/tmp/spark_checkpoint_{checkpoint_key}")
    
    # Ensure checkpoint directory exists
    os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
    
    logger.info(f"Writing data to MongoDB collection: {collection}")
    
    return df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", checkpoint_path) \
        .option("uri", mongodb_config.get("uri")) \
        .option("database", mongodb_config.get("database")) \
        .option("collection", collection) \
        .outputMode(output_mode) \
        .start()

def write_to_cassandra(df, config, checkpoint_key, output_mode):
    """Write data to Cassandra based on configuration"""
    logger = logging.getLogger(__name__)
    
    cassandra_config = config.get("cassandra", {})
    checkpoints = config.get("checkpoints", {})
    
    # Get the checkpoint path
    checkpoint_paths = checkpoints.get("paths", {})
    checkpoint_path = checkpoint_paths.get(checkpoint_key, f"/tmp/spark_checkpoint_{checkpoint_key}")
    
    # Ensure checkpoint directory exists
    os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
    
    table = cassandra_config.get("tableAgg","cassandra_aggregated")
    keyspace = cassandra_config.get("keyspace")
    
    logger.info(f"Writing data to Cassandra table: {keyspace}.{table}")
    
    return df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .outputMode(output_mode) \
        .option("confirm.truncate", "true") \
        .option("checkpointLocation", checkpoint_path) \
        .start()
def write_detailed_data_to_cassandra(df, config, checkpoint_key):
    """Write detailed user data to Cassandra"""
    logger = logging.getLogger(__name__)
    
    cassandra_config = config.get("cassandra", {})
    checkpoints = config.get("checkpoints", {})
    
    # Get the checkpoint path
    checkpoint_paths = checkpoints.get("paths", {})
    checkpoint_path = checkpoint_paths.get(checkpoint_key, f"/tmp/spark_checkpoint_{checkpoint_key}")
    
    # Ensure checkpoint directory exists
    os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
    
    table = cassandra_config.get("tables", {}).get("detail", "user_detail")
    keyspace = cassandra_config.get("keyspace", "random_user_keyspace")
    
    logger.info(f"Writing detailed user data to Cassandra table: {keyspace}.{table}")
    
    # Flatten the nested structure to match the Cassandra table schema
    flattened_df = df.select(
        col("gender"),
        col("name.title").alias("name_title"),
        col("name.first").alias("name_first"),
        col("name.last").alias("name_last"),
        col("location.street.number").alias("location_street_number"),
        col("location.street.name").alias("location_street_name"),
        col("location.city").alias("location_city"),
        col("location.country").alias("location_country"),
        col("dob.age").alias("dob_age"),
        col("email"),
        col("login.username").alias("login_username"),
        col("login.password").alias("login_password"),
        col("phone"),
        col("registered.date").alias("registered_date"),
        col("registered.age").alias("registered_age"),
        col("coordinates.latitude").alias("coordinates_latitude"),
        col("coordinates.longitude").alias("coordinates_longitude"),
        # Generate a UUID for the primary key
         expr("uuid()").alias("id")
    )
    
    return flattened_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start()

def define_aggregated_schema():
    """Define the schema for the aggregated user data"""
    return StructType([
        StructField("country", StringType(), True),
        StructField("user_count", IntegerType(), True),
        StructField("avg_age", DoubleType(), True)
    ])     
def main():
    """Main function to run the streaming application"""
    try:
        # Load configuration
        config = load_config("config.yml")
        
        # Set up logging
        setup_logging(config)
        logger = logging.getLogger(__name__)
        logger.info("Starting Kafka-to-MongoDB/Cassandra streaming application")
        
        # Initialize Spark
        spark = create_spark_session(config)
        user_schema = define_schema()
        
        # Create streaming dataframes
        kafka_messages = read_from_kafka(spark, config)
        parsed_df = parse_kafka_messages(kafka_messages, user_schema)
        aggregated_df = aggregate_user_data(parsed_df)
        
        
        
        # Write aggregated data to Cassandra
        agg_cassandra_query = write_to_cassandra(
            aggregated_df,
            config,
            "cassandra_aggregated",
            "complete"
        )
        logger.info("Aggregated data streaming to Cassandra started")
        
        # Wait for any query to terminate
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Streaming application failed: {str(e)}", exc_info=True)
    finally:
        if 'spark' in locals():
            spark.stop()
            logging.getLogger(__name__).info("Spark session stopped")

if __name__ == "__main__":
    main()