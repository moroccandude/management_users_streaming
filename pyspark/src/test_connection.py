from pyspark.sql import SparkSession

# Create Spark Session with MongoDB Integration
spark = SparkSession.builder \
    .appName("MongoDBConnectionTest") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .getOrCreate()

# MongoDB Connection URI
mongo_uri = "mongodb://user:admin@127.0.0.1:27017/admin?authSource=admin"
database="admin"
collection="users"
# Test Data
data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]

# Create a DataFrame
df = spark.createDataFrame(data)

# Write DataFrame to MongoDB
df.write \
    .format("mongodb") \
    .option("connection.uri", mongo_uri) \
    .option("database", "admin") \
    .option("collection", "users") \
    .mode("append") \
    .save()

print("Data successfully written to MongoDB!")

# Read Data from MongoDB
read_df = spark.read \
    .format("mongodb") \
    .option("connection.uri", mongo_uri) \
    .option("database", database) \
    .option("collection", collection)\
    .load()

# Show the data
print("Data read from MongoDB:")
read_df.show()

# Stop the Spark session
spark.stop()