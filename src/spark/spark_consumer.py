from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, cast
from pyspark.sql.types import StructType, StructField, FloatType

# Define the schema for the incoming data
schema = StructType([
    StructField("bitcoin", StructType([
        StructField("usd", FloatType(), True)
    ]), True),
    StructField("ethereum", StructType([
        StructField("usd", FloatType(), True)
    ]), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092,kafka:9093,localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Convert the value column to string
df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Function to print each batch of data
def print_batch(batch_df, epoch_id):
    batch_df.select(
        col("data.bitcoin.usd").alias("bitcoin_usd"),
        col("data.ethereum.usd").alias("ethereum_usd")
    ).show(truncate=False)

# Write the data to the console and print each batch
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(print_batch) \
    .start()

query.awaitTermination()
