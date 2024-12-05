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

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Convert the value column to string
df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Function to print each batch of data
def print_batch(batch_df, epoch_id):

    # manipulate the data
    batch_df = batch_df.withColumn("total_usd", col("data.bitcoin.usd") + col("data.ethereum.usd"))

    batch_df.select(
        col("data.bitcoin.usd").alias("bitcoin_usd"),
        col("data.ethereum.usd").alias("ethereum_usd"),
        col("total_usd")
    ).show(truncate=False)

# Write the data to the console and print each batch
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(print_batch) \
    .start()

query.awaitTermination()