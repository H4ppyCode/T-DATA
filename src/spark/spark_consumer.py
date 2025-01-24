from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json, col, lag, struct, to_json, coalesce, lit
from pyspark.sql.types import StructType, StructField, FloatType
from prometheus_client import start_http_server, Gauge
import time

# Initialize a list to store the values
stored_btc_value = ""
btc_value = ""
stored_eth_value = ""
eth_value = ""


# Define schema for incoming JSON data
schema = StructType([
    StructField("bitcoin", StructType([
        StructField("usd", FloatType(), True)
    ]), True),
    StructField("ethereum", StructType([
        StructField("usd", FloatType(), True)
    ]), True)
])

# Initialize Spark session with Kafka integration
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Initialize Prometheus metrics
total_usd_gauge = Gauge('total_usd', 'Total value in USD of Bitcoin and Ethereum')

# Start Prometheus HTTP server
start_http_server(8001)

# Read streaming data from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Parse the 'value' column from Kafka into structured JSON data
parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").cast("timestamp")
)

# Process each micro-batch of data
def process_batch(batch_df, epoch_id):
    """
    Process a single micro-batch of data:
    - Extract relevant fields
    - Compute the total value in USD
    - Convert the result to JSON format and write back to Kafka
    - Update Prometheus metrics
    """
    # Extract and calculate total USD value with null-handling
    batch_df.printSchema()

    enriched_df = batch_df.select(
        col("timestamp"),
        col("data.bitcoin.usd").alias("bitcoin_usd"),
        col("data.ethereum.usd").alias("ethereum_usd")
    ).withColumn(
        "total_usd",
        coalesce(col("bitcoin_usd"), lit(0)) + coalesce(col("ethereum_usd"), lit(0))
    )

    window_spec = Window.orderBy("timestamp")
    enriched_df = enriched_df.withColumn("btc_diff", col("bitcoin_usd") - lag("bitcoin_usd", 1).over(window_spec)) \
        .withColumn("btc_performance", col("bitcoin_usd") / lag("bitcoin_usd", 1).over(window_spec) - 1)\
        .withColumn("btc_growth", (col("bitcoin_usd") / lag("bitcoin_usd", 1).over(window_spec)) ** (1 / 24) - 1) \
        .withColumn("eth_diff", col("ethereum_usd") - lag("ethereum_usd", 1).over(window_spec)) \
        .withColumn("eth_performance", col("ethereum_usd") / lag("ethereum_usd", 1).over(window_spec) - 1)\
        .withColumn("eth_growth", (col("ethereum_usd") / lag("ethereum_usd", 1).over(window_spec)) ** (1 / 24) - 1)
    
    enriched_df = enriched_df.select(to_json(struct(
            col("btc_diff"),
            col("btc_performance"),
            col("btc_growth"),
            col("eth_diff"),
            col("eth_performance"),
            col("eth_growth")
        )).alias("value")
    ).filter(col("value") != '{}' )

    # Print results to the console for debugging
    enriched_df.show(truncate=False)

    # Update Prometheus metric
    total_usd_value = enriched_df.agg({"total_usd": "sum"}).collect()[0][0]
    if total_usd_value is not None:
        total_usd_gauge.set(total_usd_value)
    else:
        total_usd_gauge.set(0)

    enriched_df.write\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "processed_crypto_prices") \
        .mode("append") \
        .save()

# Define the streaming query to process each batch
query = parsed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
