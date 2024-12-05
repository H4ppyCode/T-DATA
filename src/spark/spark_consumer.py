from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import StructType, StructField, FloatType

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

# Read streaming data from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Parse the 'value' column from Kafka into structured JSON data
parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

# Process each micro-batch of data
def process_batch(batch_df, epoch_id):
    """
    Process a single micro-batch of data:
    - Extract relevant fields
    - Compute the total value in USD
    - Convert the result to JSON format and write back to Kafka
    """
    # Extract and calculate total USD value
    enriched_df = batch_df.select(
        col("data.bitcoin.usd").alias("bitcoin_usd"),
        col("data.ethereum.usd").alias("ethereum_usd")
    ).withColumn("total_usd", col("bitcoin_usd") + col("ethereum_usd"))

    # Print results to the console
    enriched_df.show(truncate=False)

    # Convert the result to JSON format for Kafka
    kafka_ready_df = enriched_df.withColumn(
        "value",
        to_json(struct("total_usd"))
    ).select("value")

    # Write enriched data to a new Kafka topic
    kafka_ready_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_total") \
        .save()

# Define the streaming query to process each batch
query = parsed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
