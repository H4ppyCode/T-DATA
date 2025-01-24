from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json, coalesce, lit
from pyspark.sql.types import StructType, StructField, FloatType
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
    from_json(col("value").cast("string"), schema).alias("data")
)

# Process each micro-batch of data
def process_batch(batch_df, epoch_id):
    global btc_value
    global stored_btc_value
    global eth_value
    global stored_eth_value

    """
    Process a single micro-batch of data:
    - Extract relevant fields
    - Compute the total value in USD
    - Convert the result to JSON format and write back to Kafka
    - Update Prometheus metrics
    """
    # Extract and calculate total USD value with null-handling


    enriched_df = batch_df.select(
        col("data.bitcoin.usd").alias("bitcoin_usd"),
        col("data.ethereum.usd").alias("ethereum_usd")
    ).withColumn(
        "total_usd",
        coalesce(col("bitcoin_usd"), lit(0)) + coalesce(col("ethereum_usd"), lit(0))
    )

    
    btc_df = batch_df.select("data.bitcoin.usd").withColumnRenamed("usd", "btc_value")
    btc_values = []
    btc_values = [row["btc_value"] for row in btc_df.collect()]
    print("BTC values: ", btc_values)
    print("BTC stored value: ", stored_btc_value)

    #la valeur actuelle du BTC
    if len(btc_values) > 0:
        btc_value = btc_values[0]

    #la différence entre les valeurs actuelles et précédentes
    if stored_btc_value != "":
        btc_diff = btc_value - stored_btc_value
        print("BTC difference: ", btc_diff)
    
    #la performance du BTC
    if stored_btc_value != "":
        btc_performance = btc_value / stored_btc_value - 1
        print("BTC performance: ", btc_performance)

    #la perte absolue du BTC
    if stored_btc_value != "":
        btc_loss = stored_btc_value - btc_value
        print("BTC loss: ", btc_loss)
    
    #la croissance extrapolee du BTC
    if stored_btc_value != "":
        btc_growth = (btc_value / stored_btc_value) ** (1 / 24) - 1
        print("BTC growth: ", btc_growth)

    stored_btc_value = btc_value

    eth_df = batch_df.select("data.ethereum.usd").withColumnRenamed("usd", "eth_value")
    eth_values = []
    eth_values = [row["eth_value"] for row in eth_df.collect()]
    print("ETH values: ", eth_values)
    print("ETH stored value: ", stored_eth_value)

    if len(eth_values) > 0:
        eth_value = eth_values[0]

    #la différence entre les valeurs actuelles et précédentes
    if stored_eth_value != "":
        eth_diff = eth_value - stored_eth_value
        print("ETH difference: ", eth_diff)

    #la performance de l'ETH
    if stored_eth_value != "":
        eth_performance = eth_value / stored_eth_value - 1
        print("ETH performance: ", eth_performance)
    
    #la perte absolue de l'ETH
    if stored_eth_value != "":
        eth_loss = stored_eth_value - eth_value
        print("ETH loss: ", eth_loss)

    #la croissance extrapolee de l'ETH
    if stored_eth_value != "":
        eth_growth = (eth_value / stored_eth_value) ** (1 / 24) - 1
        print("ETH growth: ", eth_growth)
    
    stored_eth_value = eth_value

    # Print results to the console
    enriched_df.show(truncate=False)

    # Update Prometheus metric
    total_usd_value = enriched_df.agg({"total_usd": "sum"}).collect()[0][0]
    if total_usd_value is not None:
        total_usd_gauge.set(total_usd_value)
    else:
        total_usd_gauge.set(0)

    # Convert the result to JSON format for Kafka
    kafka_ready_df = enriched_df.withColumn(
        "value",
        to_json(struct("total_usd"))
    ).select("value")

    kafka_ready_etc = enriched_df.withColumn(
        "value",
        to_json(struct("ethereum_usd"))
    ).select("value")

    kafka_ready_btc = enriched_df.withColumn(
        "value",
        to_json(struct("bitcoin_usd"))
    ).select("value")

    kafka_ready_btcAvg = enriched_df.withColumn(
        "value",
        to_json(struct("btc_diff"))
    ).select("value")

    kafka_ready_btcPerf = enriched_df.withColumn(
        "value",
        to_json(struct("btc_performance"))
    ).select("value")

    kafka_ready_btcLoss = enriched_df.withColumn(
        "value",
        to_json(struct("btc_loss"))
    ).select("value")

    kafka_ready_btcGrowth = enriched_df.withColumn(
        "value",
        to_json(struct("btc_growth"))
    ).select("value")

    kafka_ready_ethAvg = enriched_df.withColumn(
        "value",
        to_json(struct("eth_diff"))
    ).select("value")

    kafka_ready_ethPerf = enriched_df.withColumn(
        "value",
        to_json(struct("eth_performance"))
    ).select("value")

    kafka_ready_ethLoss = enriched_df.withColumn(
        "value",
        to_json(struct("eth_loss"))
    ).select("value")

    kafka_ready_ethGrowth = enriched_df.withColumn(
        "value",
        to_json(struct("eth_growth"))
    ).select("value")

    # Write enriched data to a new Kafka topic
    kafka_ready_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_total") \
        .save()
    
    kafka_ready_etc.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_ethereum") \
        .save()
    
    kafka_ready_btc.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_bitcoin") \
        .save()
    
    kafka_ready_btcAvg.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_bitcoin_diff") \
        .save()
    
    kafka_ready_btcPerf.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_bitcoin_performance") \
        .save()
    
    kafka_ready_btcLoss.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_bitcoin_loss") \
        .save()
    
    kafka_ready_btcGrowth.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_bitcoin_growth") \
        .save()
    
    kafka_ready_ethAvg.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_ethereum_diff") \
        .save()
    
    kafka_ready_ethPerf.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_ethereum_performance") \
        .save()
    
    kafka_ready_ethLoss.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_ethereum_loss") \
        .save()
    
    kafka_ready_ethGrowth.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_ethereum_growth") \
        .save()


# Define the streaming query to process each batch
query = parsed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
