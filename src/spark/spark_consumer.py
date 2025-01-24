from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import logging

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkKafkaConsumer")

# Schéma Avro pour les données consommées dans Kafka
schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("price_24h_ago", DoubleType(), True)
])

try:
    logger.info("Initialisation de la session Spark...")

    # Initialiser une session Spark avec des configurations optimisées
    spark = SparkSession.builder \
        .appName("Spark Kafka Consumer") \
        .config("spark.network.timeout", 800) \
        .config("spark.rpc.message.maxSize", 240) \
        .config("spark.network.maxRemoteBlockSizeFetchToMem", "240m") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.shuffle.partitions", 50) \
        .config("spark.reducer.maxSizeInFlight", "48m") \
        .config("spark.shuffle.file.buffer", "32k") \
        .getOrCreate()

    logger.info("Session Spark initialisée avec succès.")

    # Lire les données depuis Kafka
    logger.info("Lecture du topic Kafka 'crypto_prices'...")
    # 10MB pour les messages Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "crypto_prices") \
        .option("startingOffsets", "latest") \
        .option("fetch.message.max.bytes", 10485760) \
        .load()

    logger.info("Données Kafka chargées avec succès.")
    df.printSchema()  # Debugging : affichage du schéma des données brutes.

    # Désérialiser les messages Kafka (payload JSON)
    logger.info("Désérialisation des messages Kafka...")
    crypto_data = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select(
        col("data.timestamp").alias("timestamp"),
        col("data.symbol").alias("symbol"),
        col("data.price").alias("price")
    )

    logger.info("Schéma des données désérialisées :")
    crypto_data.printSchema()  # Debugging : affichage du schéma après désérialisation.

    # Filtrer uniquement BTC et ETH
    logger.info("Filtrage des données pour Bitcoin et Ethereum...")
    filtered_data = crypto_data.filter(col("symbol").isin("bitcoin", "ethereum"))

    logger.info("Vérification des données filtrées...")
    # Vous pouvez désactiver cette ligne pour les grandes données car elle déclenche une action
    # logger.info(f"Nombre de lignes après filtre : {filtered_data.count()}")

    # Calculer la moyenne des prix entre BTC et ETH
    logger.info("Calcul de la moyenne des prix entre BTC et ETH...")
    # Répartition pour améliorer la distribution
    average_price = filtered_data.repartition(4, col("timestamp")) \
        .groupBy("timestamp") \
        .agg(
        avg(col("price")).alias("average_price")
    )

    logger.info("Schéma après calcul des moyennes :")
    average_price.printSchema()  # Debugging

    # Préparer les données pour l'envoi à Kafka
    logger.info("Préparation des données pour l'envoi au topic Kafka 'crypto_prices_average'...")
    average_price_json = average_price.select(
        to_json(
            struct(
                col("timestamp"),
                col("average_price")
            )
        ).alias("value")
    )

    logger.info("Données préparées avec succès.")

    # Écrire le résultat dans un nouveau topic Kafka
    logger.info("Écriture des données dans le topic Kafka 'crypto_prices_average'...")
    query = average_price_json.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "crypto_prices_average") \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .outputMode("complete") \
        .start()

    logger.info("Écriture dans Kafka démarrée avec succès.")

    # Attendre la fin du traitement
    query.awaitTermination()

except Exception as e:
    logger.error(f"Une erreur est survenue : {str(e)}", exc_info=True)
