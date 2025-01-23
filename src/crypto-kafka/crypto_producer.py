import json
import time
import random
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import start_http_server, Summary, Counter
from scrapper.provider.CoinGecko import CoinGecko

# Prometheus Metrics
UP_TIME = Summary('up_time_seconds', 'Time since the producer started')
SCRAP_TIME = Summary('scrap_time_seconds', 'Time spent processing request')
SEND_TIME = Summary('send_time_seconds', 'Time spent processing request')
MESSAGES_SENT = Counter('messages_sent_total', 'Total number of messages sent')
FALLBACK_MESSAGES = Counter('fallback_messages_total', 'Total number of messages sent via fallback serialization')
ERRORS_SENDING = Counter('errors_sending_total', 'Total number of errors while sending messages')

# Avro Schema for CryptoData
CRYPTO_VALUE_SCHEMA_STR = """
{
    "type": "record",
    "name": "CryptoDataAvro",
    "fields": [
        {"name": "timestamp", "type": "long"},
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "price_24h_ago", "type": "double"}
    ]
}
"""


class Producer:
    def __init__(self, kafka_server: str = 'kafka:9092', schema_registry_url: str = 'http://schema-registry:8081',
                 avro_topic: str = 'crypto_prices', fallback_topic: str = 'crypto_prices_fallback',
                 use_api: bool = True):
        self.scrapper = CoinGecko()
        self.use_api: bool = use_api
        self.kafka_server: str = kafka_server
        self.avro_topic: str = avro_topic
        self.fallback_topic: str = fallback_topic
        self.producer = None
        self.avro_producer = None

        self.create_kafka_producer(kafka_server)
        self.create_avro_producer(schema_registry_url)

    def create_kafka_producer(self, kafka_server: str):
        """Create a regular Kafka JSON producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialisation JSON
                linger_ms=30000,
                retries=5,
                retry_backoff_ms=100
            )
            print("Kafka JSON producer created successfully!")
        except KafkaError as e:
            print(f"Failed to create Kafka producer: {e}")
            raise

    def create_avro_producer(self, schema_registry_url: str):
        """Create an AvroProducer for Kafka"""
        try:
            value_schema = avro.loads(CRYPTO_VALUE_SCHEMA_STR)
            producer_config = {
                'bootstrap.servers': self.kafka_server,
                'schema.registry.url': schema_registry_url
            }
            self.avro_producer = AvroProducer(
                producer_config,
                default_value_schema=value_schema
            )
            print("Avro producer created successfully!")
        except Exception as e:
            print(f"Failed to create Avro producer: {e}")
            raise

    @SCRAP_TIME.time()
    def get_crypto_data(self, use_api=True):
        """Fetch cryptocurrency data via API or generate random data"""
        if use_api:
            try:
                prices_now = self.scrapper.get_prices(['bitcoin', 'ethereum'], ['usd'])
                btc_24h_ago = self.scrapper.get_price_24h_ago('bitcoin', 'usd')
                eth_24h_ago = self.scrapper.get_price_24h_ago('ethereum', 'usd')
                print("Data fetched from API")
            except Exception as e:
                print(f"Error fetching data from API: {e}")
                raise
        else:
            # Simulated random data
            prices_now = {
                'bitcoin': {'usd': random.uniform(30000, 60000)},
                'ethereum': {'usd': random.uniform(1000, 4000)}
            }
            btc_24h_ago = random.uniform(30000, 60000)
            eth_24h_ago = random.uniform(1000, 4000)
            print("Random data generated")

        # Add 24-hour prices to the data
        prices_now['bitcoin']['usd_yesterday'] = btc_24h_ago
        prices_now['ethereum']['usd_yesterday'] = eth_24h_ago
        return prices_now

    @SEND_TIME.time()
    def send_to_avro(self, crypto_data):
        """Transform and send cryptocurrency data as Avro messages"""
        timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds
        try:
            for currency, values in crypto_data.items():
                # Transform data to fit the Avro schema
                message = {
                    "timestamp": timestamp,
                    "symbol": currency,  # e.g. "bitcoin" or "ethereum"
                    "price": values['usd'],
                    "price_24h_ago": values['usd_yesterday']
                }
                print(f"Message to send to Avro: {message}")  # Log for debugging

                # Produce the message using AvroProducer
                self.avro_producer.produce(topic=self.avro_topic, value=message)
                self.avro_producer.flush()
                print(f"Avro message sent successfully to topic '{self.avro_topic}': {message}")
                MESSAGES_SENT.inc()
        except Exception as e:
            print(f"Error sending Avro message, falling back: {e}")
            self.send_to_fallback(message)

    def send_to_fallback(self, message):
        """Send messages to the fallback topic as JSON"""
        try:
            self.producer.send(self.fallback_topic, message)
            self.producer.flush()
            FALLBACK_MESSAGES.inc()
            print(f"Fallback message sent to topic '{self.fallback_topic}': {message}")
        except KafkaError as e:
            print(f"Error sending fallback message: {e}")

    @UP_TIME.time()
    def start(self, prometheus_port: int = 8000):
        """Start the producer process"""
        start_http_server(prometheus_port)  # Start Prometheus metrics server
        while True:
            crypto_data = self.get_crypto_data(use_api=self.use_api)
            self.send_to_avro(crypto_data)
            time.sleep(5)  # Wait 5 seconds before the next batch


if __name__ == "__main__":
    producer = Producer(
        kafka_server="kafka:9092",
        schema_registry_url="http://schema-registry:8081",
        avro_topic="crypto_prices",
        fallback_topic="crypto_prices_fallback"
    )
    producer.start(prometheus_port=8000)
