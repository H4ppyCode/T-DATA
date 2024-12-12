import json
import time
import random
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import start_http_server, Summary, Counter

from scrapper.provider.CoinGecko import CoinGecko

UP_TIME = Summary('up_time_seconds', 'Time since the producer started')
SCRAP_TIME = Summary('scrap_time_seconds', 'Time spent processing request')
SEND_TIME = Summary('send_time_seconds', 'Time spent processing request')
MESSAGES_SENT = Counter('messages_sent_total', 'Total number of messages sent')
ERRORS_SENDING = Counter('errors_sending_total', 'Total number of messages sent')


class Producer:
    def __init__(self, kafka_server: str = 'kafka:9092', destination_topic: str = 'crypto_prices', use_api: bool = True):
        self.scrapper = CoinGecko()
        self.use_api: bool = use_api
        self.producer: KafkaProducer
        self.kafka_server: str = kafka_server
        self.create_producer(kafka_server)
        self.destination_topic: str = destination_topic


    def create_producer(self, kafka_server: str):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=30000,
                retries=5,
                retry_backoff_ms=100
            )
            print("Kafka producer created successfully")
        except KafkaError as e:
            print(f"Failed to create Kafka producer: {e}")
            raise

    @SCRAP_TIME.time()
    def get_crypto_data(self, use_api=True):
        if use_api:
            prices_now = self.scrapper.get_prices(['bitcoin', 'ethereum'], ['usd'])
            btc_24h_ago = self.scrapper.get_price_24h_ago('bitcoin', 'usd')
            eth_24h_ago = self.scrapper.get_price_24h_ago('ethereum', 'usd')
            print("Data fetched from API")
        else:
            prices_now = {
                'bitcoin': {'usd': random.uniform(30000, 60000)},
                'ethereum': {'usd': random.uniform(1000, 4000)}
            }
            btc_24h_ago = random.uniform(30000, 60000)
            eth_24h_ago = random.uniform(1000, 4000)
            print("Random data generated")
        prices_now['bitcoin']['usd_yesterday'] = btc_24h_ago
        prices_now['ethereum']['usd_yesterday'] = eth_24h_ago
        return prices_now

    @SEND_TIME.time()
    def send_to_topic(self, data):
        try:
            future = self.producer.send(self.destination_topic, data)
            future.add_callback(self.on_send_success).add_errback(self.on_send_error)
            self.producer.flush()
            print(f"Sent data: {data}")
        except KafkaError as e:
            print(f"Error sending message: {e}")
            if 'RecordAccumulator is closed' in str(e):
                self.create_producer(self.kafka_server)

    @UP_TIME.time()
    def start(self, prometheus_port: int = 8000):
        start_http_server(prometheus_port)
        while True:
            crypto_data = self.get_crypto_data(use_api=self.use_api)
            self.send_to_topic(crypto_data)

    def on_send_success(self, record_metadata):
        print(f"Successfully sent message to topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
        MESSAGES_SENT.inc()

    def on_send_error(self, excp):
        print(f"Error sending message: {excp}")
        ERRORS_SENDING.inc()

def main(use_api=True):
    producer = Producer(use_api=use_api)
    producer.start()

if __name__ == "__main__":
    main(use_api=True)
