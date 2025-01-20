import json
import time
import random
import requests
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import avro
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import start_http_server, Summary, Counter

from scrapper.provider.CoinGecko import CoinGecko

UP_TIME = Summary('up_time_seconds', 'Time since the producer started')
SCRAP_TIME = Summary('scrap_time_seconds', 'Time spent processing request')
SEND_TIME = Summary('send_time_seconds', 'Time spent processing request')
MESSAGES_SENT = Counter('messages_sent_total', 'Total number of messages sent')
ERRORS_SENDING = Counter('errors_sending_total', 'Total number of messages sent')

# Définition du schéma Avro
value_schema_str = """
{
    "namespace": "crypto.prices",
    "type": "record",
    "name": "CryptoPrices",
    "fields": [
        {"name": "bitcoin", "type": {
            "type": "record",
            "name": "BitcoinPrice",
            "fields": [
                {"name": "usd", "type": "double"},
                {"name": "usd_yesterday", "type": "double"}
            ]
        }},
        {"name": "ethereum", "type": {
            "type": "record",
            "name": "EthereumPrice",
            "fields": [
                {"name": "usd", "type": "double"},
                {"name": "usd_yesterday", "type": "double"}
            ]
        }}
    ]
}
"""

import json
import time
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import requests
from prometheus_client import start_http_server, Counter, Summary


class Producer:
    def __init__(self, bootstrap_servers, schema_registry_url, topic_name, use_api=True):
        self.topic_name = topic_name
        self.use_api = use_api

        # Définition du schéma Avro
        value_schema_str = """
        {
            "type": "record",
            "name": "CryptoData",
            "fields": [
                {"name": "timestamp", "type": "long"},
                {"name": "symbol", "type": "string"},
                {"name": "price", "type": "double"}
            ]
        }
        """

        value_schema = avro.loads(value_schema_str)

        # Configuration du producteur
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'schema.registry.url': schema_registry_url
        }

        try:
            self.avro_producer = AvroProducer(
                producer_config,
                default_value_schema=value_schema
            )
            print("Kafka producers created successfully")
        except Exception as e:
            print(f"Error creating Kafka producer: {e}")
            raise

        # Métriques Prometheus
        self.messages_sent = Counter('crypto_messages_sent', 'Number of messages sent to Kafka')
        self.message_size = Summary('crypto_message_size_bytes', 'Size of messages in bytes')
        self.processing_time = Summary('crypto_processing_time_seconds', 'Time spent processing message')

    def get_crypto_data_from_api(self):
        """Récupère les données crypto depuis l'API"""
        url = "https://api.binance.com/api/v3/ticker/price"
        params = {"symbols": '["BTCUSDT","ETHUSDT","BNBUSDT"]'}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching data from API: {e}")
            return None

    def get_mock_crypto_data(self):
        """Génère des données crypto simulées"""
        mock_data = [
            {"symbol": "BTCUSDT", "price": "40000.00"},
            {"symbol": "ETHUSDT", "price": "2500.00"},
            {"symbol": "BNBUSDT", "price": "300.00"}
        ]
        return mock_data

    def get_crypto_data(self):
        """Récupère les données crypto (API ou mock)"""
        if self.use_api:
            return self.get_crypto_data_from_api()
        return self.get_mock_crypto_data()

    def send_message(self, message):
        """Envoie un message au topic Kafka"""
        try:
            self.avro_producer.produce(
                topic=self.topic_name,
                value=message
            )
            self.avro_producer.flush()
            self.messages_sent.inc()
            self.message_size.observe(len(str(message)))
        except Exception as e:
            print(f"Error sending message to Kafka: {e}")

    def start(self):
        """Démarre le producteur"""
        while True:
            with self.processing_time.time():
                crypto_data = self.get_crypto_data()
                if crypto_data:
                    timestamp = int(time.time() * 1000)
                    for data in crypto_data:
                        message = {
                            'timestamp': timestamp,
                            'symbol': data['symbol'],
                            'price': float(data['price'])
                        }
                        self.send_message(message)
            time.sleep(1)


def main(use_api=True):
    # Configuration
    bootstrap_servers = 'kafka:9092'
    schema_registry_url = 'http://schema-registry:8081'
    topic_name = 'crypto_prices'

    # Démarrage du serveur Prometheus
    start_http_server(8000)

    # Création et démarrage du producteur
    producer = Producer(bootstrap_servers, schema_registry_url, topic_name, use_api)
    producer.start()


if __name__ == "__main__":
    main(use_api=True)
