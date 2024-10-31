import json
import time
import random
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import start_http_server, Summary, Counter

REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
MESSAGES_SENT = Counter('messages_sent_total', 'Total number of messages sent')

def get_crypto_data(use_api=True):
    if use_api:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
        response = requests.get(url)
        data = response.json()
        print("Data fetched from API")
    else:
        data = {
            "bitcoin": {"usd": random.uniform(30000, 60000)},
            "ethereum": {"usd": random.uniform(1000, 4000)},
            "transaction_id": random.randint(1, 1000000)
        }
        print("Random data generated")

    data['transaction_id'] = random.randint(1, 1000000)
    return data

def on_send_success(record_metadata):
    print(f"Successfully sent message to topic: {record_metadata.topic}")
    print(f"Partition: {record_metadata.partition}")
    print(f"Offset: {record_metadata.offset}")
    MESSAGES_SENT.inc()

def on_send_error(excp):
    print(f"Error sending message: {excp}")

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=30000,
            retries=5,
            retry_backoff_ms=100
        )
        print("Kafka producer created successfully")
        return producer
    except KafkaError as e:
        print(f"Failed to create Kafka producer: {e}")
        raise

@REQUEST_TIME.time()
def main(use_api=True):
    producer = create_producer()
    topic = 'crypto_prices'

    start_http_server(8000)

    while True:
        crypto_data = get_crypto_data(use_api=use_api)
        try:
            future = producer.send(topic, crypto_data)
            future.add_callback(on_send_success).add_errback(on_send_error)
            producer.flush()
            print(f"Sent data: {crypto_data}")
        except KafkaError as e:
            print(f"Error sending message: {e}")
            if 'RecordAccumulator is closed' in str(e):
                producer = create_producer()
        time.sleep(1)

if __name__ == "__main__":
    main(use_api=False)
