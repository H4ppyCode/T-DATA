import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import start_http_server, Summary, Counter


# Create Prometheus metrics
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
MESSAGES_SENT = Counter('messages_sent_total', 'Total number of messages sent')

def get_crypto_data():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    response = requests.get(url)
    return response.json()

def on_send_success(record_metadata):
    print(f"Successfully sent message to topic: {record_metadata.topic}")
    print(f"Partition: {record_metadata.partition}")
    print(f"Offset: {record_metadata.offset}")
    MESSAGES_SENT.inc()

def on_send_error(excp):
    print(f"Error sending message: {excp}")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=30000,  # increased linger_ms value to 30 seconds
        retries=5,
        retry_backoff_ms=100  # added retry backoff to handle temporary errors
    )
@REQUEST_TIME.time()  # Measure the time taken by this function
def main():
    producer = create_producer()
    topic = 'crypto_prices'

    # Start up the server to expose the metrics.
    start_http_server(8000)

    while True:
        crypto_data = get_crypto_data()
        try:
            future = producer.send(topic, crypto_data)
            future.add_callback(on_send_success).add_errback(on_send_error)
            producer.flush()
            print(f"Sent data: {crypto_data}")
        except KafkaError as e:
            print(f"Error sending message: {e}")
            if 'RecordAccumulator is closed' in str(e):
                producer = create_producer()  # Reinitialize the producer
        time.sleep(20)

if __name__ == "__main__":
    main()