import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

def get_crypto_data():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    response = requests.get(url)
    return response.json()

def on_send_success(record_metadata):
    print(f"Successfully sent message to topic: {record_metadata.topic}")
    print(f"Partition: {record_metadata.partition}")
    print(f"Offset: {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error sending message: {excp}")

def main():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=30000,  # increased linger_ms value to 30 seconds
        retries=5,
        retry_backoff_ms=100  # added retry backoff to handle temporary errors
    )
    topic = 'crypto_prices'

    while True:
        crypto_data = get_crypto_data()
        try:
            future = producer.send(topic, crypto_data)
            future.add_callback(on_send_success).add_errback(on_send_error)
            producer.flush()
            print(f"Sent data: {crypto_data}")
        except KafkaError as e:
            print(f"Error sending message: {e}")
        finally:
            producer.close()
        time.sleep(300)

if __name__ == "__main__":
    main()