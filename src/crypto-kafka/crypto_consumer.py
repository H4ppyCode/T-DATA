import json
from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        'crypto_prices',
        group_id='crypto-group',
        bootstrap_servers=['kafka:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        print(f"Received data: {message.value}")

if __name__ == "__main__":
    main()