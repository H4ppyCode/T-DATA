import json
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Gauge
import random
import time

def main():
    while True:
        try:
            consumer = KafkaConsumer(
                'processed_crypto_prices',
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            break
        except:
            print("Error connecting to Kafka. Retrying...")
            time.sleep(10)
            continue
    print("Connected to Kafka")

    

    # Prometheus metrics
    btc_diff_gauge = Gauge('btc_diff', 'Bitcoin Difference')
    btc_performance_gauge = Gauge('btc_performance', 'Bitcoin Performance')
    btc_growth_gauge = Gauge('btc_growth', 'Bitcoin Growth')

    eth_diff_gauge = Gauge('eth_diff', 'Ethereum Difference')
    eth_performance_gauge = Gauge('eth_performance', 'Ethereum Performance')
    eth_growth_gauge = Gauge('eth_growth', 'Ethereum Growth')

    start_http_server(8002)  # Expose metrics at http://localhost:8002/metrics
    


    while True:
        for message in consumer:
            print(f"Received data: {message.value}")
            data = message.value

            # Update Prometheus metrics
            btc_diff_gauge.set(random.randint(0, 100))
            btc_diff_gauge.set(data.get("btc_diff", 0))
            btc_performance_gauge.set(data.get("btc_performance", 0))
            btc_growth_gauge.set(data.get("btc_growth", 0))

            eth_diff_gauge.set(data.get("eth_diff", 0))
            eth_performance_gauge.set(data.get("eth_performance", 0))
            eth_growth_gauge.set(data.get("eth_growth", 0))


if __name__ == "__main__":
    main()
