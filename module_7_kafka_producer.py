# module_7_kafka_producer.py
# Big Data Practicum - Module 7: Kafka Producer
# Environment: Google Colab (Requires Kafka Setup)

import json
import time
import random
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = "transaksi-toko"
KAFKA_SERVER = "localhost:9092"


def generate_dummy_data(pk_id):
    """Generates a dummy transaction JSON."""
    products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset"]
    product = random.choice(products)

    # Random Price and Quantity
    price_map = {
        "Laptop": 1000,
        "Mouse": 20,
        "Keyboard": 50,
        "Monitor": 200,
        "Headset": 80,
    }
    price = price_map[product]
    quantity = random.randint(1, 5)

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "transaction_id": pk_id,
        "product": product,
        "price": price,
        "quantity": quantity,
        "timestamp": timestamp,
    }


def run_producer():
    print(f"Connecting to Kafka at {KAFKA_SERVER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("Connected! Sending data...")

        transaction_count = 0
        try:
            while True:
                transaction_count += 1
                data = generate_dummy_data(transaction_count)

                # Send data to Kafka
                producer.send(KAFKA_TOPIC, data)

                print(f"Sent: {data}")
                time.sleep(2)  # Send every 2 seconds

        except KeyboardInterrupt:
            print("\nStopping producer...")
        finally:
            producer.close()

    except Exception as e:
        print(f"Error: {e}")
        print("Ensure Kafka is running and pip install kafka-python is done.")


if __name__ == "__main__":
    run_producer()
