from confluent_kafka import Consumer
import json
from dotenv import load_dotenv
import os

load_dotenv()
consumer_config = { 'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
                    'group.id': 'order-tracker',
                    'auto.offset.reset': 'earliest',
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'PLAIN',
                    'sasl.username': os.getenv("SASL_USERNAME"),
                    'sasl.password': os.getenv("SASL_PASSWORD")}
consumer = Consumer(consumer_config)
consumer.subscribe(["Orders"])

print("✅ consumer is running and subscribed to Orders topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Error: {msg.error()}")
            continue
        value=msg.value().decode("utf-8")
        Order=json.loads(value)
        print(f"📦 received order: {Order['quantity']} X {Order['item']} from {Order['user']}")
except KeyboardInterrupt:
    print("\n 🔴 stopping consumer")
finally:
    consumer.close()