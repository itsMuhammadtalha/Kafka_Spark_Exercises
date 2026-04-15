import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Instantiate Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "transactions-topic"
merchants = ["Amazon", "Walmart", "Apple", "Uber", "Local Coffee Shop"]
users = [f"user_{i}" for i in range(1, 100)]

print(f"Starting to produce live transactions to topic: {TOPIC}...")

try:
    while True:
        # Guarantee unique transaction ID
        transaction_id = str(uuid.uuid4())
        
        #  5% chance the amount is > 10,000 for the rule engine to catch
        is_fraud = random.random() < 0.05
        amount = round(random.uniform(10, 500) if not is_fraud else random.uniform(10000, 50000), 2)

        data = {
            "transaction_id": transaction_id,
            "user_id": random.choice(users),
            "merchant": random.choice(merchants),
            "amount": amount,
            "timestamp": datetime.utcnow().isoformat()
        }

        producer.send(TOPIC, data)
        print(f"Produced: {data}")
        time.sleep(random.uniform(0.1, 1.0)) # Rapid streaming
        
except KeyboardInterrupt:
    print("Producer successfully stopped.")
