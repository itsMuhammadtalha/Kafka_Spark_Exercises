import time
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

counter = 1
print("Sending test data to 'input-topic'...")
try:
    while True:
        data = {"id": f"msg-{counter}", "amount": 100 * counter}
        producer.send("input-topic", data)
        print(f"Sent: {data}")
        counter += 1
        time.sleep(3) 
except KeyboardInterrupt:
    print("Producer stopped.")
