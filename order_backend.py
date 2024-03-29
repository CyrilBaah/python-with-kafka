import json
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("ORDER GENERATION FACTORY | 10 Seconds")
print("Will generate one unique order every 10 seconds")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{1}",
        "total_cost": i * 2,
        "items": "burger, sandwich"
    }
    
    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )
    
    print(f"Done generating ...{i}")
    time.sleep(10)