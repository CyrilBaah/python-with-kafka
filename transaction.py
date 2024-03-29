import json
from kafka import KafkaConsumer, KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(ORDER_KAFKA_TOPIC,
                         bootstrap_servers="localhost:29092",
                         auto_offset_reset='earliest')

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Start Listening...")

try:
    for message in consumer:
        print("Transactions Ongoing...")
        try:
            consumed_message = json.loads(message.value.decode())
            print(consumed_message)

            user_id = consumed_message["user_id"]
            total_cost = consumed_message["total_cost"]
            
            data = {
                "consumer_id": user_id,
                "consumer_email": f"{user_id}@gmail.com",
                "total_cost": total_cost
            }
            
            print("Successful transaction...")
            producer.send(
                ORDER_CONFIRMED_KAFKA_TOPIC,
                json.dumps(data).encode("utf-8")
            )
        except Exception as e:
            print(f"Error processing message: {e}. Skipping...")
except KeyboardInterrupt:
    print("Stopping consumer and producer...")
finally:
    consumer.close()
    producer.close()
