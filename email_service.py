import json
import logging
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

email_sent_so_far = set()
logger.info("Email listener is running...")

try:
    for message in consumer:
        try:
            consumed_message = json.loads(message.value.decode())
            customer_email = consumed_message["customer_email"]
            logger.info(f"Sending email to {customer_email}")
            
            email_sent_so_far.add(customer_email)
            logger.info(f"So far emails sent to {len(email_sent_so_far)} unique emails.")
        except KeyError as e:
            logger.error(f"Error processing message: {e}. Skipping...")
except KeyboardInterrupt:
    logger.info("Stopping email listener...")
finally:
    consumer.close()
