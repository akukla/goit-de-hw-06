from kafka import KafkaProducer
from configs import kafka_config, my_name
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    # security_protocol=kafka_config['security_protocol'],
    # sasl_mechanism=kafka_config['sasl_mechanism'],
    # sasl_plain_username=kafka_config['username'],
    # sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
topic_name = f'{my_name}_building_sensors'
i = 0
key = str(uuid.uuid4())
while True:
    # Відправлення повідомлення в топік
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "temp": random.randint(15, 75),  # Випадкове значення
            "humidity": random.randint(15, 85)  # Випадкове значення
        }
        producer.send(topic_name, key=key, value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(1)
        i += 1
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer

