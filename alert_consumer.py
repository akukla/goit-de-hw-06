from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, my_name
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    # security_protocol=kafka_config['security_protocol'],
    # sasl_mechanism=kafka_config['sasl_mechanism'],
    # sasl_plain_username=kafka_config['username'],
    # sasl_plain_password=kafka_config['password'],
    # value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    # key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

topics = [f'{my_name}_alerts']
# Підписка на тему
consumer.subscribe(topics)

print(f"Subscribed to topics '{topics}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        # print(f"{type(message)}")
        msg = json.loads(message.value.decode('utf-8'))
        # print(f"{message.topic} Received message: {message.value} with key: {message.key}, partition {message.partition}")
        print(f"{message.topic} Received message: {msg}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer