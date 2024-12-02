from time import sleep

from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, my_name

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    # security_protocol=kafka_config['security_protocol'],
    # sasl_mechanism=kafka_config['sasl_mechanism'],
    # sasl_plain_username=kafka_config['username'],
    # sasl_plain_password=kafka_config['password']
)

# Визначення нового топіку
num_partitions = 2
replication_factor = 1
topics = [f'{my_name}_building_sensors', f'{my_name}_alerts']

for topic_name in topics:
    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    # Створення нового топіку
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        # admin_client._wait_for_futures(admin_client.create_topics(new_topics=[new_topic], validate_only=False))
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

sleep(1) # wait for topics to be created

# Перевіряємо список існуючих топіків
print(admin_client.list_topics())

# admin_client.delete_topics(topics)
# admin_client.delete_topics(['alexander_humidity_alerts', 'alexander_temperature_alerts'])

# Закриття зв'язку з клієнтом
admin_client.close()

