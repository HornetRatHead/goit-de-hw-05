from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random
import uuid

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "tsy_hw_4"
topic_name = f"{my_name}_building_sensors"

# Генеруємо унікальний ID для кожного запуску
sensor_id = str(uuid.uuid4())

print(f"Sensor {sensor_id} is sending data to topic '{topic_name}'")

try:
    for i in range(300):  # Відправляємо 300 повідомлень
        temperature = random.uniform(25, 45)
        humidity = random.uniform(15, 85)
        timestamp = time.time()

        message = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2)
        }

        producer.send(topic_name, key=sensor_id, value=message)
        producer.flush()
        print(f"[{i+1}] Sent: {message}")
        time.sleep(2)
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    producer.close()
