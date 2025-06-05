from kafka import KafkaConsumer
from configs import kafka_config
import json

my_name = "tstsy_hw_4"

consumer = KafkaConsumer(
    f"{my_name}_temperature_alerts",
    f"{my_name}_humidity_alerts",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8'),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert_printer_group'
)

print("Listening to alerts...")

try:
    for msg in consumer:
        print(f"[ALERT] {msg.topic} | Sensor: {msg.key} | {msg.value}")
except Exception as e:
    print("Error:", e)
finally:
    consumer.close()
