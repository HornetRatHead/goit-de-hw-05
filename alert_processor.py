from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

my_name = "tsy_hw_4"

consumer = KafkaConsumer(
    f"{my_name}_building_sensors",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8'),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert_processor_group'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8')
)

print("Alert processor started and listening for sensor data...")

for msg in consumer:
    try:
        data = msg.value
        sensor_id = data['sensor_id']
        temp = float(data['temperature'])  
        hum = float(data['humidity'])     
        timestamp = data['timestamp']

        if temp > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temp,
                "message": "Temperature exceeded 40°C"
            }
            producer.send(f"{my_name}_temperature_alerts", key=sensor_id, value=alert)
            print(f"[ALERT] Temp alert sent for sensor {sensor_id}: {temp}°C")

        if hum > 80 or hum < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": hum,
                "message": "Humidity out of range (20–80%)"
            }
            producer.send(f"{my_name}_humidity_alerts", key=sensor_id, value=alert)
            print(f"[ALERT] Humidity alert sent for sensor {sensor_id}: {hum}%")

        producer.flush()

    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}")
    data = msg.value
    sensor_id = data['sensor_id']
    temp = data['temperature']
    hum = data['humidity']
    timestamp = data['timestamp']

    if temp > 40:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temp,
            "message": "Temperature exceeded 40°C"
        }
        producer.send(f"{my_name}_temperature_alerts", key=sensor_id, value=alert)

    if hum > 80 or hum < 20:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "humidity": hum,
            "message": "Humidity out of range (20–80%)"
        }
        producer.send(f"{my_name}_humidity_alerts", key=sensor_id, value=alert)

    producer.flush()
