from kafka import KafkaConsumer, KafkaProducer
from kafka.serializer.json import JsonSerializer
from configs import connection_params, SENSORS_TOPIC, TEMP_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC

TEMP_MAX = 40
HUMIDITY_MIN = 20
HUMIDITY_MAX = 80

consumer = KafkaConsumer(
    SENSORS_TOPIC,
    **connection_params(),
    value_deserializer=JsonSerializer(),
    auto_offset_reset='latest',  # обробляємо лише нові дані з датчиків
    group_id='olivin_alert_processor',
)

producer = KafkaProducer(
    **connection_params(),
    value_serializer=JsonSerializer(),
    key_serializer=JsonSerializer(),
)


def send_alert(topic, data, message):
    alert = {
        "sensor_id": data["sensor_id"],
        "timestamp": data["timestamp"],
        "temperature": data["temperature"],
        "humidity": data["humidity"],
        "message": message,
    }
    producer.send(topic, key=data["sensor_id"], value=alert)
    print(f"  -> ALERT sent to '{topic}': {message}")


print(f"Processor subscribed to '{SENSORS_TOPIC}'")

try:
    for msg in consumer:
        data = msg.value
        print(f"Received: {data}")

        if data["temperature"] > TEMP_MAX:
            send_alert(TEMP_ALERTS_TOPIC, data,
                       f"Temperature {data['temperature']}°C exceeds {TEMP_MAX}°C")

        if data["humidity"] > HUMIDITY_MAX or data["humidity"] < HUMIDITY_MIN:
            send_alert(HUMIDITY_ALERTS_TOPIC, data,
                       f"Humidity {data['humidity']}% is out of range {HUMIDITY_MIN}-{HUMIDITY_MAX}%")

        producer.flush()
except KeyboardInterrupt:
    print("Stopped")
finally:
    consumer.close()
    producer.close()
