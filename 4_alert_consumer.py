from kafka import KafkaConsumer
from kafka.serializer.json import JsonSerializer
from configs import connection_params, TEMP_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC

consumer = KafkaConsumer(
    TEMP_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC,
    **connection_params(),
    value_deserializer=JsonSerializer(),
    auto_offset_reset='earliest',  # показуємо також сповіщення, що вже накопичились
    group_id='olivin_alert_reader',
)

print(f"Listening to '{TEMP_ALERTS_TOPIC}' and '{HUMIDITY_ALERTS_TOPIC}'")

try:
    for msg in consumer:
        a = msg.value
        print(f"[{msg.topic}] sensor {a['sensor_id']} at {a['timestamp']}: {a['message']} "
              f"(t={a['temperature']}°C, h={a['humidity']}%)")
except KeyboardInterrupt:
    print("Stopped")
finally:
    consumer.close()
