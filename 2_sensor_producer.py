import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.serializer.json import JsonSerializer
from configs import connection_params, SENSORS_TOPIC

producer = KafkaProducer(
    **connection_params(),
    value_serializer=JsonSerializer(),
    key_serializer=JsonSerializer(),
)

# ID датчика однаковий протягом одного запуску скрипту
sensor_id = random.randint(1000, 9999)
print(f"Sensor {sensor_id} started -> '{SENSORS_TOPIC}'")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "temperature": round(random.uniform(25, 45), 1),
            "humidity": round(random.uniform(15, 85), 1),
        }
        producer.send(SENSORS_TOPIC, key=sensor_id, value=data)
        producer.flush()
        print(f"Sent: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Stopped")
finally:
    producer.close()
