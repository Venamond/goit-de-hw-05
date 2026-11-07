kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN',
}

# Префікс, щоб імена топіків не перетиналися з іншими студентами
MY_NAME = "olivin"

SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
TEMP_ALERTS_TOPIC = f"{MY_NAME}_temperature_alerts"
HUMIDITY_ALERTS_TOPIC = f"{MY_NAME}_humidity_alerts"


def connection_params():
    # Спільні параметри підключення для producer / consumer / admin
    return dict(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
    )
