import json
import time
import random
import logging
from datetime import datetime, UTC
from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'sensor-data'

SENSORS = [
    {'id': 'sensor-temp-01', 'type': 'temperature', 'unit': 'C', 'min': 60.0, 'max': 90.0, 'anomaly_max': 110.0},
    {'id': 'sensor-vibr-01', 'type': 'vibration', 'unit': 'mm/s', 'min': 0.5, 'max': 5.0, 'anomaly_max': 15.0},
    {'id': 'sensor-press-01', 'type': 'pressure', 'unit': 'bar', 'min': 1.0, 'max': 6.0, 'anomaly_max': 10.0},
]


def generate_reading(sensor):
    anomaly = random.random() < 0.05
    if anomaly:
        value = round(random.uniform(sensor['max'], sensor['anomaly_max']), 2)
    else:
        value = round(random.uniform(sensor['min'], sensor['max']), 2)
    return {
        'sensor_id': sensor['id'],
        'sensor_type': sensor['type'],
        'value': value,
        'unit': sensor['unit'],
        'anomaly': anomaly,
        'timestamp': datetime.now(UTC).isoformat()
    }


def main():
    logger.info('Connecting to Kafka broker at %s', KAFKA_BROKER)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info('Producer started — sending to topic: %s', TOPIC)

    while True:
        for sensor in SENSORS:
            reading = generate_reading(sensor)
            producer.send(TOPIC, value=reading)
            status = 'ANOMALY' if reading['anomaly'] else 'OK'
            logger.info('[%s] %s = %.2f %s [%s]',
                        reading['sensor_id'], reading['sensor_type'],
                        reading['value'], reading['unit'], status)
        producer.flush()
        time.sleep(1)


if __name__ == '__main__':
    main()