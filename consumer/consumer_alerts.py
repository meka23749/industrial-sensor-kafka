import json
import logging
from kafka import KafkaConsumer, KafkaProducer


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
TOPIC_IN = 'sensor-data'
TOPIC_ALERTS = 'sensor-alerts'
GROUP_ID = 'alerts-consumer-group'

THRESHOLDS = {
    'temperature': {'max': 95.0, 'unit': 'C'},
    'vibration': {'max': 8.0, 'unit': 'mm/s'},
    'pressure': {'max': 8.0, 'unit': 'bar'},
}


def is_anomaly(reading):
    sensor_type = reading['sensor_type']
    if sensor_type not in THRESHOLDS:
        return False
    return reading['value'] > THRESHOLDS[sensor_type]['max']


def main():
    logger.info('Connecting to Kafka broker at %s', KAFKA_BROKER)

    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest'
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    logger.info('Alert consumer started — monitoring topic: %s', TOPIC_IN)

    for message in consumer:
        reading = message.value
        if is_anomaly(reading):
            threshold = THRESHOLDS[reading['sensor_type']]['max']
            severity = 'HIGH' if reading['value'] > threshold * 1.2 else 'MEDIUM'
            alert = {
                'sensor_id': reading['sensor_id'],
                'sensor_type': reading['sensor_type'],
                'value': reading['value'],
                'unit': reading['unit'],
                'threshold': threshold,
                'timestamp': reading['timestamp'],
                'severity': severity
            }
            producer.send(TOPIC_ALERTS, value=alert)
            producer.flush()
            logger.warning('ALERT [%s] %s = %.2f %s (threshold: %.2f) [%s]',
                           alert['sensor_id'],
                           alert['sensor_type'],
                           alert['value'],
                           alert['unit'],
                           alert['threshold'],
                           alert['severity'])
        else:
            logger.info('OK [%s] %s = %.2f %s',
                        reading['sensor_id'],
                        reading['sensor_type'],
                        reading['value'],
                        reading['unit'])


if __name__ == '__main__':
    main()
