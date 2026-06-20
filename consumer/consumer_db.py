import json
import logging
import psycopg2
from kafka import KafkaConsumer


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'sensor-data'
GROUP_ID = 'db-consumer-group'

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'sensors',
    'user': 'admin',
    'password': 'admin123'
}


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id          SERIAL PRIMARY KEY,
                sensor_id   VARCHAR(50),
                sensor_type VARCHAR(50),
                value       FLOAT,
                unit        VARCHAR(20),
                anomaly     BOOLEAN,
                timestamp   TIMESTAMP
            );
        """)
        conn.commit()
    logger.info('Table sensor_readings ready')


def insert_reading(conn, reading):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sensor_readings
                (sensor_id, sensor_type, value, unit, anomaly, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            reading['sensor_id'],
            reading['sensor_type'],
            reading['value'],
            reading['unit'],
            reading['anomaly'],
            reading['timestamp']
        ))
        conn.commit()


def main():
    logger.info('Connecting to PostgreSQL...')
    conn = psycopg2.connect(**DB_CONFIG)
    create_table(conn)

    logger.info('Connecting to Kafka broker at %s', KAFKA_BROKER)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    logger.info('Consumer started — listening on topic: %s', TOPIC)

    for message in consumer:
        reading = message.value
        insert_reading(conn, reading)
        status = 'ANOMALY' if reading['anomaly'] else 'OK'
        logger.info('Stored: [%s] %s = %.2f %s [%s]',
                    reading['sensor_id'],
                    reading['sensor_type'],
                    reading['value'],
                    reading['unit'],
                    status)


if __name__ == '__main__':
    main()
