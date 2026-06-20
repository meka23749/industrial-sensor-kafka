import json
import time
import logging
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'test-sensor-data'

DB_CONFIG = {
    'host'    : 'localhost',
    'port'    : 5432,
    'dbname'  : 'sensors',
    'user'    : 'admin',
    'password': 'admin123'
}

def test_kafka_producer_consumer():
    logger.info('TEST 1: Kafka producer/consumer')
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    test_message = {'sensor_id': 'test-01', 'value': 42.0, 'unit': 'C'}
    producer.send(TOPIC, value=test_message)
    producer.flush()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id='test-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000
    )
    received = []
    for message in consumer:
        received.append(message.value)
        break

    assert len(received) == 1, 'No message received from Kafka'
    assert received[0]['sensor_id'] == 'test-01', 'Wrong sensor_id'
    assert received[0]['value'] == 42.0, 'Wrong value'
    logger.info('TEST 1 PASSED: Kafka producer/consumer OK')

def test_postgresql_connection():
    logger.info('TEST 2: PostgreSQL connection')
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
        result = cur.fetchone()
    assert result[0] == 1, 'PostgreSQL connection failed'
    conn.close()
    logger.info('TEST 2 PASSED: PostgreSQL connection OK')

def test_postgresql_insert():
    logger.info('TEST 3: PostgreSQL insert and read')
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id SERIAL PRIMARY KEY,
                sensor_id VARCHAR(50),
                sensor_type VARCHAR(50),
                value FLOAT,
                unit VARCHAR(20),
                anomaly BOOLEAN,
                timestamp TIMESTAMP
            )
        """)
        cur.execute("""
            INSERT INTO sensor_readings
                (sensor_id, sensor_type, value, unit, anomaly, timestamp)
            VALUES ('test-01', 'temperature', 75.5, 'C', false, NOW())
        """)
        conn.commit()
        cur.execute("SELECT value FROM sensor_readings WHERE sensor_id = 'test-01'")
        result = cur.fetchone()
    assert result[0] == 75.5, 'Wrong value in database'
    conn.close()
    logger.info('TEST 3 PASSED: PostgreSQL insert/read OK')

if __name__ == '__main__':
    logger.info('Starting pipeline integration tests...')
    test_kafka_producer_consumer()
    test_postgresql_connection()
    test_postgresql_insert()
    logger.info('All tests PASSED')