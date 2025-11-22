import time
import pandas as pd
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


KAFKA_TOPIC = 'tweets_topic'
KAFKA_SERVER = 'kafka:9092'

STATIC_FILE = '/app/data/airline_sentiment.csv'


print("--- Bắt đầu Kafka Producer (Static + Real-Time) ---")


def get_kafka_producer(server):
    retries = 30
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(">>> Connected Kafka Producer!")
            return producer
        except NoBrokersAvailable:
            print("Kafka chưa sẵn sàng, thử lại...")
            retries -= 1
            time.sleep(1)
    return None


def send_static_data(producer):
    print(f">>> Đang gửi FILE TĨNH: {STATIC_FILE}")
    df = pd.read_csv(STATIC_FILE)

    for _, row in df.iterrows():
        msg = row.where(pd.notnull(row), None).to_dict()
        producer.send(KAFKA_TOPIC, value=msg)
        print(f"[STATIC] gửi: {msg['airline_sentiment']} | {msg['airline']}")
    producer.flush()

def run():
    producer = get_kafka_producer(KAFKA_SERVER)
    if not producer:
        print("Không kết nối được Kafka. Thoát.")
        return

    send_static_data(producer)


if __name__ == "__main__":
    run()
