import requests
import time
import json
from datetime import datetime
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# === Cấu hình ===
KAFKA_TOPIC = 'reddit_topic'
KAFKA_SERVER = 'kafka:9092'

MODEL_PATH = "./model"
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)
model.eval()
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

# Hàm kết nối Kafka
def get_kafka_producer(server):
    producer = None
    retries = 30
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(">>>Đã kết nối Kafka Producer!")
            return producer
        except NoBrokersAvailable:
            print(f"Không thể kết nối Kafka. Thử lại sau 5 giây...")
            retries -= 1
            time.sleep(5)
    return None

# === Model ===
def roberta_sentiment(text):
    if not text:
        return "neutral"

    inputs = tokenizer(
        text,
        truncation=True,
        padding=True,
        max_length=128,
        return_tensors="pt"
    ).to(device)

    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits
        pred = torch.argmax(logits, dim=1).item()

    label_map = {0: "negative", 1: "neutral", 2: "positive"}
    return label_map.get(pred, "neutral")

# Cấu hình Reddit
subreddits_by_airline = {
    "UnitedAirlines": ["UnitedAirlines"],
    "AmericanAirlines": ["AmericanAirlines"],
    "SouthwestAirlines": ["SouthwestAirlines"],
    "Delta": ["Delta"],
    "AlaskaAirlines": ["AlaskaAirlines"],
}
headers = {'User-Agent': 'StudentProject/1.0'}

producer = get_kafka_producer(KAFKA_SERVER)
if not producer: exit()

print("Bắt đầu crawl và gán nhãn theo quy tắc đếm từ...")
stt = 1
seen_comment_ids = set()

try:
    while True:
        for airline, sub_list in subreddits_by_airline.items():
            subreddit = random.choice(sub_list)
            url = f"https://www.reddit.com/r/{subreddit}/comments.json?sort=new&limit=5"

            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    comments = data.get("data", {}).get("children", [])

                    for comment in comments:
                        c_data = comment.get("data", {})
                        c_id = c_data.get("id")

                        if c_id in seen_comment_ids: continue
                        seen_comment_ids.add(c_id)

                        body = c_data.get("body", "").strip().replace("\n", " ")
                        created_utc = datetime.fromtimestamp(c_data.get("created_utc", 0)).strftime("%Y-%m-%d %H:%M:%S")

                        if body:
                            # 1. Gán nhãn ngay tại đây
                            sentiment = roberta_sentiment(body)
                            
                            # 2. Tạo message
                            message = {
                                "id": stt,
                                "event_time": created_utc,
                                "airline": airline,
                                "text": body,
                                "sentiment": sentiment # Đã có nhãn!
                            }
                            
                            # 3. Gửi Kafka
                            producer.send(KAFKA_TOPIC, value=message)
                            print(f"[#{stt}] [{airline}] ({sentiment}): {body[:30]}...")
                            stt += 1
            except Exception as e:
                print(f"Lỗi request: {e}")
            
            time.sleep(2)

        print("Đợi 5s...")
        time.sleep(5)

except KeyboardInterrupt:
    print("Stop.")
finally:
    if producer: producer.close()
