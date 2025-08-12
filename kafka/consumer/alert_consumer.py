from kafka import KafkaConsumer
import json
import os

import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Gửi tin nhắn tới bot chat telegram
def send_telegram_message(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }
    response = requests.post(url, data=payload)
    if response.status_code != 200:
        print(f"[WARN] Telegram API error: {response.text}")

#Kết nối kafka
consumer = KafkaConsumer(
    'anomaly_alerts',
    bootstrap_servers='localhost:9093',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Đang lắng nghe các thông báo cảnh báo từ Kafka trên topic 'anomaly_alerts'....")
for message in consumer:
    alert = message.value
    msg = (
        f"⚠️ Anomaly detected:\n"
        f"User: {alert['user_id']}\n"
        f"Count: {alert['count']}\n"
        f"Window: {alert['window_start']} - {alert['window_end']}\n"
        f"Z-score: {alert['z_score']}"
    )
    try:
        send_telegram_message(msg)
        print("Đã gửi thông báo tới Telegram.")
    except Exception as e:
        print(f"[ERROR] Gửi thông báo thất bại: {e}")
