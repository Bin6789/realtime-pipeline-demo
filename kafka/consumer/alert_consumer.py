import time
from kafka import KafkaConsumer
import json
import os

import requests
import logging
from pathlib import Path

from dotenv import load_dotenv

# ========================
# Load environment variables
# ========================
load_dotenv()

# ========================
# CONFIG
# ========================
KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC = "anomaly_alerts"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Log files
LOG_FILE = str(Path("D:/realtime-pipeline/tmp/alert_consumer.log"))
ALERT_PAYLOAD_FILE = str(Path("D:/realtime-pipeline/tmp/alert_payload.log"))


logging.basicConfig(
    level=logging.INFO,
    filename=LOG_FILE,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filemode="a",
    encoding="utf-8"
)
logger = logging.getLogger("alert_consumer")

# ========================
# Helpers
# ========================
def send_telegram_message(msg, max_retries=3, retry_delay=1):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not config -> skipping")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }
    for attempt in range(max_retries):
        try:
            response = requests.post(url, data=payload, timeout=5)
            if response.status_code == 200:
                logger.info("Telegram message sent successfully.")
                return True
            else:
                logger.warning(f"Failed to send Telegram message: {response.status_code}")
        except requests.RequestException as e:
            logger.warning("Telegram send failed (attempt %d/%d): %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    return False

# Log vào file
def log_to_file(alert):
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(alert) + "\n")  
# ========================
# Kafka Consumer
# ========================
try:
        #Kết nối kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="alert_consumer_group",
        max_poll_records=1000
    )
except Exception as e:
    logger.error(f"Error initializing Kafka Consumer: {e}")
    exit(1)
logging.info("Đang lắng nghe các thông báo cảnh báo từ Kafka trên topic '%s'....", KAFKA_TOPIC)

#batch processing
batch = []
start_time = time.time()
for message in consumer:
    alert = message.value
    batch.append(alert)
    if len(batch) >= 100 or (time.time() - start_time) > 1:  # Process batch every 100 messages or 1s
        for alert in batch:
            try:
                msg = (
                    f"⚠️ Anomaly detected:\n"
                    f"User: {alert['user_id']} ({alert.get('user_name', 'Unknown')})\n"
                    f"Region: {alert.get('region', 'Unknown')}\n"
                    f"Count: {alert['event_count']}\n"
                    f"Window: {alert['window_start']} - {alert['window_end']}\n"
                    f"Z-score: {alert['z_score']}\n"
                    f"Severity: {alert['severity']}"
                )
                logger.info("Đã nhân được thông báo: %s", json.dumps(alert))
                print(f"[ALERT] {msg}")
                # Ghi lại payload đầy đủ của cảnh báo vào file riêng
                with open(ALERT_PAYLOAD_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(alert) + "\n")
                if alert['severity'] in ['high', 'medium']:
                    if not send_telegram_message(msg):
                        logger.warning("Failed to send Telegram message, logged to file -> skipping")
            except KeyError as e:
                logger.error("Invalid alert schema: %s, alert: %s", e, alert)
            except Exception as e:
                logger.error("Error processing alert: %s", e)
        batch = []
        start_time = time.time()
