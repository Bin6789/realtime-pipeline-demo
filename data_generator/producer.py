from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
from kafka.errors import KafkaError

try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9093',  # Adjust the port if necessary 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        max_block_ms=10000
    )
except KafkaError as e:
    print(f"Lỗi khi khởi tạo Kafka Producer: {e}")
    exit(1)

actions = ['view', 'click', 'add_to_cart', 'purchase']
user_ids = list(range(1, 101))
product_ids = list(range(1000, 1020))
device_types = ['mobile', 'desktop', 'tablet']
locations = ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Hue', 'Can Tho']
user_segments = ['new', 'loyal', 'promo_hunter']

def generate_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

while True:
    try:
        event = {
            'user_id': random.choice(user_ids),
            'product_id': random.choice(product_ids),
            'action': random.choice(actions),
            'timestamp': datetime.utcnow().isoformat(),
            'device_id': f"device_{random.randint(10000,99999)}",
            'device_type': random.choice(device_types),
            'location': random.choice(locations),
            'user_segment': random.choice(user_segments),
            'ip_address': generate_ip()
        }
        future = producer.send('user_events', value=event)
        result = future.get(timeout=10)
        print(f"Đã gửi: {event}")
    except KafkaError as e:
        print(f"Lỗi khi gửi message: {e}")
    time.sleep(1)