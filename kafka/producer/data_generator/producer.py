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
user_ids = list(range(1, 1001))  # Giả sử có 1000 người dùng
product_ids = list(range(1000, 2000))  # Giả sử có 1000 sản phẩm
device_types = ['mobile', 'desktop', 'tablet', 'smart_tv', 'wearable', 'other']
locations = ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Hue', 'Can Tho', 'Nha Trang', 'other']
user_segments = ['new', 'loyal', 'promo_hunter', 'high_value', 'other']

def generate_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

SPAM_USERS = [10, 455, 789, 123, 999]  # Giả sử đây là những người dùng spammer

while True:
    try:
        # 80% xác suất là spammer thực hiện "click", còn lại là user ngẫu nhiên
        if random.random() < 0.8:
            spam_user = random.choice(SPAM_USERS)
            event = {
                'user_id': spam_user,
                'product_id': random.choice(product_ids),
                'action': 'click',
                'timestamp': datetime.utcnow().isoformat(),
                'device_id': f"device_{random.randint(10000,99999)}",
                'device_type': random.choice(device_types),
                'location': random.choice(locations),
                'user_segment': 'promo_hunter',
                'ip_address': generate_ip()
            }
            interval = 0.5  # spam mỗi 500ms
        else:
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
            interval = 2  # user bình thường mỗi 2 giây

        future = producer.send('user_events', value=event)
        result = future.get(timeout=10)  # Chờ tối đa 10 giây để gửi
        if result:
            print(f"Đã gửi sự kiện: {event['action']} từ người dùng {event['user_id']} vào lúc {event['timestamp']}")
        else:
            print("Không có kết quả trả về từ Kafka.")
        time.sleep(interval)
    except KafkaError as e:
        print(f"Lỗi khi gửi message: {e}")
    time.sleep(1)