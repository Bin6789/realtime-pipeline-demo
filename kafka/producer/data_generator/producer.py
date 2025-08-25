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
        compression_type='snappy',
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

def generate_event():
    is_spammer = random.random() < 0.4  # 40% xác suất là spammer
    user_id = random.choice(SPAM_USERS) if is_spammer else random.choice(user_ids)
    return {
        'user_id': user_id,
        'product_id': random.choice(product_ids),
        'action': 'click' if is_spammer else random.choice(actions),
        'timestamp': datetime.utcnow().isoformat(),
        'device_id': f"device_{random.randint(10000,99999)}",
        'device_type': random.choice(device_types),
        'location': random.choice(locations),
        'user_segment': 'promo_hunter' if is_spammer else random.choice(user_segments),
        'ip_address': generate_ip()
    }
batch_size = 1000
batch = []
start_time = time.time()
while True:
    try:
        batch.append(generate_event())
        if len(batch) >= batch_size:
            for event in batch:
                future = producer.send('user_events', value=event)
                future.get(timeout=10)  # Chờ tối đa 10 giây để gửi
            producer.flush()    
            elapsed = time.time() - start_time
            print(f"Đã gửi {len(batch)} sự kiện trong {elapsed:.2f}s giây (~{len(batch) / elapsed:.2f} sự kiện/giây)")
            batch = []
            start_time = time.time()
            time.sleep(0.1)
    except KafkaError as e:
        print(f"Lỗi khi gửi message: {e}")
    time.sleep(0.1)