from google.cloud import pubsub_v1
from faker import Faker
import json
import time
import random

fake = Faker()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('click-stream-463520', 'clicks')

def generate_clickstream_data():
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    return {
        'timestamp': current_time,
        'user_id': f'user_{random.randint(1, 1000)}',
        'session_id': f'session_{random.randint(1, 10000)}',
        'url': random.choice(['/home', '/products/shoes', '/cart']),
        'http_status_code': random.choice(['200', '404', '500']),
        'response_time_ms': str(random.randint(50, 500)),
        'event_type': random.choice(['page_view', 'add_to_cart', 'purchase', 'click']),
        'product_id': f'prod_{random.randint(100, 999)}',
        'price': str(round(random.uniform(10, 200), 2)),
        'colour': random.choice(['Blue', 'Red', 'Black']),
        'category': random.choice(['Shoes', 'Clothing', 'Electronics']),
        'geolocation': f'{fake.city()}, {fake.country()}',
        'user_agent': fake.user_agent(),
        'entry_url': '/home',
        'exit_url': random.choice(['', '/cart']),
        'session_start_time': current_time,
        'session_end_time': current_time,
        'click_timestamp': current_time,
        'error_type': random.choice(['', 'ClientError', 'ServerError']),
        'error_message': random.choice(['', 'Timeout', 'Invalid Request']),
        'first_visit_timestamp': current_time,
        'session_count': str(random.randint(1, 10))
    }

def publish_data():
    message_count = 0
    while True:
        data = generate_clickstream_data()
        # Convert all values to strings as per schema
        data = {k: str(v) for k, v in data.items()}
        # Publish with attributes for the new template
        message = json.dumps(data).encode('utf-8')
        future = publisher.publish(
            topic_path,
            message,
            timestamp=str(int(time.time() * 1000))  # milliseconds
        )
        message_id = future.result()
        message_count += 1
        print(f'[{message_count}] Published message: {data["user_id"]} - {data["event_type"]} (ID: {message_id})')
        time.sleep(1)

if __name__ == '__main__':
    publish_data()