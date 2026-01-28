import os
import glob
import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

# Set credentials
for file in glob.glob('*.json'):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file

# Configuration
project_id = "directed-symbol-485723-i0"
topic_id = "csvDataTopic"
subscription_id = "csvDataTopic-sub"

# Create subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message):
    print("=" * 50)
    print("Received message:")

    # Deserialize message
    message_str = message.data.decode('utf-8')
    record = json.loads(message_str)

    # Print all values
    print("Record values:")
    for key, value in record.items():
        print(f"  {key}: {value}")

    print("=" * 50)

    # Acknowledge message
    message.ack()


print(f"Listening for messages on {subscription_path}...")
print("Press Ctrl+C to stop.\n")

# Subscribe
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\nStopped listening.")