import os
import glob
import json
import csv
from google.cloud import pubsub_v1

# Set credentials
for file in glob.glob('*.json'):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file

# Configuration
project_id = "directed-symbol-485723-i0"
topic_id = "csvDataTopic"

# Create publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

print("Reading CSV file and publishing records...")

# Read and publish CSV records
with open('Labels.csv', 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile)

    for row in csv_reader:
        # Convert row to dictionary (already is one from DictReader)
        record = dict(row)

        # Serialize to JSON
        message_json = json.dumps(record)
        message_bytes = message_json.encode('utf-8')

        # Publish
        try:
            future = publisher.publish(topic_path, data=message_bytes)
            print(f"Published record: {record}")
            future.result()
        except Exception as e:
            print(f"Error publishing: {e}")

print("All records published!")