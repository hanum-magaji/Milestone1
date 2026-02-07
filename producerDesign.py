import os
import csv
import json
from google.cloud import pubsub_v1

# Search for the service account JSON key in the current directory
for file in os.listdir("."):
    if file.endswith(".json"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file
        break

# Set your project ID and topic name
project_id = "projectmilestone-486607" 
topic_name = "LabelReader"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# 1. Read the CSV file
csv_file_path = "Labels.csv"

with open(csv_file_path, mode='r') as infile:
    # 2. Iterate over the records in the CSV file
    reader = csv.DictReader(infile)
    for row in reader:
        # Convert record to dictionary (already done by DictReader) and serialize
        message_data = json.dumps(row).encode("utf-8")
        
        # Publish the message to the topic
        future = publisher.publish(topic_path, data=message_data)
        print(f"Published record: {row['profileName']} at {row['time']}")
        future.result() 

print("All records published successfully.")