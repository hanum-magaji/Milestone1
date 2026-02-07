import os
import json
from google.cloud import pubsub_v1

# Search for the service account JSON key
for file in os.listdir("."):
    if file.endswith(".json"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file
        break

project_id = "projectmilestone-486607"
subscription_id = "LabelReader-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    # 2. Process each message: Deserialize into a dictionary
    data = json.loads(message.data.decode("utf-8"))
    
    # Print the values of the dictionary
    print(f"Received Data: {data}")
    
    # Acknowledge the message
    message.ack()

# 1. Receive messages from the topic
print(f"Listening for messages on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()