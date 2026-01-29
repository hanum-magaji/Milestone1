import json
import os
import glob
from google.cloud import pubsub_v1

# Search for the service account JSON key
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
else:
    print("Error: No service account JSON key found.")
    exit()

# Set your specific Project ID, Topic Name, and Subscription ID
project_id = "milestone1-485717"
topic_name = "smartMeter"
subscription_id = "smartMeter-sub" 

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...\n")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Deserialize: Decode bytes to string, then load JSON to dict
        message_data = json.loads(message.data.decode('utf-8'))
        
        # Print the values of the dictionary
        print("Received dictionary values:")
        for key, value in message_data.items():
            print(f"  {key}: {value}")
        print("-" * 20)
        
        message.ack()
        
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()