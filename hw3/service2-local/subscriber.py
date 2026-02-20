import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

project_id = "main-tokenizer-486322-e1"
subscription_id = "hw3-forbidden-files-subscription"
LOCAL_FILE = "log.txt"

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Received message: {message}")
    
    try:
        # Decode the data
        if message.data:
            data = json.loads(message.data.decode("utf-8"))
            print(f"Data: {data}")
            
            # Append to local file for testing purpose
            # TODO: Change this to append to storage bucket
            with open(LOCAL_FILE, 'a') as f:
                f.write(json.dumps(data) + "\n")
                
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        # Shut down the subscriber gracefully on timeout.
        streaming_pull_future.cancel()
        streaming_pull_future.result()
