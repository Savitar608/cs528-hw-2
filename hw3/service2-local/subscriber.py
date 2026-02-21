import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage

import threading

# Configuration: GCP project, Pub/Sub subscription, and GCS bucket details
project_id = "main-tokenizer-486322-e1"
subscription_id = "hw3-forbidden-files-subscription"

# Configuration: Bucket name
BUCKET_NAME = "cs528-adithyav-hw2"
LOG_FILE = "forbidden-countries/log.txt"

subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# Lock for local thread safety when writing to GCS
log_lock = threading.Lock() 

subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Received message: {message}")
    
    try:
        # Decode the data
        if message.data:
            data = json.loads(message.data.decode("utf-8"))
            print(f"Data: {data}")
            
            # Append to storage bucket with concurrency handling
            with log_lock:
                try:
                    blob = bucket.get_blob(LOG_FILE)
                    existing_content = ""
                    if blob:
                        existing_content = blob.download_as_text()
                    else:
                        blob = bucket.blob(LOG_FILE)
                    
                    new_content = existing_content + json.dumps(data) + "\n"
                    
                    blob.upload_from_string(new_content)
                    print(f"Appended log to gs://{BUCKET_NAME}/{LOG_FILE}")
                
                except Exception as gcs_error:
                     print(f"Error writing to GCS: {gcs_error}")
                
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
