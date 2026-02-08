#!./venv/bin/python3
from google.cloud import storage

# Create a client to interact with Google Cloud Storage
client = storage.Client()

testing_enabled = True  # Set to True to analyze a smaller bucket for testing purposes, False to analyze the full bucket

def analyze_bucket(bucket_name):
    # Get the bucket object
    bucket = client.get_bucket(bucket_name)
    
    # If testing is enabled, analyze a smaller bucket for faster results
    if testing_enabled:
        # Get only the first 10 blobs for testing
        blobs = bucket.list_blobs(max_results=10)
    else:
        # List all blobs (objects) in the bucket
        blobs = bucket.list_blobs()

    # Analyze the blobs and print their names and sizes
    for blob in blobs:
        print(f"Blob Name: {blob.name}, Size: {blob.size} bytes")


if __name__ == "__main__":
    analyze_bucket('cs528-adithyav-hw2')