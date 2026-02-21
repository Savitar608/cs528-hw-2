# Google cloud functions framework for defining the HTTP handler
from flask import Request

import functions_framework

# Google cloud libraries for Storage, Logging, and Pub/Sub
from google.cloud import storage
from google.cloud import pubsub_v1

# Exception handling for Google Cloud Storage operations
import google.api_core.exceptions

# json library for message formatting in Pub/Sub
import json

# Initialize clients for Storage, Logging, and Pub/Sub
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

# Bucket name
BUCKET_NAME = "cs528-adithyav-hw2"

# Configuration: Bucket name and Pub/Sub topic path
TOPIC_PATH = "projects/main-tokenizer-486322-e1/topics/hw3-forbidden-files"

@functions_framework.http
def get_file_from_bucket(request: Request):
    """
    HTTP Cloud Function to retrieve files from GCS.
    """
    
    # 1. Enforce HTTP Method (Only GET allowed)
    if request.method != 'GET':
        error_msg = f"Method {request.method} not allowed."
        
        # Structured Logging
        print(json.dumps({
            "severity": "ERROR",
            "message": error_msg,
            "method": request.method,
            "status": 501
        }))
        
        return "Not Implemented", 501
    
    # 2. Parse the file name from the URL path
    filename = request.path.lstrip('/').split('/')[-1] # Remove leading slash and get the last part

    if not filename:
        return "Please specify the file name in the URL path", 400

    print(json.dumps({
        "severity": "INFO",
        "message": f"Received {request.method} request for path: {request.path}",
        "filename": filename
    }))
    
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)

    # 3. Try to fetch the file
    try:
        if not blob.exists():
            error_msg = f"File {filename} not found in bucket {bucket.name}."
            
            # Simple print statement
            print(f"ERROR: {error_msg}")
            
            # Structured Logging
            print(json.dumps({
                "severity": "WARNING",
                "message": error_msg,
                "file": filename,
                "status": 404
            }))

            # Check your assignment details for "below".
            message_json = json.dumps({"event": "file_not_found", "file": filename, "bucket": bucket.name})
            message_bytes = message_json.encode('utf-8')
            future = publisher.publish(TOPIC_PATH, message_bytes)
            future.result() # Wait for publish confirmation
            print(f"INFO: Published message to Pub/Sub topic {TOPIC_PATH} about missing file {filename}.")
            
            return "Specified file not found in bucket", 404

        # 5. Success Case: Read the file
        contents = blob.download_as_text()
        return contents, 200

    # 6. Handle specific exceptions for not found and other errors
    except google.api_core.exceptions.NotFound:
        print(json.dumps({
            "severity": "WARNING",
            "message": f"File {filename} not found in bucket {bucket.name}.",
            "file": filename, 
            "status": 404
        }))
        return "Specified file not found in bucket", 404
    # 7. Catch-all for other exceptions (permissions, connection issues, etc.)
    except Exception as e:
        print(f"CRITICAL: {e}")
        print(json.dumps({
            "severity": "CRITICAL",
            "message": str(e), 
            "file": filename, 
            "status": 500
        }))
        return "Internal Server Error", 500