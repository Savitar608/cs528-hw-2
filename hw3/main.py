# Google cloud functions framework for defining the HTTP handler
import functions_framework

# Google cloud libraries for Storage, Logging, and Pub/Sub
from google.cloud import storage
from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
import google.api_core.exceptions

# json library for message formatting in Pub/Sub
import json

# Initialize clients for Storage, Logging, and Pub/Sub
storage_client = storage.Client()
logging_client = cloud_logging.Client()
logger = logging_client.logger("hw3-microservice-logs") # Custom log name
publisher = pubsub_v1.PublisherClient()

# Configuration: Bucket name and Pub/Sub topic path
BUCKET_NAME = "cs528-adithyav-hw2"
TOPIC_PATH = "projects/main-tokenizer-486322-e1/topics/hw3-topic-forbidden-files"

@functions_framework.http
def get_file_from_bucket(request):
    """
    HTTP Cloud Function to retrieve files from GCS.
    """
    
    # 1. Enforce HTTP Method (Only GET allowed)
    if request.method != 'GET':
        error_msg = f"Method {request.method} not allowed."
        
        print(f"ERROR: {error_msg}") 
        
        # Structured Logging
        logger.log_struct(
            {"message": error_msg, "method": request.method, "status": 501},
            severity="ERROR"
        )
        
        return "Not Implemented", 501

    # 2. Parse the bucket name from the path
    bucket = request.path.split('/')[0] if len(request.path.split('/')) > 1 else None
    
    # 3. Parse the filename from the path
    # If bucket is specified, the filename is the rest of the path after the bucket name
    # If bucket is not specified, it is a local request and the filename is the entire path
    # and has to be fetched from local storage
    filename = request.path[len(bucket)+1:] if bucket else request.path[1:]
    
    # Handle the case where bucket is not specified (local storage)
    if not bucket:
        # open the file from local storage
        try:
            with open(filename, 'r') as f:
                contents = f.read()
                return contents, 200
        except FileNotFoundError:
            error_msg = f"File {filename} not found in local storage."
            
            print(f"ERROR: {error_msg}")
            
            # Structured Logging
            logger.log_struct(
                {"message": error_msg, "file": filename, "status": 404},
                severity="WARNING"
            )
            
            return "Specified file not found in local storage", 404
        except Exception as e:
            print(f"CRITICAL: {e}")
            logger.log_struct(
                {"message": str(e), "file": filename, "status": 500},
                severity="CRITICAL"
            )
            return "Internal Server Error", 500
    
    if not filename:
        return "Please specify a file path", 400

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)

    # 3. Try to fetch the file
    try:
        if not blob.exists():
            error_msg = f"File {filename} not found in bucket {BUCKET_NAME}."
            
            # Simple print statement
            print(f"ERROR: {error_msg}")
            
            # Structured Logging
            logger.log_struct(
                {"message": error_msg, "file": filename, "status": 404},
                severity="WARNING"
            )

            # Check your assignment details for "below".
            message_json = json.dumps({"event": "file_not_found", "file": filename})
            message_bytes = message_json.encode('utf-8')
            future = publisher.publish(TOPIC_PATH, message_bytes)
            future.result() # Wait for publish confirmation
            
            return "Specified file not found in bucket", 404

        # 4. Success Case: Read the file
        contents = blob.download_as_text()
        return contents, 200

    except google.api_core.exceptions.NotFound:
        logger.log_struct(
            {"message": f"File {filename} not found in bucket {BUCKET_NAME}.", "file": filename, "status": 404},
            severity="WARNING"
        )
        return "Specified file not found in bucket", 404
    except Exception as e:
        # Catch-all for other errors (permissions, connection issues)
        print(f"CRITICAL: {e}")
        logger.log_struct(
            {"message": str(e), "file": filename, "status": 500},
            severity="CRITICAL"
        )
        return "Internal Server Error", 500