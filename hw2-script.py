#!./venv/bin/python3
from google.cloud import storage
from bs4 import BeautifulSoup

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
        
        # Only analyze HTML files
        if not blob.name.endswith('.html'):
            continue
        
        content = blob.download_as_text()
        
        # parsing with BeautifulSoup with lxml parser
        soup = BeautifulSoup(content, 'lxml')
        
        # Extract the outlinks from the HTML content
        references = soup.find_all('a', href=True)
        outlinks = [a['href'] for a in references]
        
        print(f"Outlinks in {blob.name}:")
        for outlink in outlinks:
            print(f"  {outlink}")

if __name__ == "__main__":
    analyze_bucket('cs528-adithyav-hw2')