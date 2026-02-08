#!./venv/bin/python3
from collections import defaultdict
from google.cloud import storage
from bs4 import BeautifulSoup
import numpy as np

# Create a client to interact with Google Cloud Storage
client = storage.Client()

testing_enabled = True  # Set to True to analyze a smaller bucket for testing purposes, False to analyze the full bucket


def get_stats(data):
        return {
            "Avg": np.mean(data),
            "Median": np.median(data),
            "Max": np.max(data),
            "Min": np.min(data),
            "Quintiles": np.percentile(data, [20, 40, 60, 80])
        }

def analyze_bucket(bucket_name):
    # Dictionary to store the in-count of each target URL
    in_count = defaultdict(int)
    
    # Dictionary to store the edges (outlinks) for each blob
    edges = defaultdict(list)
    
    # List of nodes (blobs) in the bucket
    nodes = []
    
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
        # print(f"Blob Name: {blob.name}, Size: {blob.size} bytes")
        
        # Only analyze HTML files
        if not blob.name.endswith('.html'):
            continue
        
        content = blob.download_as_text()
        
        # parsing with BeautifulSoup with lxml parser
        soup = BeautifulSoup(content, 'lxml')
        
        # Extract the outlinks from the HTML content
        references = soup.find_all('a', href=True)
        outlinks = [a['href'] for a in references]
        
        links = []
        # Add the outlinks to the links list
        for outlink in outlinks:
            links.append(outlink)
            in_count[outlink] +=1
            
        edges[blob.name] = links
        nodes.append(blob.name)
    
    # Create lists of out-degrees and in-degrees for each node
    # To be processed for statistics
    out_degrees = [len(edges[node]) for node in nodes]
    in_degrees = [in_count[node] for node in nodes]
    
    # Print the statistics for out-degrees and in-degrees
    print("\nOut-Degree Statistics:")
    out_stats = get_stats(out_degrees)
    for stat, value in out_stats.items():
        print(f"{stat}: {value}")
        
    print("\nIn-Degree Statistics:")
    in_stats = get_stats(in_degrees)
    for stat, value in in_stats.items():
        print(f"{stat}: {value}")

if __name__ == "__main__":
    analyze_bucket('cs528-adithyav-hw2')