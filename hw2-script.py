#!./venv/bin/python3
from collections import defaultdict
from time import time
from google.cloud import storage
from bs4 import BeautifulSoup
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import argparse
import os

testing_enabled = False  # Set to True to analyze a smaller bucket for testing purposes, False to analyze the full bucket


# Specify the bucket name
BUCKET_NAME = 'cs528-adithyav-hw2'

class LocalBlob:
    def __init__(self, full_path, base_folder):
        self.path = full_path
        parent_dir = os.path.dirname(os.path.abspath(base_folder))
        rel_path = os.path.relpath(full_path, parent_dir)
        self.name = rel_path.replace(os.sep, '/')

    def download_as_text(self):
        with open(self.path, 'r', encoding='utf-8') as f:
            return f.read()


# Formula from assignment: PR(A) = 0.15/n + 0.85 (PR(T1)/C(T1) + … +PR(Tn)/C(Tn))
# where PR(X) is the pagerank of a page X, T1..Tn are all the pages pointing to page X, and C(X) is the number of outgoing links that page X has.
# From this, d is 0.85, and (1-d)/n is 0.15/n
def compute_page_rank(nodes, edges, d=0.85, tol=0.005) -> dict:
    '''
    Calculated using the iterative formula: PR(X) = (1-d)/n + d * (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
    where T1...Tn are the pages linking to page X,
    C(Ti) is the number of outbound links on page Ti
    Args:
        nodes (list): A list of node identifiers (e.g., blob names).
        edges (dict): A dictionary where keys are node identifiers and values are lists of outbound links from the corresponding node.
        d (float): Damping factor (set to 0.85 by default).
        tol (float): Tolerance level for convergence (set to 0.005 by default).
    Returns:
        dict: A dictionary mapping each node to its computed PageRank value.
    '''
    n = len(nodes)
    
    # Initialize PageRank values
    page_ranks = { node: 1.0 / n for node in nodes }
        
    converged = False
    iteration = 0
    
    # Precompute outbound link counts
    outbound_counts = { node: len(edges[node]) for node in nodes }
    
    # Build incoming links mapping
    incoming_links = defaultdict(list)
    for node in nodes:
        for outlink in edges[node]:
            incoming_links[outlink].append(node)

    # Iterate until convergence
    while not converged:
        # Initialize new PageRank values
        new_page_ranks = {}
        
        for node in nodes:
            # Calculate the sum of PageRank contributions from incoming links
            incoming_sum = 0
            for incoming_node in incoming_links[node]:
                if outbound_counts[incoming_node] > 0:
                    incoming_sum += page_ranks[incoming_node] / outbound_counts[incoming_node]
            
            # Update PageRank value using the formula
            new_rank = (1 - d) / n + d * incoming_sum
            new_page_ranks[node] = new_rank
            
        # Increment iteration count
        iteration += 1

        # Calculate the sum of all page ranks
        total_rank = sum(new_page_ranks.values())
        
        # Calculate the previous total rank for normalization
        previous_total_rank = sum(page_ranks.values())
        
        
        # If the change in total rank is less than 0.5%, we stop iterating
        if abs(total_rank - previous_total_rank) / previous_total_rank < tol:
            print(f"Iteration {iteration}: Total PageRank sum changed from {previous_total_rank} to {total_rank}")
            converged = True
        
        page_ranks = new_page_ranks
            
    return page_ranks
        


def get_stats(data):
    '''Calculate and return statistics for a given list of data.
    Args:
        data (list): A list of numerical values for which statistics are to be calculated.
    Returns:
        dict: A dictionary containing the average, median, maximum, minimum, and quintiles of the input data.
    '''
    if not data: return {}
    quintiles = np.percentile(data, [20, 40, 60, 80])
    
    return {
        "Avg": np.mean(data),
        "Median": np.median(data),
        "Max": np.max(data),
        "Min": np.min(data),
        "Quintiles": [f"{x:g}" for x in quintiles]
    }


def process_blob_content(args):
    """Worker function for ThreadPool."""
    name, content = args
    try:
        soup = BeautifulSoup(content, 'lxml')
        outlinks = [a['href'] for a in soup.find_all('a', href=True)]
        return name, outlinks
    except Exception as e:
        return name, []


def analyze_bucket(source, is_local=False):
    '''Analyze the specified bucket or local folder and compute the statistics for in-degrees and out-degrees.
    Args:
        source (str): The name of the bucket or path to local folder.
        is_local (bool): Whether to read from local file system.
    This function retrieves the blobs (objects) from the specified bucket or folder, extracts the outlinks from HTML files, and computes the in-degrees and out-degrees for each blob. It then calculates and prints the statistics for both in-degrees and out-degrees.
    '''
    start_time = time.time()
    
    # Dictionary to store the in-count of each target URL
    in_count = defaultdict(int)
    
    # Dictionary to store the edges (outlinks) for each blob
    edges = defaultdict(list)
    
    # List of nodes (blobs) in the bucket
    nodes = []
    
    blobs = []
    
    if is_local:
        if not os.path.exists(source):
            print(f"Error: Path '{source}' does not exist.")
            return

        print(f"Reading from local folder: {source}")
        all_files = []
        for root, _, files in os.walk(source):
             for file in files:
                  if file.endswith('.html'):
                       all_files.append(os.path.join(root, file))
        
        all_files.sort()

        if testing_enabled:
            all_files = all_files[:10]
            
        for fpath in all_files:
            blobs.append(LocalBlob(fpath, source))
            
    else:
        # Create a client to interact with Google Cloud Storage
        client = storage.Client()
        
        # Get the bucket object
        bucket = client.get_bucket(source)
    
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
        
        # Add the outlinks to the links list
        links = []
        for outlink in outlinks:
            # Fix for relative paths and keys mismatch
            if '/' in blob.name and not outlink.startswith('http'):
                 directory = blob.name.rsplit('/', 1)[0]
                 if not outlink.startswith('/'):
                     outlink = f"{directory}/{outlink}"

            links.append(outlink)
            
            # Increment the in-count for the target URL
            in_count[outlink] +=1
        
        # Store the edges (outlinks) for the current blob
        edges[blob.name] = links
        
        # Add the blob name to the list of nodes
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
        
    # Compute PageRank values
    page_ranks = compute_page_rank(nodes, edges)
    
    # Calculate the top 5 PageRank values
    top_5_pageranks = sorted(page_ranks.items(), key=lambda x: x[1], reverse=True)[:5]
    
    print("--- Top 5 Pages by PageRank ---")
    for page, score in top_5_pageranks:
        print(f"{page}: {score:.6f}")
        
    print(f"\nTotal Execution Time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze PageRank on HTML files.")
    parser.add_argument('--local', action='store_true', help="Run in local mode reading from disk.")
    parser.add_argument('source', nargs='?', default=BUCKET_NAME, help="Bucket name or local folder path.")
    
    args = parser.parse_args()
    
    analyze_bucket(args.source, is_local=args.local)