#!./venv/bin/python3
from collections import defaultdict
import time
from google.cloud import storage
from bs4 import BeautifulSoup
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import argparse
import os

testing_enabled = True  # Set to True to analyze a smaller bucket for testing purposes, False to analyze the full bucket


# Specify the bucket name
BUCKET_NAME = 'cs528-adithyav-hw2'

class LocalBlob:
    """A simple class to mimic GCS Blob behavior for local files."""
    def __init__(self, full_path, base_folder):
        self.path = full_path
        self.name = os.path.relpath(full_path, base_folder).replace(os.sep, '/')
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

        # Calculate the average error percentage across all nodes to check for convergence
        average_error = np.mean([abs(new_page_ranks[node] - page_ranks[node])/page_ranks[node] for node in nodes])
        print(f"Iteration {iteration}: Average PageRank error = {average_error:.6f}")
        
        # If the average error is below the tolerance level, we consider it converged
        if average_error < tol:
            print(f"Iteration {iteration}: Convergence achieved with average error {average_error:.6f}")
            converged = True
        
        page_ranks = new_page_ranks
        
    # Test if the sum of PageRank values is approximately 1.0 (as expected in a closed system)
    total_rank = sum(page_ranks.values())
    print(f"Total PageRank sum: {total_rank:.6f} (should be close to 1.0)")
    if abs(total_rank - 1.0) > 0.01:
        print("Warning: Total PageRank sum is not close to 1.0, which may indicate an issue with the graph structure or convergence.")
            
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
    
def download_helper(blob):
    """Worker function for ThreadPool.
    Args:
        blob: A blob object from GCS.
    Returns:
        tuple: A tuple containing the blob name and its content as text, or None if the blob is not an HTML file.
    """
    if not blob.name.endswith('.html'): return None
    return (blob.name, blob.download_as_text())


def analyze_bucket(source, is_local=False):
    '''Analyze the specified bucket or local folder and compute the statistics for in-degrees and out-degrees.
    Args:
        source (str): The name of the bucket or path to local folder.
        is_local (bool): Whether to read from local file system.
    This function retrieves the blobs (objects) from the specified bucket or folder, extracts the outlinks from HTML files, and computes the in-degrees and out-degrees for each blob. It then calculates and prints the statistics for both in-degrees and out-degrees.
    '''
    start_time = time.time()
    
    # 1. Get the data
    blobs_to_process = []

    if is_local:
        print(f"Scanning local folder: {source}")
        for root, _, files in os.walk(source):
            for file in files:
                if file.endswith('.html'):
                    path = os.path.join(root, file)
                    blob = LocalBlob(path, source)
                    blobs_to_process.append((blob.name, blob.download_as_text()))
    else:
        print(f"Connecting to GCS Bucket: {source}")
        client = storage.Client()
        bucket = client.bucket(source)
        
        all_blobs = list(bucket.list_blobs(max_results=10 if testing_enabled else None))
        
        print(f"Downloading {len(all_blobs)} files (Threaded)...")

        with ThreadPoolExecutor(max_workers=50) as downloader:
            results = list(downloader.map(download_helper, all_blobs))
            blobs_to_process = [r for r in results if r]

    # 2. Parse HTML with multiple threads
    print("Parsing HTML content...")
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as parser:
        parsed_results = list(parser.map(process_blob_content, blobs_to_process))

    # 3. Build Graph
    nodes = []
    edges = {}
    in_counts = defaultdict(int)
    
    for name, links in parsed_results:
        nodes.append(name)
        edges[name] = links
        for link in links:
            in_counts[link] += 1

    # 4. Compute Stats
    out_degrees = [len(edges[n]) for n in nodes]
    in_degrees = [in_counts[n] for n in nodes]

    print("\n--- Statistics ---")
    outgoing_links_stats = get_stats(out_degrees)
    incoming_links_stats = get_stats(in_degrees)
    
    for stat_name, stats in [("Outgoing Links", outgoing_links_stats), ("Incoming Links", incoming_links_stats)]:
        print(f"\n{stat_name}:")
        for k, v in stats.items():
            if k == "Quintiles":
                print(f"  {k}: {', '.join(v)}")
            else:
                print(f"  {k}: {v:.2f}" if isinstance(v, float) else f"  {k}: {v}")
                
    print("\n--- Page rank computation ---")
    
    # 5. Compute PageRank values
    page_ranks = compute_page_rank(nodes, edges)
    
    # 6. Calculate the top 5 PageRank values
    top_5_pageranks = sorted(page_ranks.items(), key=lambda x: x[1], reverse=True)[:5]
    
    print("\n--- Top 5 Pages by PageRank ---")
    for page, score in top_5_pageranks:
        print(f"{page}: {score:.6f}")
        
    print(f"\nTotal Execution Time: {time.time() - start_time:.2f} seconds")


def test_independent_correctness():
    """
    Verifies PageRank logic on a known 3-node graph.
    A -> B
    B -> C
    C -> A
    Should converge to equal ranks (0.333) due to symmetry.
    """
    print("\n--- Running Independent Correctness Test ---")
    nodes = ['A', 'B', 'C']
    edges = {'A': ['B'], 'B': ['C'], 'C': ['A']}
    
    # Run logic
    pr = compute_page_rank(nodes, edges, tol=0.001)
    
    # Check results
    assert abs(pr['A'] - pr['B']) < 0.01, "Symmetric graph should have equal ranks"
    assert abs(sum(pr.values()) - 1.0) < 0.01, "Closed loop sum should be ~1.0"
    print("✅ Test Passed: Logic is correct on control graph.\n")


if __name__ == "__main__":
    test_independent_correctness()
    
    parser = argparse.ArgumentParser(description="Analyze PageRank on HTML files.")
    parser.add_argument('--local', action='store_true', help="Run in local mode reading from disk.")
    parser.add_argument('source', nargs='?', default=BUCKET_NAME, help="Bucket name or local folder path.")
    
    args = parser.parse_args()
    
    analyze_bucket(args.source, is_local=args.local)