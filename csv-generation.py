from elasticsearch import Elasticsearch, exceptions
import pandas as pd
import csv
from datetime import datetime

# Define Elasticsearch host, credentials, and certificate path
es_host = "https://elastic.wavelet.com:9443"
es_username = "eck-admin"
es_password = "password"
es_certificate_path = "eck.pem"  # Replace with the actual path
index_pattern = "index_name*"
start_time = "2024-01-10T00:00:00"  # Replace with your desired start time
end_time = "2024-02-10T12:20:00"    # Replace with your desired end time

# Convert time strings to Elasticsearch-compatible format
start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").isoformat()
end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S").isoformat()

# Initialize Elasticsearch client with authentication and certificate
es = Elasticsearch([es_host], http_auth=(es_username, es_password), scheme="https", port=443, ca_certs=es_certificate_path)

# Check Elasticsearch cluster health for connectivity
def check_elasticsearch_health():
    try:
        health = es.cluster.health()
        print(f"Elasticsearch Cluster Health: {health['status']}")
        return True
    except exceptions.TransportError as e:
        print(f"Failed to connect to Elasticsearch: {e}")
        return False

# Perform the health check before executing further operations
if check_elasticsearch_health():
    # Scroll API to retrieve data from a specific index pattern and time range
    scroll_size = 2000
    scroll_timeout = "2m"
    scroll_id = None

    # Process and save results in chunks using pandas DataFrame
    chunk_size = 1000  # Adjust based on your needs
    csv_filename = "output.csv"
    
    # Use a generator function to yield chunks of results
    def scroll_search(index, query_body, scroll_size, scroll_timeout):
        try:
            results = es.search(index=index, scroll=scroll_timeout, size=scroll_size, body=query_body)
            while results["hits"]["hits"]:
                yield results["hits"]["hits"]
                results = es.scroll(scroll_id=results["_scroll_id"], scroll=scroll_timeout)
        except exceptions.TransportError as e:
            print(f"Error during scroll search: {e}")
        finally:
            es.clear_scroll(scroll_id=results["_scroll_id"])  # Clear the scroll to release resources

    # Define the function to convert chunks to a DataFrame and save to CSV
    def process_and_save_chunk(chunk, csv_writer):
        data = []
        for result in chunk:
            data.append(result["_source"])
        df = pd.DataFrame(data)
        df.to_csv(csv_writer, index=False, header=False, mode="a")

    # Write header
    header_written = False

    # Process and write data in chunks
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)
        
        query_body = {
                    "query": {
                        "bool": {
                            "must": [
                                {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                            ]
                        }
                    }
                }

        for chunk in scroll_search(index=index_pattern, query_body=query_body, scroll_size=scroll_size, scroll_timeout=scroll_timeout):
            if not header_written:
                header = list(chunk[0]["_source"].keys())
                csv_writer.writerow(header)
                header_written = True

            # Process and write data for each document in the chunk
            for result in chunk:
                csv_writer.writerow(list(result["_source"].values()))
                
        print(f"CSV file '{csv_filename}' created successfully.")

    # Close the Elasticsearch connection when done
    es.transport.close()
