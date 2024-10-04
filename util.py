import os
from azure.storage.blob import BlobServiceClient
from scrapingant_client import ScrapingAntClient
from urllib.parse import urlparse
from datetime import datetime

CONTAINER_SCRAPER = "scraping"

def download_html_and_upload_to_blob(url, name, container_name=CONTAINER_SCRAPER):
    """
    Downloads the HTML content of the given URL using ScrapingAnt and uploads it directly to Azure Blob Storage.

    Parameters:
    - url (str): The URL of the page to scrape.
    - container_name (str): The name of the container in Blob Storage.
    - blob_name (str): The name of the blob (file) in Blob Storage.

    Returns:
    - None
    """
    # Initialize ScrapingAnt client
    client = ScrapingAntClient(token=os.getenv('SCRAPINGANT_API_KEY'))
    
    # Fetch HTML content from the URL
    result = client.general_request(url)
    
    if result.status_code != 200:
        raise Exception(f"Failed to fetch the page. Status code: {result.status_code}")
    
    # Retrieve the Azure Blob Storage connection string from environment variables
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not found.")
    
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Create the container if it doesn't exist
    try:
        blob_service_client.create_container(container_name)
    except Exception as e:
        print(f"Container may already exist: {e}")
    
    blob_name = make_blob_name(url, name)
    # Get the BlobClient for the target blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    try:
        # Upload the HTML content directly to Azure Blob Storage
        blob_client.upload_blob(result.content, overwrite=False)
    except Exception as e:
        if "BlobAlreadyExists" in str(e):
            return f"Blob '{blob_name}' already exists in container '{container_name}'."
        else:
            raise e
    print(f"HTML content from {url} has been uploaded to blob '{blob_name}' in container '{container_name}'.")

# Example usage:
#download_html_and_upload_to_blob("https://example.com", "html-container", "example_page.html")

def extract_domain_name(url):
    """
    Extracts the domain name from a given URL.
    
    Parameters:
    - url (str): The URL from which to extract the domain name.
    
    Returns:
    - str: The extracted domain name.
    """
    parsed_url = urlparse(url)
    return parsed_url.netloc.split('.')[1]


def make_blob_name(url, name):
    """
    Generates a unique blob name for the given URL.
    
    Parameters:
    - url (str): The URL for which to generate the blob name.
    
    Returns:
    - str: The generated blob name.
    """
    domain_name = extract_domain_name(url)
    date = datetime.now().strftime("%Y-%m-%d")
    return f"{domain_name}/{date}/{name}.html"