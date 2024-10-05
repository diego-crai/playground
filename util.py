import os
from azure.storage.blob import BlobServiceClient
from scrapingant_client import ScrapingAntClient
from urllib.parse import urlparse
from datetime import datetime
import time
import requests

CONTAINER_SCRAPER = "scraping"

def download_html_and_upload_to_blob_scrapingant(url, name, container_name=CONTAINER_SCRAPER):
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
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Fetch HTML content from the URL
            result = client.general_request(url)
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(0.02)  # Short delay before retrying
            else:
                raise Exception(f"Failed to fetch the page after {max_retries} attempts. Error: {e}")
    
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


def download_html_and_upload_to_blob(url, name, container_name=CONTAINER_SCRAPER):
    """
    Downloads the HTML content of the given URL using requests and uploads it directly to Azure Blob Storage.

    Parameters:
    - url (str): The URL of the page to scrape.
    - container_name (str): The name of the container in Blob Storage.
    - blob_name (str): The name of the blob (file) in Blob Storage.

    Returns:
    - None
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Fetch HTML content from the URL
            result = requests.get(url)
            result.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
            break
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(0.02)  # Short delay before retrying
            else:
                raise Exception(f"Failed to fetch the page after {max_retries} attempts. Error: {e}")
    
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