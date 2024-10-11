import os
from azure.storage.blob import BlobServiceClient
from scrapingant_client import ScrapingAntClient
from urllib.parse import urlparse
from datetime import datetime, timedelta
import requests

CONTAINER_SCRAPER = "scraping"

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
    # Get the current datetime and add 1 hour
    current_datetime = datetime.now() + timedelta(hours=1)
    # Format the new date (next day if the time was 23:00)
    date = current_datetime.strftime("%Y-%m-%d")
    return f"{domain_name}/{date}/{name}.html"

def upload_to_blob(content, url, name, container_name=CONTAINER_SCRAPER):
    """
    Uploads content directly to Azure Blob Storage.

    Parameters:
    - content (bytes): The content to upload.
    - url (str): The URL of the page (used for blob naming).
    - name (str): Additional identifier for the blob.
    - container_name (str): The name of the container in Blob Storage.

    Returns:
    - str: Success or error message.
    """
    try:
        # Retrieve the Azure Blob Storage connection string from environment variables
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        if not connection_string:
            return "AZURE_STORAGE_CONNECTION_STRING environment variable not found."
        
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
        
        # Upload the content directly to Azure Blob Storage
        blob_client.upload_blob(content, overwrite=False)
        return f"Content from {url} has been uploaded to blob '{blob_name}' in container '{container_name}'."
    except Exception as e:
        if "BlobAlreadyExists" in str(e):
            return f"Blob '{blob_name}' already exists in container '{container_name}'."
        else:
            return f"An error occurred during upload: {e}"

def download_html_and_upload_to_blob_scrapingant(url, name, container_name=CONTAINER_SCRAPER):
    """
    Downloads the HTML content of the given URL using ScrapingAnt and uploads it directly to Azure Blob Storage.

    Parameters:
    - url (str): The URL of the page to scrape.
    - name (str): Additional identifier for the blob.
    - container_name (str): The name of the container in Blob Storage.

    Returns:
    - str: Success or error message.
    """
    try:
        # Initialize ScrapingAnt client
        client = ScrapingAntClient(token=os.getenv('SCRAPINGANT_API_KEY'))
        
        # Fetch HTML content from the URL
        result = client.general_request(url)
        
        if result.status_code != 200:
            return f"Failed to fetch the page. Status code: {result.status_code}"
        
        # Upload the content to blob storage
        return upload_to_blob(result.content, url, name, container_name)
    except Exception as e:
        return f"An error occurred during scraping: {e}"

def download_html_and_upload_to_blob(url, name, container_name=CONTAINER_SCRAPER):
    """
    Downloads the HTML content of the given URL using requests and uploads it directly to Azure Blob Storage.

    Parameters:
    - url (str): The URL of the page to scrape.
    - name (str): Additional identifier for the blob.
    - container_name (str): The name of the container in Blob Storage.

    Returns:
    - str: Success or error message.
    """
    try:
        # Fetch HTML content from the URL
        result = requests.get(url)
        result.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
        
        # Upload the content to blob storage
        return upload_to_blob(result.content, url, name, container_name)
    except requests.exceptions.RequestException as e:
        return f"Failed to fetch the page. Error: {e}"
    except Exception as e:
        return f"An error occurred: {e}"

