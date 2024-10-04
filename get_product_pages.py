import os
import urllib.parse
import logging
import time
from datetime import datetime
from lxml import etree
from io import StringIO
from scrapingant_client import ScrapingAntClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logging.info('Imported libraries in scraper')

# ScrapingAnt API details
SCRAPINGANT_API_KEY = "563c23070e6e4d6ea38beedb892e0d7f"  # Replace with your actual API key

# Root URL
ROOT_URL = "https://www.sportsdirect.com/football/all-football"

# ScrapingAnt request function with retry
def scrapingant_request(url, client, max_retries=3):
    for attempt in range(max_retries):
        try:
            # Use the synchronous request
            result = client.general_request(url, browser=True)
            return result.content
        except Exception as e:
            logging.error(f"ScrapingAnt error on attempt {attempt + 1} for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    return None

# Parse HTML content using lxml
def parse_html(content):
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(content), parser)
    return tree

# Get page URL function
def get_page_url(page):
    return f"{ROOT_URL}#dcp={page}&dppp=59&OrderBy=rank"

# Get page URLs function
def get_page_urls(client):
    html_content = scrapingant_request(ROOT_URL, client)
    if not html_content:
        logging.error("Failed to retrieve content for root URL")
        return {}

    tree = parse_html(html_content)

    # XPath to find the pagination div
    pagination = tree.xpath('//div[@id="divPagination"]')
    if not pagination:
        logging.error("Pagination div not found")
        return {}

    # Extract all page links with class 'swipeNumberClick'
    page_links = pagination[0].xpath('.//a[contains(@class, "swipeNumberClick")]/text()')
    if not page_links:
        logging.error("No pagination links found")
        return {}

    try:
        max_page = int(page_links[-1].strip())
    except ValueError:
        logging.error("Failed to parse the maximum page number")
        return {}

    pages = range(1, max_page + 1)
    return {page: get_page_url(page) for page in pages}

# Main function to run the scraper and get page URLs
def main():
    if not SCRAPINGANT_API_KEY:
        logging.error("SCRAPINGANT_API_KEY environment variable is not set.")
        return

    client = ScrapingAntClient(token=SCRAPINGANT_API_KEY)

    logging.info('Fetching page URLs...')
    page_urls = get_page_urls(client)
    if page_urls:
        logging.info("Page URLs retrieved successfully:")
        for page, url in page_urls.items():
            print(f"Page {page}: {url}")
    else:
        logging.error("No page URLs found.")
    return page_urls

if __name__ == "__main__":
    main()
