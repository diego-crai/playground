import os
import urllib.parse
import requests
from lxml import etree
from io import StringIO
import dotenv
dotenv.load_dotenv()
# ScrapingAnt API key from environment variables
SCRAPINGANT_API_KEY = os.getenv("SCRAPINGANT_API_KEY")
if not SCRAPINGANT_API_KEY:
    raise EnvironmentError("SCRAPINGANT_API_KEY environment variable not set.")

# ScrapingAnt API endpoint
SCRAPINGANT_API_URL = "https://api.scrapingant.com/v2/general"

# Function to fetch page content using ScrapingAnt
def scrapingant_request(url, api_key, params=None, max_retries=3):
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key
    }
    if params is None:
        params = {}
    params.update({"url": url, "browser": True})  # Enable browser rendering if needed

    for attempt in range(max_retries):
        try:
            response = requests.get(SCRAPINGANT_API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"ScrapingAnt error on attempt {attempt + 1} for {url}: {str(e)}")
            if attempt < max_retries - 1:
                import time
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    print(f"Failed to retrieve content for {url} after {max_retries} attempts.")
    return None

# Parse HTML content using lxml
def parse_html(content):
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(content), parser)
    return tree

# Function to find product page URLs from a given URL
def find_product_pages(url, api_key):
    print(f"Fetching products from URL: {url}")
    html_content = scrapingant_request(url, api_key)
    if not html_content:
        print(f"Failed to retrieve content for URL: {url}")
        return []

    tree = parse_html(html_content)

    # XPath to find the products container
    products_container = tree.xpath('//div[@id="productlistcontainer"]')
    if not products_container:
        print(f"Products container not found for URL: {url}")
        return []

    # Extract all product items; assuming 'li-name' is a custom attribute
    product_items = products_container[0].xpath('.//li[@li-name]')
    if not product_items:
        print(f"No product items found for URL: {url}")
        return []

    product_urls = []
    for item in product_items:
        href = item.xpath('.//a/@href')
        if not href:
            continue
        full_url = urllib.parse.urljoin('https://www.sportsdirect.com', href[0])
        product_urls.append(full_url)

    return product_urls

# Main function to run the scraper
def find_products_in_url(input_url):
    # Get URL input from the user
    url = input_url
    if not url:
        print("No URL provided. Exiting.")
        return

    # Find product pages
    product_urls = find_product_pages(url, SCRAPINGANT_API_KEY)

    # Display the results
    if product_urls:
        print(f"\nFound {len(product_urls)} product URLs:")
        for idx, product_url in enumerate(product_urls, start=1):
            print(f"{idx}. {product_url}")
    else:
        print("No product URLs found.")
    return product_urls

