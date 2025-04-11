import asyncio
import logging
from typing import List, Dict, Any

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler
from crawlee.models import Request
from crawlee.storages import KeyValueStore, RequestQueue # Import RequestQueue if needed explicitly

# Configure logging for Crawlee
logging.basicConfig(level=logging.INFO) # See INFO level logs from Crawlee
# logging.basicConfig(level=logging.DEBUG) # Use DEBUG for more verbose output

class QuotesCrawler(BeautifulSoupCrawler):
    """
    A simple crawler to scrape quotes from quotes.toscrape.com.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scraped_items_count = 0

    async def request_handler(self, request: Request, soup: Any) -> None:
        """
        This function is called for each page the crawler visits.
        It extracts data and enqueues links to follow.

        Args:
            request: The Request object representing the current URL being processed.
            soup: A BeautifulSoup object representing the parsed HTML of the page.
        """
        self.log.info(f"Processing page: {request.url}")

        # --- Data Extraction ---
        extracted_data: List[Dict[str, Any]] = []
        quote_elements = soup.select('div.quote') # Select all quote containers

        if not quote_elements:
            self.log.warning(f"No quotes found on page: {request.url}")
            return

        for quote_div in quote_elements:
            text = quote_div.select_one('span.text')
            author = quote_div.select_one('small.author')
            tags = quote_div.select('a.tag')

            if text and author:
                item = {
                    'text': text.text.strip(),
                    'author': author.text.strip(),
                    'tags': [tag.text.strip() for tag in tags]
                }
                extracted_data.append(item)
                self.scraped_items_count += 1
            else:
                 self.log.warning(f"Could not extract complete quote data on {request.url}")

        # --- Save Data ---
        # Push data to the default dataset associated with the crawl run.
        # Crawlee automatically handles saving this to storage (e.g., JSON files).
        if extracted_data:
            await self.push_data(extracted_data) # Can push a list of items
            self.log.info(f"Successfully scraped {len(extracted_data)} quotes from {request.url}")

        # --- Link Enqueuing ---
        # Find the 'Next' button/link and add its URL to the request queue.
        # Crawlee handles deduplication automatically.
        # We use enqueue_links to automatically find and enqueue based on a selector.
        await self.enqueue_links(selector='li.next > a') # Selector for the 'Next →' link


async def main():
    """
    Main function to configure and run the crawler.
    """
    # --- Configuration ---
    # Optional: Explicitly configure storage location (defaults work fine too)
    # from crawlee.configuration import Configuration
    # config = Configuration(storage_path='./my_crawler_storage')

    crawler = QuotesCrawler(
        # start_urls: Where the crawler begins.
        start_urls=["http://quotes.toscrape.com/"],

        # max_requests_per_crawl: Limit the number of pages visited (good for testing).
        max_requests_per_crawl=10, # Limit to first 10 pages (including start url)

        # Optional: Set concurrency (how many requests run in parallel)
        # min_concurrency=1,
        # max_concurrency=5,

        # Optional: Configure request delays if needed (to be polite to servers)
        # default_request_timeout_secs=30,
        # navigation_timeout_secs=60,

        # Optional: Pass other configuration if needed
        # configuration=config # Pass explicit config if created
    )

    # --- Run the Crawler ---
    print("Starting the crawler...")
    await crawler.run()
    print("Crawler finished.")
    print(f"Total quotes scraped: {crawler.scraped_items_count}")
    print("Data saved in: ./storage/datasets/default") # Default location

if __name__ == "__main__":
    # Ensure Playwright browsers are installed if not already (required by Crawlee internals)
    # You might need to run 'playwright install' in your terminal once.
    asyncio.run(main())