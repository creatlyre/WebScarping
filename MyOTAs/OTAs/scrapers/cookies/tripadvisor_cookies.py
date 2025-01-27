# %%

import asyncio
import nest_asyncio
from pyppeteer import connect
import random
import aiohttp
import sys
import pandas as pd
import os
from openpyxl import load_workbook
import time
import datetime
from logger.logger_manager_future_price import LoggerManager

# Allow nested event loops
nest_asyncio.apply()

class TripadvisorCookies:
    def __init__(self, api_key,file_manager, date_today):
        self.api_key = api_key
        european_countries = [
            'dk', 'fr', 'de', 'nl', 'pl', 'sk', 'es', 'gb']
        
        # Choose a random country code from the list for the proxy
        self.proxy_country = random.choice(european_countries)
        self.session_ttl_in_minutes = 15
        self.connection_url = f'wss://browser.zenrows.com?apikey={api_key}&proxy_country={self.proxy_country}&session_ttl={self.session_ttl_in_minutes}m'
        self.date_today = date_today
        self.browser = None
        self.page = None
        self.output_filename = file_manager.file_path_output
        self.currency_code = "EUR"
        self.file_manager = file_manager
        self.df_links = pd.DataFrame()   


        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.html_output_dir = "html_snapshots"
        os.makedirs(self.html_output_dir, exist_ok=True)
        # Logger setup
        self.logger = LoggerManager(file_manager, application="tripadvisor_cookies_collector")
        # Load existing data if file exists
        self.load_urls_to_complete()
        

        # CSS Selectors
        self.css_accept_button = 'button[id="onetrust-accept-btn-handler"]'
        self.css_currency_language_button = 'button[aria-label*="Currency:"]'
        self.css_currency_button = f'span[id*="menu-item-{self.currency_code}"]'
        self.css_total_products = 'div.Ci'
        
    def load_urls_to_complete(self):
        self.df_links = pd.read_csv(self.file_manager.get_file_paths()['link_file'])
         # Filter links based on the 'Run' flag
        self.df_links = self.df_links[self.df_links['Run'] == 1]


    def close_logger(self):
        if self.logger:
            self.logger.close()
            
    async def validate_websocket_url(self, url, timeout=10):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, timeout=timeout):
                    self.logger.logger_info.info("WebSocket URL is valid and accessible.")
                    return True
        except Exception as e:
            self.logger.logger_err.error(f"WebSocket URL validation failed: {e}")
            return False

    async def connect_browser(self):
        try:
            self.browser = await asyncio.wait_for(connect(browserWSEndpoint=self.connection_url), timeout=15)
            self.logger.logger_info.info(f"Successfully connected to the remote browser. Proxy code: {self.proxy_country}")
            self.page = await self.browser.newPage()
            await self.page.setViewport({'width': 1920, 'height': 1080})
            await self.page.evaluate('''() => { document.documentElement.requestFullscreen(); }''')
            # Record the connection start time and session TTL
            self.connection_start_time = time.time()
            self.session_ttl = self.session_ttl_in_minutes * 60  # 15 minutes in seconds
        except asyncio.TimeoutError:
            self.logger.logger_err.error("Connection to the remote browser timed out. Exiting.")
            sys.exit(1)
        except Exception as e:
            self.logger.logger_err.error(f"Failed to connect to the remote browser: {e}")
            sys.exit(1)


    async def scrape_cookies(self):
        # Validate the WebSocket URL
        is_valid_url = await self.validate_websocket_url(self.connection_url)
        if not is_valid_url:
            self.logger.logger_err.error("Invalid or inaccessible WebSocket URL. Exiting.")
            sys.exit(1)

        # Connect to the remote browser
        await self.connect_browser()
        
        for _, row in self.df_links.iterrows():
            self.url = row['URL']
            self.city = row['City']
            self.category = row['Category']

            try:
                # Proces single URL
                await self.process_url()


                self.logger.logger_done.info("Scraping completed successfully.")

            finally:
                # Ensure the browser is closed even if an error occurs
                await self.browser.close()
                self.logger.logger_done.info("Browser connection closed.")
                self.logger.close_logger()
                return True

    async def process_url(self):
        await self.navigate_to_url(self.url)
            
        # Handle cookies
        await self.handle_cookies()

        # Handle currency settings
        await self.handle_currency()
        await self.collect_cookies_and_save()
            # Collect data by iterating through dates and number of adults

    async def collect_cookies_and_save(self):
        all_cookies = await self.page.cookies()  # This returns a list of dicts
            # Filter only cookies for TripAdvisor (or you can keep them all if you want)
        tripadvisor_cookies = [c for c in all_cookies if "tripadvisor" in c['domain']]

            # Convert to a single Cookie header string
        cookie_string = "; ".join([f"{c['name']}={c['value']}" for c in tripadvisor_cookies])

        with open(self.cookies_file_path, "w") as f:
            f.write(cookie_string)

    async def navigate_to_url(self, url_to_collect_data):
        self.logger.logger_info.info(f"Processing URL: {url_to_collect_data}")
        try:
            await self.page.goto(url_to_collect_data, timeout=90000)
            self.logger.logger_info.info(f"Navigated to {url_to_collect_data}")
        except asyncio.TimeoutError:
            self.logger.logger_err.error(f"Navigation to {url_to_collect_data} timed out.")
            await self.take_top_half_screenshot(context_info="Timeout_navigation_to_URL")
            await self.log_html_content(context_info="Timeout_navigation_to_URL")
            return


    async def handle_cookies(self):
        try:
            accept_button = await self.page.querySelector(self.css_accept_button)
            if accept_button:
                await accept_button.click()
                self.logger.logger_info.info("Accepted cookies.")
        except Exception as e:
            self.logger.logger_err.error(f"Cookie accept button not found or failed to click: {e}")
            self.log_html_content("Cookies_not_found")
            self.take_top_half_screenshot('Cookies_not_found')

    async def handle_currency(self):
        try:
            currency_language_button = await self.page.querySelector(self.css_currency_language_button)

            text_content = await self.page.evaluate('(element) => element.textContent', currency_language_button)
            self.logger.logger_info.debug(f"Current currency code is: {text_content}")
            if self.currency_code.lower() not in text_content.lower():
                await currency_language_button.click()
                await asyncio.sleep(random.uniform(1, 4))
                self.logger.logger_info.info("Currency drop down clicked.")
                await self.page.waitForSelector(self.css_currency_button)
                currency_button = await self.page.querySelector(self.css_currency_button)
                await currency_button.click()
                await asyncio.sleep(random.uniform(2, 4))
            else:
                self.logger.logger_err.error("Currency element not found.")
        except Exception as e:
            self.logger.logger_err.error(f"Currency element not found or failed to process: {e}")
            await self.log_html_content("Currency_element_not_found")
            await self.take_top_half_screenshot('Currency_element_not_found')

    async def take_top_half_screenshot(self, context_info=""):
        """
        Takes a screenshot of the top half of the current page and saves it with a timestamp and context info.
        
        :param context_info: Additional information to include in the screenshot filename.
        """
        try:
            # Retrieve the viewport dimensions
            viewport = await self.page.evaluate("""() => {
                return {
                    width: window.innerWidth,
                    height: window.innerHeight
                };
            }""")
            viewport_width = viewport['width']
            viewport_height = viewport['height']
            
            # Define the clipping rectangle for the top half
            clip = {
                'x': 0,
                'y': 0,
                'width': viewport_width,
                'height': viewport_height # * 2 # / 2  # Top half
            }
            
            # Generate a unique filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_context = "".join(c if c.isalnum() else "_" for c in context_info)
            filename = f"screenshot_top_half_{timestamp}_{safe_context}.png"
            filepath = os.path.join(self.screenshot_dir, filename)
            
            # Take the screenshot with the specified clip
            await self.page.screenshot({
                'path': filepath,
                'clip': clip
            })
            
            # Log the successful capture
            self.logger.logger_info.info(f"Top half screenshot saved to {filepath}")
        
        except Exception as e:
            self.logger.logger_err.error(f"Failed to take top half screenshot: {e}")


    async def log_html_content(self, context_info=""):
        """
        Retrieves the current page's HTML content and logs it.
        
        :param context_info: Additional information to include in the log entry.
        """
        try:
            # Retrieve the page's HTML content
            html_content = await self.page.content()
            
            # Option 1: Log the entire HTML content (Use with caution)
            # self.logger.logger_info.info(f"HTML Content ({context_info}):\n{html_content}")
            
            # Option 2: Save the HTML content to a file and log the file path
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_context = "".join(c if c.isalnum() else "_" for c in context_info)
            filename = f"html_snapshot_{safe_context}_{timestamp}.html"
            filepath = os.path.join(self.html_output_dir, filename)
            
            with open(filepath, "w", encoding="utf-8") as file:
                file.write(html_content)
            
            self.logger.logger_info.info(f"HTML content saved to {filepath} (Context: {context_info})")
        
        except Exception as e:
            self.logger.logger_err.error(f"Failed to log HTML content ({context_info}): {e}")