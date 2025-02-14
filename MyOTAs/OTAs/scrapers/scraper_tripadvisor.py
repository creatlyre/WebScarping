API_KEY = "56ed5b7f827aa5c258b3f6d3f57d36999aa949e8"

import io
import os
import time
import random
import sys
import shutil
import glob
import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import traceback  # For detailed stack traces
import requests
import pandas as pd
from bs4 import BeautifulSoup
import nest_asyncio
import asyncio
# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)


from logger.logger_manager import LoggerManager
from file_management.file_path_manager import DetermineDebugRun
from scrapers.cookies.tripadvisor_cookies import TripadvisorCookies

nest_asyncio.apply()

class TripadvisorScraper:

    def __init__(self, api_key, file_manager, date_today, css_selectors, currency_code='EUR'):
        try:
            self.api_key = api_key
            self.file_manager = file_manager
            self.logger = LoggerManager(file_manager, application="tripadvisor_daily")
            self.date_today = date_today
            self.currency_code = currency_code
            self.currency_code_found = ''
            self.accumulated_cost = 0
            self.accumulated_products_collected = 0
            self.accumulated_time = 0
            self.partialy_done = False
            self.threshold_total_products = 600
            self.headers = {}
            # List of European countries for randomizing the proxy
            european_countries = [
                'al', 'ad', 'at', 'be', 'ba', 'bg', 'hr', 'cy', 'cz', 'dk', 'ee', 'fi', 
                'fr', 'de', 'gr', 'hu', 'is', 'ie', 'it', 'lv', 'li', 'lt', 'lu', 'mt', 
                'mc', 'me', 'nl', 'mk', 'no', 'pl', 'pt', 'ro', 'ru', 'sm', 'sk', 'si', 
                'es', 'se', 'ch', 'gb'
            ]
            self.proxy_country = random.choice(european_countries)
            
            # Basic ZenRows params for requests
            self.params = {
                "apikey": self.api_key,
                # "js_render": "true",
                "premium_proxy": "true",
                "proxy_country": self.proxy_country,
                'custom_headers': 'true',
            }

            # Load the latest cookies from the cookie file
            self.load_latest_cookies()

            self.output_filename = file_manager.file_path_output
            self.screenshot_dir = "screenshots"
            self.html_output_dir = "html_snapshots"
            os.makedirs(self.screenshot_dir, exist_ok=True)
            os.makedirs(self.html_output_dir, exist_ok=True)

            # DataFrame with links
            self.df_links = pd.DataFrame()
            self.load_urls_to_complete()
            self.load_existing_data()

            # self.headers = {
            #     'Cookie': 'TAUD=LA-1737366455103-1*ARC-1*RDD-1-2025_01_20*LG-1-2.1.F.*LD-2-.....*CUR-1-EUR;
            # }

            # CSS selectors for extraction
            self.css_total_products = css_selectors.get('total_products')
            self.css_products_list  = css_selectors.get('products_list')
            self.css_product_link   = css_selectors.get('product_link')
            self.css_product_title  = css_selectors.get('product_title')
            self.css_product_reviews_amount = css_selectors.get('product_reviews_amount')
            self.css_product_reviews_rating = css_selectors.get('product_reviews_rating')
            self.css_product_price   = css_selectors.get('product_price')
            self.css_product_discount= css_selectors.get('product_discount')
            self.css_product_text    = css_selectors.get('product_text')
            self.css_currency_language_button = css_selectors.get('currency_language_button')
            # Collect supplier name
            self.css_supplier_section = css_selectors.get('supplier_section')
            self.css_supplier = css_selectors.get('supplier')
            
            #For new city and their category
            self.css_category = css_selectors.get('category')

        except Exception as e:
            print("Error during initialization:", e)
            traceback.print_exc()
            sys.exit(1)  # Exit if initialization fails

    def load_latest_cookies(self):
        """
        Loads cookies from the latest file. If today's cookie file is not found, 
        it loads the most recent cookie file available. Extracts only necessary cookies like TAUD 
        and replaces the currency with self.currency_code.
        """
        try:
            today_cookies_path = fr'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Baza Excel\Tripadvisor\Cookies\{self.date_today}_tripadvisor_eur_cookies.txt'

            def extract_necessary_cookies(cookie_string):
                cookies = cookie_string.split("; ")
                filtered_cookies = [c for c in cookies if c.startswith("TAUD")]
            
                if filtered_cookies:
                    taud_cookie = filtered_cookies[0]
                    taud_cookie = taud_cookie.rsplit("CUR-", 1)[0] + f"CUR-1-{self.currency_code}"
                    return taud_cookie
                return None  # Return None if TAUD cookie not found

            if os.path.exists(today_cookies_path):
                self.logger.logger_info.info(f"Loading cookies from today's file: {today_cookies_path}")
                with open(today_cookies_path, "r") as f:
                    raw_cookies = f.read().strip()
                    extracted_cookie = extract_necessary_cookies(raw_cookies)
                    if extracted_cookie:
                        self.headers['Cookie'] = extracted_cookie
                    else:
                        self.logger.logger_warning.warning("TAUD cookie not found in today's cookies.")
            else:
                cookies_dir = os.path.dirname(today_cookies_path)
                cookie_files = glob.glob(os.path.join(cookies_dir, "*_tripadvisor_eur_cookies.txt"))

                if cookie_files:
                    latest_cookie_file = max(cookie_files, key=os.path.getmtime)
                    self.logger.logger_info.info(f"Today's file not found. Loading from latest file: {latest_cookie_file}")
                    with open(latest_cookie_file, "r") as f:
                        raw_cookies = f.read().strip()
                        extracted_cookie = extract_necessary_cookies(raw_cookies)
                        if extracted_cookie:
                            self.headers['Cookie'] = extracted_cookie
                        else:
                            self.logger.logger_warning.warning("TAUD cookie not found in the latest cookies.")
                else:
                    self.logger.logger_err.error("No cookie files found. Headers will not include cookies.")
        except Exception as e:
            self.logger.logger_err.error(f"Exception in load_latest_cookies: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def load_urls_to_complete(self):
        """
        Loads the initial list of links to process from a CSV and filters out
        only the ones where 'Run' == 1.
        """
        try:
            paths = self.file_manager.get_file_paths()
            self.df_links = pd.read_csv(paths['link_file'])
            self.df_links_update_version = self.df_links.copy()
            self.df_links = self.df_links[self.df_links['Run'] == 1]
            self.logger.logger_info.info(f"Loaded {len(self.df_links)} links to scrape.")
        except Exception as e:
            self.logger.logger_err.error(f"Exception in load_urls_to_complete: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def load_existing_data(self):
        """
        Checks if today’s run is already completed or if a partial run is stored 
        and needs resuming.
        """
        try:
            paths = self.file_manager.get_file_paths()
            if len(self.df_links) == 0:
                self.logger.logger_info.info(f"No links to process in '{paths['link_file']}'.")
                return

            if os.path.exists(paths['file_path_output']):
                self.logger.logger_info.info(f"Today's ({self.date_today}) TripAdvisor run is already completed.")
                return 'Done'

            if os.path.exists(paths['file_path_done']):
                self.logger.logger_info.info(f"Resuming from previous run.")
                df_done = pd.read_csv(paths['file_path_done'])
                for index, row in df_done.iterrows():
                    done_id = row['ID']
                    done_products_collected = row['ProductsCollected']
                    
                    if done_id in self.df_links['ID'].values:
                        total_products = self.df_links.loc[self.df_links['ID'] == done_id, 'TotalProducts'].values[0]
                        if done_products_collected >= total_products or done_products_collected > self.threshold_total_products:
                            self.df_links = self.df_links[self.df_links['ID'] != done_id]
                        else:
                            self.partialy_done = True
                            self.products_collected = done_products_collected
                self.logger.logger_info.info(f"Resumed {len(df_done)} links from the previous run.")
            else:
                self.logger.logger_info.info("Starting fresh run.")
        except Exception as e:
            self.logger.logger_err.error(f"Exception in load_existing_data: {e}")
            self.logger.logger_err.error(traceback.format_exc())


    def make_request(self, url):
        """
        Sends a GET request through ZenRows to retrieve the rendered HTML page.
        Includes retry logic and handles timeouts gracefully.
        """
        try:
            # Create a session with retry logic
            session = requests.Session()
            retries = Retry(
                total=5,  # Retry up to 5 times
                backoff_factor=1,  # Backoff time: 0s, 1s, 2s, 4s, etc.
                status_forcelist=[422, 500, 502, 503, 504],  # Retry on server errors
                allowed_methods=["GET"],  # Retry only GET requests
            )
            adapter = HTTPAdapter(max_retries=retries)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            # Prepare request parameters
            local_params = dict(self.params)
            local_params["url"] = url

            # Log the request
            self.logger.logger_info.info(f"Making request to {url}")

            # Make the GET request
            response = session.get(
                "https://api.zenrows.com/v1/",
                params=local_params,
                headers=self.headers,
                timeout=20  # Increase timeout to 20 seconds
            )

            # Handle successful response
            if response.status_code == 200:
                # Extract ZenRows usage headers
                concurrency_limit = response.headers.get("Concurrency-Limit")
                concurrency_remaining = response.headers.get("Concurrency-Remaining")
                request_cost = response.headers.get("X-Request-Cost")
                self.accumulated_cost += float(request_cost) if request_cost else 0

                self.logger.logger_info.info(f"Concurrency-Limit: {concurrency_limit}")
                self.logger.logger_info.info(f"Concurrency-Remaining: {concurrency_remaining}")
                self.logger.logger_info.info(f"X-Request-Cost (fraction of a request): {request_cost}")
                self.logger.logger_info.info(f"Total accumulated cost: {self.accumulated_cost}")

                return response
            else:
                # Log the error if the status code is not 200
                self.logger.logger_err.error(f"Request failed for {url}, status_code: {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            # Handle timeout exceptions
            self.logger.logger_err.error(f"Request to {url} timed out.")
            return None
        except requests.exceptions.RequestException as e:
            # Handle other request exceptions
            self.logger.logger_err.error(f"Error making request to {url}: {e}")
            self.logger.logger_err.error(traceback.format_exc())
            return None
        except Exception as e:
            # Handle unexpected errors
            self.logger.logger_err.error(f"Unexpected error in make_request for {url}: {e}")
            self.logger.logger_err.error(traceback.format_exc())
            return None


    def scrape(self):
        """
        Main method to start the scraping process for each link in df_links.
        """
        try:
            if self.df_links.empty:
                self.logger.logger_info.info("No links to scrape.")
                return True

            for _, row in self.df_links.iterrows():
                try:
                    time_start = time.time()
                    self.url = row['URL']
                    self.city = row['City']
                    self.category = row['Category']
                    self.id = row['ID']

                    if not self.partialy_done:
                        self.products_collected = 0

                        response = self.make_request(self.url)

                        if self.currency_code_found == '':
                            self.verify_correct_currency_code(response)
                        if not response:
                            self.logger.logger_err.error(f"Failed to retrieve initial page for ID {self.id}. Skipping.")
                            continue

                        self.total_products = self.collect_max_products(response)
                        if not self.total_products:
                            self.total_products = 300
                        if self.total_products > self.threshold_total_products:
                            self.total_products = self.threshold_total_products
                    else:
                        self.logger.logger_info.info(f"Resuming from {self.products_collected} products.")
                        self.total_products = self.df_links.loc[self.df_links['ID'] == self.id, 'TotalProducts'].values[0]

                    # For debugging purposes, limit the total products to 30
                    DEBUG = DetermineDebugRun()
                    if DEBUG.debug:
                        self.logger.logger_info.warning("DEBUG mode is enabled. Limiting total products to 120.")
                        self.total_products = 120

                    # 3) Loop in increments of 30. (TripAdvisor often uses “-oa30”, “-oa60” etc.)
                    while self.products_collected < self.total_products:
                        if self.products_collected == 0:
                            page_url = self.url
                            self.collect_data_from_html(response)
                            self.products_collected += 30
                            
                            # Save done part to CSV DONE file
                            self.save_done_part_to_csv()
                        else:
                            page_url = self.url.split('.html')[0] + f'-oa{self.products_collected}.html'
                            self.logger.logger_info.info(f"Processing: {page_url} (collected so far: {self.products_collected})")
                            response = self.make_request(page_url)
                            if not response:
                                self.logger.logger_err.error(f"Failed to retrieve page {page_url}. Stopping pagination for ID {self.id}.")
                                break
                            # 4) Parse the HTML, collect data
                            self.collect_data_from_html(response)
                            self.products_collected += 30
                            # Save done part to CSV DONE file
                            self.save_done_part_to_csv()
                        # if DEBUG.debug:
                            # break
                    
                    
                    if self.partialy_done:
                        self.partialy_done = False
                    time_end = time.time()

                    self.accumulated_products_collected += self.products_collected
                    time_for_url = time_end - time_start
                    self.accumulated_time += time_for_url
                    self.logger.logger_done.info(f"Scraping completed for city={self.city}, category={self.category}. Time {time_for_url:.2f} seconds. Total products collected: {self.products_collected}. Pages visited: {self.products_collected // 30}")
                    
                except Exception as e:
                    self.logger.logger_err.error(f"Exception while scraping ID {row.get('ID', 'Unknown')}: {e}")
                    self.logger.logger_err.error(traceback.format_exc())
                    continue  # Continue with the next link

            self.logger.logger_done.info("All links processed. Closing.")
            return True
        except Exception as e:
            self.logger.logger_err.error(f"Exception in scrape method: {e}")
            self.logger.logger_err.error(traceback.format_exc())
            return False

    def collect_max_products(self, response):
        """
        Extract the total number of products from the page 
        using the self.css_total_products selector (div.Ci).
        """
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            total_products_element = soup.select_one(self.css_total_products)
            if total_products_element:
                total_products_text = total_products_element.text.strip().split()[-1]
                total_products_text = total_products_text.replace(',', '')
                total_products = int(total_products_text)
                
                # Update the TotalProducts column in df_links
                self.df_links_update_version.loc[self.df_links_update_version['URL'] == self.url, 'TotalProducts'] = total_products
                self.df_links_update_version.to_csv(self.file_manager.get_file_paths()['link_file'], index=False)
                
                return total_products
                
            self.logger.logger_err.error("Unable to find or parse total products. Using fallback=300.")
            return 300
        except Exception as e:
            self.logger.logger_err.error(f"Error parsing total products: {e}")
            self.logger.logger_err.error(traceback.format_exc())
            return 300

    def collect_data_from_html(self, response):
        """
        Given an HTTP response, parse out each product's relevant data
        and append it to a CSV.
        """
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            products_list = soup.select(self.css_products_list)
            self.logger.logger_info.info(f"Found {len(products_list)} product sections on this page.")

            data_list = []
            count_on_page = 1
            for product in products_list:
                try:
                    # Extract data from each product
                    title_element = product.select_one(self.css_product_title)
                    link_element  = product.select_one(self.css_product_link)
                    price_element = product.select_one(self.css_product_price)
                    discount_element = product.select_one(self.css_product_discount)
                    rating_element   = product.select_one(self.css_product_reviews_rating)
                    reviews_element  = product.select_one(self.css_product_reviews_amount)
                    text_element     = product.select_one(self.css_product_text)

                    # Get textual contents
                    title = title_element.text.strip() if title_element else None
                    product_url = "https://www.tripadvisor.com" + link_element['href'] if (link_element and link_element.has_attr('href')) else None
                    price = price_element.text.strip() if price_element else None
                    rating = rating_element.text.strip() if rating_element else None
                    reviews_amount = reviews_element.text.strip() if reviews_element else None
                    discount = discount_element.text.strip() if discount_element else None
                    text = text_element.text.strip() if text_element else None

                    # If discount is not None, the current 'price' might be the discounted price
                    if discount is not None:
                        # Swap them
                        temp_price = discount
                        discount = price
                        price = temp_price

                    row_data = {
                        'Tytul': title,
                        'Tytul URL': product_url,
                        'Cena': price,
                        'Opinia': rating,
                        'IloscOpini': reviews_amount,
                        'Przecena': discount,
                        'Tekst': text,
                        'Data zestawienia': self.date_today,
                        'Pozycja': self.products_collected + count_on_page,
                        'Kategoria': self.category,
                        'Booked': "N/A",
                        'SiteUse': "Tripadvisor",
                        'Miasto': self.city
                    }
                    data_list.append(row_data)
                    count_on_page += 1
                except Exception as e:
                    self.logger.logger_err.error(f"Error parsing product data: {e}")
                    self.logger.logger_err.error(traceback.format_exc())
                    continue  # Skip this product and continue with others

            if not data_list:
                self.logger.logger_info.info("No products extracted from this page.")
                return

            df_temp = pd.DataFrame(data_list)
            self.save_data_to_csv(df_temp)
        except Exception as e:
            self.logger.logger_err.error(f"Exception in collect_data_from_html: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def save_data_to_csv(self, df_temp):
        """
        Appends the given DataFrame to the corresponding city-based CSV file.
        E.g. 2025-01-17-Paris-Tripadvisor.csv
        """
        try:
            file_path = f"{self.file_manager.output}/{self.date_today}-{self.city}-Tripadvisor.csv"
            self.logger.logger_info.info(f"Saving data to {file_path} (rows={len(df_temp)})")
            df_temp.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
            self.logger.logger_info.info("Data saved at location: " + file_path)
        except Exception as e:
            self.logger.logger_err.error(f"Error saving data to CSV: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def save_done_part_to_csv(self):
        """
        Saves the current progress to a CSV file to resume later. File path is 2025-01-20-DONE-Tripadvisor.csv
        Overwrites existing ID if found, otherwise appends new rows.
        """
        try:
            done_df = pd.DataFrame([[self.id, self.url, self.city, self.products_collected]], columns=['ID', 'URL', 'City', 'ProductsCollected'])
            
            if os.path.exists(self.file_manager.file_path_done):
                existing_df = pd.read_csv(self.file_manager.file_path_done)
                existing_df = existing_df[existing_df['ID'] != self.id]  # Remove existing ID if found
                done_df = pd.concat([existing_df, done_df], ignore_index=True)
            
            done_df.to_csv(self.file_manager.file_path_done, index=False)
            self.logger.logger_info.info(f"Saved progress to {self.file_manager.file_path_done}.")
        except Exception as e:
            self.logger.logger_err.error(f"Error saving done part to CSV: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def combine_csv_to_xlsx(self):
        """
        Combines multiple CSV files into a single Excel file with separate sheets for each CSV.
        Moves the original CSV files to an archive folder after combining.
        """
        try:
            csv_files_locations = self.file_manager.get_file_paths()['output']
            archive_folder = self.file_manager.get_file_paths()['archive_folder']
            
            file_path_output = self.file_manager.get_file_paths()['file_path_output']

            # Get all CSV files with the specified date prefix in the output directory
            csv_files = [
                file for file in os.listdir(csv_files_locations)
                if file.endswith('.csv') and file.startswith(self.date_today)
            ]

            if not csv_files:
                self.logger.logger_info.info(
                    f"No CSV files found with the date prefix '{self.date_today}'"
                )
                return

            if not os.path.exists(archive_folder):
                os.makedirs(archive_folder)

            with pd.ExcelWriter(file_path_output, engine='xlsxwriter') as writer:
                for csv_file in csv_files:
                    try:
                        csv_path = os.path.join(csv_files_locations, csv_file)
                        sheet_name = os.path.splitext(csv_file)[0]
                        sheet_name = sheet_name.split(self.date_today + '-')[1].split(
                            f'-{self.file_manager.site}'
                        )[0]

                        df = pd.read_csv(csv_path)
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    except Exception as e:
                        self.logger.logger_err.error(f"Error processing CSV file {csv_file}: {e}")
                        self.logger.logger_err.error(traceback.format_exc())
                        continue  # Skip this CSV and continue with others

            self.logger.logger_done.info(f"Combined CSV files into '{file_path_output}'")

            # Move the original CSV files to the archive folder
            for csv_file in csv_files:
                try:
                    csv_path = os.path.join(csv_files_locations, csv_file)
                    destination_path = os.path.join(archive_folder, csv_file)
                    shutil.move(csv_path, destination_path)
                    self.logger.logger_info.info(f"Moved {csv_file} to the archive folder.")
                except FileNotFoundError as e:
                    self.logger.logger_err.error(f"Error moving {csv_file}: {str(e)}")
                except Exception as e:
                    self.logger.logger_err.error(f"Unexpected error moving {csv_file}: {e}")
                    self.logger.logger_err.error(traceback.format_exc())
        except Exception as e:
            self.logger.logger_err.error(f"Exception in combine_csv_to_xlsx: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def new_links_extract_category_from_html(self):
        """
        Extracts all available categories from the HTML content and updates the DataFrame with the extracted data.
        """
        try:
            extracted_data = []
            for index, row in self.df_links.iterrows():
                try:
                    self.url = row['URL']
                    self.city = row['City']
                    self.category = row['Category']
                    self.id = row['ID']

                    response = self.make_request(self.url)

                    if not response:
                        self.logger.logger_err.error(f"Failed to retrieve page for category extraction ID {self.id}. Skipping.")
                        continue

                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Find all category elements using the specified CSS selector
                    category_elements = soup.select(self.css_category)
                    
                    for category_element in category_elements:
                        try:
                            category_name = category_element.select_one('div.biGQs._P.pZUbB.KxBGd').get_text(strip=True)
                            if 'tours' == category_name.lower().strip():
                                tours_url = "https://www.tripadvisor.com" + category_element['href']
                                replace_key = category_element['href'].split('Activities-')[-1].split('-')[0]

                            if 'category' in category_element['href']:
                                extracted_category = category_element['href'].split('#category=')[-1]
                                category_link = tours_url.replace(replace_key, f'c{extracted_category}')
                                
                            else:
                                category_link = "https://www.tripadvisor.com" + category_element['href']

                            extracted_data.append({
                                "ID": self.id,
                                "City": self.city,
                                "Category": category_name,
                                "Category Link": category_link,
                            })
                        except Exception as e:
                            self.logger.logger_err.error(f"Error extracting category from element: {e}")
                            self.logger.logger_err.error(traceback.format_exc())
                            continue  # Skip this category and continue with others

                    # Convert extracted data to DataFrame and merge with the original DataFrame
                    if extracted_data:
                        categories_df = pd.DataFrame(extracted_data)
                        self.df_links = pd.concat([self.df_links, categories_df], ignore_index=True)
                        self.logger.logger_info.info("Categories extracted and added to DataFrame.")
                        self.new_links_save_new_ids_and_links(categories_df)
                    else:
                        self.logger.logger_warning.warning("No categories were extracted from the HTML.")
                except Exception as e:
                    self.logger.logger_err.error(f"Exception while extracting categories for ID {row.get('ID', 'Unknown')}: {e}")
                    self.logger.logger_err.error(traceback.format_exc())
                    continue  # Continue with the next link
        except Exception as e:
            self.logger.logger_err.error(f"Exception in extract_category_from_html: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def new_links_save_new_ids_and_links(self, categories_df):
        """
        Saves the new collected IDs and links to a file.
        """
        try:
            new_ids_links_file = 'tripadvisor_new_links.csv'
            categories_df.to_csv(new_ids_links_file, index=False, mode='a', header=not os.path.exists(new_ids_links_file))
            self.logger.logger_info.info(f"New IDs and links saved to {new_ids_links_file}.")
        except Exception as e:
            self.logger.logger_err.error(f"Error saving new IDs and links: {e}")
            self.logger.logger_err.error(traceback.format_exc())

    def verify_correct_currency_code(self, response):
        """
        Verifies if the currency code is correct and updates the TAUD cookie if needed.
        """
        # Extract the currency code from the response
        soup = BeautifulSoup(response.content, 'html.parser')
        self.currency_code_found = soup.select_one(self.css_currency_language_button).text.strip()
        # Check if the currency code is correct
        if self.currency_code.lower() not in self.currency_code_found.lower():
            self.logger.logger_info.info(f"Currency code extracted: {self.currency_code_found} not matching the expected code: {self.currency_code}.")
            self.logger.logger_info.info("Updating the TAUD cookie with the correct currency code.")
            self.update_taud_cookie()
        else:
            self.logger.logger_info.info(f"Currency code {self.currency_code_found} is correct.")

    def update_taud_cookie(self):
        """
        Updates the TAUD cookie with the new currency code.
        """
        tripadvisor = TripadvisorCookies(API_KEY, self.file_manager, self.file_manager.date_today)
        asyncio.run(tripadvisor.scrape_cookies())

    def all_links_get_provider_name(self, url):
        """
        Extracts the supplier name from the HTML content and updates the DataFrame with the extracted data.
        """
        try:
            response = self.make_request(url)

            if not response:
                self.logger.logger_err.error(f"Failed to retrieve page for supplier extraction ID {self.url}. Skipping.")
                return "NotFound"
                

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all supplier elements using the specified CSS selector
            supplier_elements = soup.select(self.css_supplier_section)
            
            for supplier_element in supplier_elements:
                try:
                    supplier_name = supplier_element.select_one(self.css_supplier)
                    if supplier_name is not None:
                        supplier_name = supplier_name.get_text(strip=True)
                        return supplier_name
                except Exception as e:
                    self.logger.logger_err.error(f"Error extracting supplier from element: {e}")
                    self.logger.logger_err.error(traceback.format_exc())
                    return "NotFound"  # Skip this supplier and continue with others

            self.logger.logger_info.info("Supplier name extracted and added to DataFrame.")
        except Exception as e:
            self.logger.logger_err.error(f"Exception while extracting supplier for {url}: {e}")
            self.logger.logger_err.error(traceback.format_exc())
    def all_links_save_dataframe(self, df, file_path):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            workbook.strings_to_urls = False
            df.to_excel(writer, index=False, sheet_name='AllLinks')
        with open(file_path, 'wb') as f:
            f.write(output.getvalue())

