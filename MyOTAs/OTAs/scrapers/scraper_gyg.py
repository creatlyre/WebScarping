import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from selenium.webdriver.common.by import By
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import traceback
import re

from scrapers.scraper_base import ScraperBase


class ScraperGYG(ScraperBase):
    
    """
    A scraper class for GetYourGuide (GYG) website to extract product data,
    handle currency settings, manage logging, and upload results to Azure Blob Storage.
    """
    def __init__(self, url, city, css_selectors, file_manager, logger, activity_per_page=16, provider=False):
        super().__init__(url, city, css_selectors, file_manager, logger)
        if provider:
            self.css_provider = self.css_selectors.get('provider')
        self.activity_per_page = activity_per_page

    def handle_error_and_rerun(self, error):
        """
        Handles errors by logging them and implementing any necessary rerun logic.

        Args:
            error (Exception): The exception that was raised.
        """
        tb = traceback.format_exc()
        self.logger.logger_err.error(f'An error occurred: {error}\nTraceback: {tb}')
        # Placeholder for additional error handling (e.g., sending notifications)
        # Example: send_error_notification(error, tb)
        # 
    def get_provider_name(self):
        provider_name = self.driver.find_element(
            By.CLASS_NAME, self.css_provider
        )
        return provider_name

    def daily_run_gyg(self, df_links: pd.DataFrame = pd.DataFrame(), re_run: bool = False):
        """
        Executes the daily scraping routine for GYG across specified cities and categories.

        Args:
            df_links (pd.DataFrame, optional): DataFrame containing URLs and city information.
                Defaults to an empty DataFrame.
            re_run (bool, optional): Flag indicating whether to perform a rerun. Defaults to False.
        """
        try:
            paths = self.file_manager.get_file_paths()
            # Load links if not provided
            if df_links.empty:
                df_links = pd.read_csv(paths['link_file'])
                self.logger.logger_info.info(f"Loaded {len(df_links)} links from '{paths['link_file']}'.")

            # Define currency-based city groups
            EUR_City = [
                "Amsterdam", "Athens", "Barcelona", "Berlin", "Dublin", "Dubrovnik", "Florence", "Istanbul",
                "Krakow", "Lisbon", "Madrid", "Milan", "Naples", "Paris", "Porto", "Rome", "Palermo", "Venice",
                "Taormina", "Capri", "Sorrento", "Mount-Etna", "Mount-Vesuvius", "Herculaneum", "Amalfi-Coast",
                "Pompeii", "Sintra", "Heraklion"
            ]

            USD_City = [
                "Las-Vegas", "New-York-City", "Cancun", "Dubai"
            ]

            GBP_City = [
                "Edinburgh", "London"
            ]

            # Check if today's run is already completed
            if os.path.exists(paths['file_path_output']) and not re_run:
                self.logger.logger_info.info(f"Today's ({self.date_today}) GYG run is already completed.")
                return 'Done'

            # Handle resuming from a previous incomplete run
            if os.path.exists(paths['file_path_done']) and not re_run:
                done_msg = pd.read_csv(paths['file_path_done'])
                done_msg = done_msg.transpose()
                done_msg = done_msg.set_axis(done_msg.iloc[0], axis=1)
                done_msg = done_msg.iloc[1:]
                done_index = int(done_msg.index.values[0])
                df_links = df_links.iloc[(done_index + 1):]
                self.logger.logger_info.info(f"Resuming from index {done_index + 1}.")
            elif re_run:
                self.logger.logger_info.info(f"Re-running with {len(df_links)} links.")
            else:
                self.logger.logger_info.info("Starting fresh run.")

            # Filter links based on the 'Run' flag
            df_links = df_links[df_links['Run'] == 1]
            self.logger.logger_info.info(f"{len(df_links)} links marked for running.")

            # Initialize timers for performance monitoring
            start_time = time.time()
            total_pages = 1
            iter_count = 0
            # Iterate over each link to perform scraping
            for index, row in df_links.iterrows():
                page = 1
                max_pages = 9999  # Placeholder for maximum pages
                data = []
                position = 0
                url_time = time.time()

                while page <= max_pages:
                    # Reinitialize driver every 25 iterations to prevent session issues
                    if iter_count % 25 == 0 and iter_count != 0:
                        self.quit_driver()
                        self.driver = self.initialize_driver()
                    iter_count += 1

                    url = f'{row["URL"]}&p={page}'
                    self.logger.logger_info.info(f"Processing URL: {url}")

                    # Navigate to the URL
                    self.driver.get(url)
                    time.sleep(1)  # Wait for page to load

                    # Handle potential page load issues
                    try:
                        title_webpage = self.driver.title
                        current_url = self.driver.current_url
                        self.logger.logger_info.info(f"Title: {title_webpage} | Current URL: {current_url}")
                    except WebDriverException:
                        self.logger.logger_err.error("Page unresponsive. Attempting to refresh...")
                        try:
                            self.driver.refresh()
                            time.sleep(1)
                        except WebDriverException:
                            self.quit_driver()
                            self.logger.logger_err.error("Failed to refresh the page. Reinitializing driver.")
                            self.driver = self.initialize_driver()
                            self.driver.get(url)
                            self.logger.logger_info.info("Reopened the webpage after failure.")
                            time.sleep(4)

                    # Verify and set currency based on the city
                    self._verify_and_set_currency(row['City'], EUR_City, USD_City, GBP_City)

                    # Parse the HTML content using BeautifulSoup
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')

                    # Determine the maximum number of pages if not already set
                    if max_pages == 9999:
                        max_pages = self._determine_max_pages(soup)
                        total_pages += max_pages

                    # Extract data from the current page
                    tour_items = soup.select("[data-test-id=vertical-activity-card]")
                    if not tour_items:
                        self.logger.logger_info.info("No tour items found with the current CSS selector.")
                        tour_items = soup.find_all('li', {'class': 'list-element'})

                    self.logger.logger_info.info(f"Found {len(tour_items)} tour items on page {page}.")
                    for tour_item in tour_items:
                        try:
                            product_data = self._extract_product_data(tour_item, page, position, row['City'], row['RawCategory'])
                            data.append(product_data)
                            position += 1
                        except Exception as e:
                            self.logger.logger_err.error(f"Error extracting tour item data: {e}")

                    page += 1  # Proceed to the next page

                # Log performance metrics for the current city-category combination
                url_done = time.time()
                message = (f"Time for {row['City']} - {row['RawCategory']}: "
                           f"{round((url_done - url_time)/60, 3)} mins | "
                           f"Pages: {max_pages} | "
                           f"AVG: {round((url_done - url_time)/max_pages, 2)}s per page")
                self.logger.logger_done.info(message)

                # Create DataFrame from extracted data
                df = pd.DataFrame(data, columns=[
                    'Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini',
                    'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja',
                    'Kategoria', 'Booked', 'SiteUse', 'Miasto'
                ])

                # Save the DataFrame to CSV
                file_path = f"{paths['output']}/{self.date_today}-{row['City']}-GYG.csv"
                df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
                self.logger.logger_info.info(f"Saved data to '{file_path}'.")

                # Mark the row as done
                row.to_csv(paths['file_path_done'], header=True, index=True)
                self.logger.logger_info.info(f"Marked city '{row['City']}' as done.")

            # Finalize the scraping session
            self.quit_driver()
            end_time = time.time()
            total_runtime = round((end_time - start_time)/60, 2)
            message_done = (f"Completed {len(df_links)} URLs in {total_runtime} mins | "
                            f"Total Pages: {total_pages} | "
                            f"AVG: {round((end_time - start_time)/total_pages, 2)}s per page")
            self.logger.logger_done.info(message_done)

            # Combine CSV files into Excel if not a rerun
            if not re_run:
                self.combine_csv_to_xlsx()
                return "Done"

        except Exception as e:
            self.handle_error_and_rerun(e)

    def _verify_and_set_currency(self, city: str, EUR_City: list, USD_City: list, GBP_City: list):
        """
        Verifies the current currency setting on the website and updates it if necessary.

        Args:
            city (str): The city being processed.
            EUR_City (list): List of cities using EUR.
            USD_City (list): List of cities using USD.
            GBP_City (list): List of cities using GBP.
        """
        try:
            self.logger.logger_info.info(f"Verifying currency for city '{city}'.")

            # Wait for the currency selector dropdown to be clickable
            currency_selector = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'select[id="footer-currency-selector"]'))
            )
            self.logger.logger_info.info("Currency selector dropdown is clickable.")

            # Create a Select object for the dropdown
            select = Select(currency_selector)

            # Get the currently selected option
            selected_option = select.first_selected_option
            current_currency = selected_option.text.strip()
            self.logger.logger_info.info(f"Current currency selected: {current_currency}")

            # Determine desired currency based on city
            if city in EUR_City:
                desired_currency_text = 'Euro (€)'
            elif city in USD_City:
                desired_currency_text = 'U.S. Dollar ($)'
            elif city in GBP_City:
                desired_currency_text = 'British Pound (£)'
            else:
                desired_currency_text = 'Euro (€)'
                self.logger.logger_info.info(f"City '{city}' is not categorized for currency settings.")

            # Change currency if it does not match the desired currency
            if desired_currency_text not in current_currency:
                self.logger.logger_info.info(f"Changing currency to {desired_currency_text} for city '{city}'.")
                try:
                    select.select_by_visible_text(desired_currency_text)
                    self.logger.logger_info.info(f"Selected currency '{desired_currency_text}' successfully.")
                    time.sleep(2)  # Wait for the currency change to take effect
                except Exception as e:
                    self.logger.logger_err.error(f"Failed to select currency '{desired_currency_text}': {e}")
            else:
                self.logger.logger_info.info(f"Currency already set to {desired_currency_text} for city '{city}'.")

        except TimeoutException:
            self.logger.logger_err.error("Timeout while locating the currency selector dropdown.")
        except Exception as e:
            self.logger.logger_err.error(f"Error during currency verification: {e}")

    def _determine_max_pages(self, soup: BeautifulSoup) -> int:
        """
        Determines the maximum number of pages to scrape based on the website's pagination.

        Args:
            soup (BeautifulSoup): Parsed HTML content of the page.

        Returns:
            int: The maximum number of pages to scrape.
        """
        try:
            # Alternative method to calculate max pages based on activity count
            activity_count_text = soup.select_one('div.search-header__left__data-wrapper__count').text.strip()
            activity_count = int(activity_count_text.split()[0])
            max_pages = int(activity_count / self.activity_per_page) + 1  # Assuming 16 activities per page
            self.logger.logger_info.info(f"Calculated max pages based on activity count: {max_pages}")
            return max_pages
        except (AttributeError, ValueError) as e:
            self.logger.logger_err.error(f"Failed to calculate max pages based on activity count: {e}")

        # Default fallback if all else fails
        self.logger.logger_info.info("Defaulting max pages to 5 due to inability to determine dynamically.")
        return 5

    def _extract_product_data(self, tour_item: BeautifulSoup, page: int, position: int, city: str, category: str) -> list:
        """
        Extracts relevant product data from a single tour item.

        Args:
            tour_item (BeautifulSoup): Parsed HTML of a single tour item.
            page (int): Current page number.
            position (int): Position of the product on the page.
            city (str): City associated with the product.
            category (str): Category of the product.

        Returns:
            list: A list of extracted product data fields.
        """
        try:
            # Initialize default values
            title = 'N/A'
            price = 'N/A'
            product_url = 'N/A'
            discount = 'N/A'
            amount_reviews = 'N/A'
            stars = 'N/A'
            booked = 'N/A'
            new_activity = 'N/A'

            # Extract title
            title_element = tour_item.find('h3', {'class': 'vertical-activity-card__title'})
            if title_element:
                title = title_element.get_text(strip=True)
                self.logger.logger_info.debug(f"Extracted title: {title}")
            else:
                self.logger.logger_err.error("Title element not found.")

            # Extract price and discount
            price_element = tour_item.find('div', {'class': 'activity-price'})
            discount_element = price_element.find_all('div', {'class': 'activity-price__text'})

            if len(discount_element) == 2:
                # Check for the element with actual content
                discount_texts = [el.get_text(strip=True) for el in discount_element if el.get_text(strip=True)]
                if len(discount_texts) == 2:
                    price = discount_texts[0]
                    discount = discount_texts[1]
                elif len(discount_texts) == 1:
                    price = discount_texts[0]
                    discount = "N/A"
                else:
                    self.logger.logger_err.error("Price element not found.")
                    raise ("Price element not found.")
                    

                self.logger.logger_info.debug(f"Extracted price: {price}")
                self.logger.logger_info.debug(f"Extracted discount: {discount}")
            else:
                # Extract price if discount not available
                if price_element:
                    price = price_element.get_text(strip=True)
                    self.logger.logger_info.debug(f"Extracted price: {price}")
                else:
                    self.logger.logger_err.error("Price element not found.")
                    
                self.logger.logger_info.debug("No discount found; set to 'N/A'.")
            # Extract product URL
            link_element = tour_item.find('a', href=True)
            if link_element:
                product_url = f"https://www.getyourguide.com/{link_element['href']}".split('?ranking_uuid')[0]
                self.logger.logger_info.debug(f"Extracted product URL: {product_url}")
            else:
                self.logger.logger_err.error("Product URL element not found.")

            # Determine position
            try:
                position = int(tour_item.get('key', position)) + 1 + (page - 1) * self.activity_per_page
                self.logger.logger_info.debug(f"Extracted position : {position}")
            except (ValueError, TypeError):
                position += 1  # Increment position if 'key' is not available or invalid
                self.logger.logger_err.error("Invalid or missing 'key' attribute for position calculation.")

            # Extract number of reviews
            review_element = tour_item.find('div', {'class': 'rating-overall__reviews'}) or \
                             tour_item.find('div', {'class': 'c-activity-rating__label'})
            if review_element:
                amount_reviews = review_element.get_text(strip=True)
                self.logger.logger_info.debug(f"Extracted number of reviews: {amount_reviews}")
            else:
                self.logger.logger_info.debug("No reviews found; set to 'N/A'.")

            # Extract star ratings
            stars_element = tour_item.find('span', {'class': 'rating-overall__rating-number rating-overall__rating-number--right'}) or \
                            tour_item.find('span', {'class': 'c-activity-rating__rating'}) or \
                            tour_item.find('div', {'class': 'c-activity-rating__rating'})
            if stars_element:
                stars = stars_element.get_text(strip=True)
                self.logger.logger_info.debug(f"Extracted stars: {stars}")
            else:
                self.logger.logger_info.debug("No star ratings found; set to 'N/A'.")

            # Extract booking status
            booked_element = tour_item.find('span', {'class': 'c-marketplace-badge c-marketplace-badge--secondary'})
            if booked_element:
                booked = booked_element.get_text(strip=True)
                self.logger.logger_info.debug(f"Extracted booking status: {booked}")
            else:
                self.logger.logger_info.debug("No booking status found; set to 'N/A'.")

            # Extract new activity badge
            new_activity_element = tour_item.find('span', {'class': 'activity-info__badge c-marketplace-badge c-marketplace-badge--secondary'})
            if new_activity_element:
                new_activity = new_activity_element.get_text(strip=True)
                self.logger.logger_info.debug(f"Extracted new activity badge: {new_activity}")
            else:
                self.logger.logger_info.debug("No new activity badge found; set to 'N/A'.")

            # Compile all extracted data into a list
            product_data = [
                title,
                product_url,
                price,
                stars,
                amount_reviews,
                discount,
                tour_item.get_text(strip=True),
                self.date_today,
                position,
                category,
                booked,
                'GYG',
                city
            ]

            return product_data

        except Exception as e:
            self.logger.logger_err.error(f"Error extracting data from tour item: {e}")
            raise
