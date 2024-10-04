# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.webdriver import WebDriver
import time
import pandas as pd
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

import shutil
import io


class ScraperBase:
    def __init__(self, url, city, css_selectors, file_manager, logger):
        self.logger = logger
        self.url = url
        self.city = city
        self.file_manager = file_manager
        self.css_selectors = css_selectors.copy()  # Make a copy to avoid mutation
        self.date_today = self.file_manager.date_today
        self.site = self.file_manager.site
        # Initialize common CSS selectors
        self.css_currency = self.css_selectors.get('currency')
        self.css_currency_list = self.css_selectors.get('currency_list')
        self.css_products_count = self.css_selectors.get('products_count')
        self.css_product_card = self.css_selectors.get('product_card')
        self.css_tour_price = self.css_selectors.get('tour_price')
        self.css_tour_price_discount = self.css_selectors.get('tour_price_discount')
        self.css_ratings = self.css_selectors.get('ratings')
        self.css_review_count = self.css_selectors.get('review_count')
        self.css_category_label = self.css_selectors.get('category_label')

        # Initialize the driver
        self.driver = self.initialize_driver()
        self.logger.logger_info.info("Successfully initiated Scraper for city: %s", self.city)

    def initialize_driver(self) -> WebDriver:
        try:
            self.logger.logger_info.info("Initializing the Chrome driver")

            # Setting up Chrome options
            options = webdriver.ChromeOptions()
            options.add_argument('--blink-settings=imagesEnabled=false')

            # Initialize the Chrome driver
            driver = webdriver.Chrome(options=options)
            driver.maximize_window()
            return driver

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred during driver initialization: {e}")
            raise

    def quit_driver(self) -> None:
        self.driver.quit()

    def get_url(self):
        self.driver.get(self.url)
        time.sleep(1)

    def save_dataframe(self, df, file_path):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            workbook.strings_to_urls = False
            df.to_excel(writer, index=False, sheet_name='AllLinks')
        with open(file_path, 'wb') as f:
            f.write(output.getvalue())
    def select_currency(self):
        # Base method; can be overridden in subclasses
        currency_button = self.driver.find_element(By.CSS_SELECTOR, self.css_currency)
        if "EUR" not in currency_button.get_attribute('innerHTML'):
            currency_button.click()
            currency_list = self.driver.find_elements(By.CSS_SELECTOR, self.css_currency_list)
            for currency in currency_list:
                if 'EUR' in currency.get_attribute('innerHTML'):
                    currency.click()
                    break

    def get_product_count(self):
        products_count_selenium = self.driver.find_element(By.CSS_SELECTOR, self.css_products_count)
        if 'Loading' in products_count_selenium.get_attribute('innerHTML'):
            time.sleep(1.5)
        products_count_selenium = self.driver.find_element(By.CSS_SELECTOR, self.css_products_count)
        products_count = int(products_count_selenium.get_attribute('innerHTML').split(' ')[0])
        return products_count

    def scrape_products(self, global_category=False):
        products = self.driver.find_elements(By.CSS_SELECTOR, self.css_product_card)
        data = []
        position = 1

        for product in products:
            product_data = self.extract_product_data(
                product, position, global_category
            )
            data.append(product_data)
            position += 1

        return pd.DataFrame(
            data,
            columns=[
                'Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini',
                'Przecena', 'Data zestawienia', 'Pozycja', 'Kategoria',
                'SiteUse', 'Miasto'
            ],
        )

    def extract_product_data(
        self, product, position, global_category=False
    ):
        # Base method; can be overridden in subclasses
        product_title = product.find_element(By.TAG_NAME, 'a').text
        product_url = product.find_element(By.TAG_NAME, 'a').get_attribute('href')

        try:
            product_price = product.find_element(By.CSS_SELECTOR, self.css_tour_price).text
        except:
            product_price = "N/A"

        try:
            product_discount_price = product.find_element(By.CSS_SELECTOR, self.css_tour_price_discount).text
            if product_discount_price.lower() == "from":
                product_discount_price = "N/A"
        except:
            product_discount_price = "N/A"

        if product_discount_price != 'N/A':
            product_discount_price, product_price = product_price, product_discount_price

        try:
            product_ratings = product.find_element(By.CSS_SELECTOR, self.css_ratings).text
        except:
            product_ratings = "N/A"

        try:
            product_review_count = product.find_element(By.CSS_SELECTOR, self.css_review_count).text
        except:
            product_review_count = "N/A"

        try:
            product_category = product.find_element(By.CSS_SELECTOR, self.css_category_label).text
        except:
            product_category = "N/A"

        if global_category:
            product_category = "Global"

        return [
            product_title, product_url, product_price, product_ratings,
            product_review_count, product_discount_price, self.date_today, position,
            product_category, self.site, self.city
        ]

    def save_to_csv(self, df):
        self.quit_driver()
        # Save the DataFrame to CSV using paths from FilePathManager
        file_path = self.file_manager.get_file_paths()['file_path_done_city']
        df.to_csv(
            file_path, header=not os.path.exists(file_path), index=False, mode='a'
        )
        self.logger.logger_done.info(f"Rows: {len(df)} Data saved to {file_path}")

    def is_city_already_done(self):
        file_path = self.file_manager.get_file_paths()['file_path_done_city']
        return os.path.exists(file_path)

    def is_today_already_done(self):
        file_path_output = self.file_manager.get_file_paths()['file_path_output']
        return os.path.exists(file_path_output)

    def combine_csv_to_xlsx(self):
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

        writer = pd.ExcelWriter(file_path_output, engine='xlsxwriter')

        for csv_file in csv_files:
            csv_path = os.path.join(csv_files_locations, csv_file)
            sheet_name = os.path.splitext(csv_file)[0]
            sheet_name = sheet_name.split(self.date_today + '-')[1].split(
                f'-{self.site}'
            )[0]

            df = pd.read_csv(csv_path)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        writer.close()
        self.logger.logger_done.info(f"Combined CSV files into '{file_path_output}'")

        # Move the original CSV files to the archive folder
        for csv_file in csv_files:
            csv_path = os.path.join(csv_files_locations, csv_file)
            destination_path = os.path.join(archive_folder, csv_file)
            try:
                shutil.move(csv_path, destination_path)
                self.logger.logger_info.info(f"Moved {csv_file} to the archive folder.")
            except FileNotFoundError as e:
                self.logger.logger_err.error(f"Error moving {csv_file}: {str(e)}")
