# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.webdriver import WebDriver
from bs4 import BeautifulSoup
import time
import pandas as pd
import datetime
from selenium.webdriver.common.action_chains import ActionChains
import os
import shutil
import logging
import traceback
import re
import csv
from azure.storage.blob import BlobServiceClient
import io 
import importlib
from azure.communication.email import EmailClient


# %%
class FilePathManager:
    def __init__(self, site, city):
        self.site = site
        self.city = city
        self.date_today = datetime.date.today().strftime("%Y-%m-%d")
        self.date_today = '2024-09-30'  # Uncomment for fixed date testing

        # Define the file paths
        self.output = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/{self.site}/Daily'
        self.archive_folder = fr'{self.output}/Archive'
        self.file_path_done = fr'{self.output}/{self.date_today}-DONE-{self.site}.csv'
        self.file_path_done_city = fr'{self.output}/{self.date_today}-{self.city}-{self.site}.csv'
        self.file_path_output = fr"{self.output}/{self.site} - {self.date_today}.xlsx"
        self.link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/{self.site}_links.csv'
        self.logs_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/{self.site}'
        self.storage_account_name = "storagemyotas"
        self.storage_account_key = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="
        # Local file path
        self.local_file_path = f"{self.output}/{self.site} - {self.date_today}.xlsx"
        self.file_path_csv_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_{self.site}.csv"
        self.file_path_xlsx_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_{self.site}.xlsx"
        # Azure Storage containers and blob name
        self.container_name_raw = f"raw/daily/{self.site}"
        self.container_name_refined = f"refined/daily/{self.site}"
        self.blob_name = fr'{self.site} - {self.date_today}.xlsx'

        # Logs processed path
        self.file_path_logs_processed = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed/{self.blob_name.split(".")[0]}'

    def get_file_paths(self):
        return {
            'output': self.output,
            'archive_folder': self.archive_folder,
            'file_path_done': self.file_path_done,
            'file_path_done_city': self.file_path_done_city,
            'file_path_output': self.file_path_output,
            'link_file': self.link_file,
            'logs_path': self.logs_path,
            'local_file_path': self.local_file_path,
            'container_name_raw': self.container_name_raw,
            'container_name_refined': self.container_name_refined,
            'blob_name': self.blob_name,
            'file_path_logs_processed': self.file_path_logs_processed,
            "storage_account_name": self.storage_account_name,
            "storage_account_key": self.storage_account_key,
            "date_today": self.date_today,
            'file_path_csv_operator': self.file_path_csv_operator,
            'file_path_xlsx_operator': self.file_path_xlsx_operator
        }

# %%
# LoggerManager class to handle logging configuration and operations
class LoggerManager:
    def __init__(self, file_manager):
        self.logs_path = file_manager.logs_path
        self.ensure_log_folder_exists()  # Ensure log folder exists

        # Create logger objects for error, info, and done logs
        self.logger_err = logging.getLogger('Error_logger')
        self.logger_err.setLevel(logging.DEBUG)

        self.logger_info = logging.getLogger('Info_logger')
        self.logger_info.setLevel(logging.DEBUG)

        self.logger_done = logging.getLogger('Done_logger')
        self.logger_done.setLevel(logging.DEBUG)

        # Create handlers
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.DEBUG)

        # Dynamically create paths for each log type based on current year/month
        current_log_path = self.get_current_log_path()
        self.fh_error = logging.FileHandler(os.path.join(current_log_path, 'error_logs.log'))
        self.fh_error.setLevel(logging.DEBUG)

        self.fh_info = logging.FileHandler(os.path.join(current_log_path, 'info_logs.log'))
        self.fh_info.setLevel(logging.INFO)

        self.fh_done = logging.FileHandler(os.path.join(current_log_path, 'done_logs.log'))
        self.fh_done.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Add formatter to handlers
        self.ch.setFormatter(formatter)
        self.fh_error.setFormatter(formatter)
        self.fh_info.setFormatter(formatter)
        self.fh_done.setFormatter(formatter)

        # Add handlers to loggers
        self.logger_err.addHandler(self.ch)
        self.logger_err.addHandler(self.fh_error)

        self.logger_info.addHandler(self.ch)
        self.logger_info.addHandler(self.fh_info)

        self.logger_done.addHandler(self.ch)
        self.logger_done.addHandler(self.fh_done)

    def get_current_log_path(self):
        """Returns the path for the current year's and month's logs."""
        now = datetime.datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        log_folder = os.path.join(self.logs_path, year, month)

        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        return log_folder

    def ensure_log_folder_exists(self):
        """Ensures the main logs folder exists."""
        if not os.path.exists(self.logs_path):
            os.makedirs(self.logs_path)
# %%
class ProductScraperHeadout:
    def __init__(self, url, city, css_selectors, file_manager, logger):
        self.logger = logger
        self.url = url
        self.city = city
        self.file_manager = file_manager
        self.css_currency = css_selectors['currency']
        self.css_currency_list = css_selectors['currency_list']
        self.css_products_count = css_selectors['products_count']
        self.css_product_card = css_selectors['product_card']
        self.css_tour_price = css_selectors['tour_price']
        self.css_tour_price_discount = css_selectors['tour_price_discount']
        self.css_ratings = css_selectors['ratings']
        self.css_review_count = css_selectors['review_count']
        self.css_category_label = css_selectors['category_label']
        self.driver = self.initilize_driver()

        self.logger.logger_info.info("Successfully initiated ProductScraper for city: %s", self.city)


    def initilize_driver(self) -> WebDriver:
        try:
            self.logger.logger_info.info("Initializing the Chrome driver and logging into the website")

            # Setting up Chrome options
            options = webdriver.ChromeOptions()
            # options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.add_argument('--blink-settings=imagesEnabled=false')

            # Initialize the Chrome driver
            driver = webdriver.Chrome(options=options)
            driver.maximize_window()
            
            return driver

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred during login: {e}")
            raise
        
    def quit_driver(self, driver: WebDriver) -> None:
        driver.quit()    
    
    def get_url(self):
        self.driver.get(self.url)
        time.sleep(1)

    def select_currency(self):
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

    def load_all_products(self, products_count, scroll_attempts=5, scroll_step=200):
        # Load the page with all products
        self.driver.get(f"{self.url}?limit={products_count}")
        time.sleep(3)
        
        total_height = self.driver.execute_script("return document.body.scrollHeight") * 0.9
        target_scroll_increment = total_height / scroll_attempts
        current_scroll_position = 0

        for _ in range(scroll_attempts):
            target_scroll_position = current_scroll_position + target_scroll_increment
            
            while current_scroll_position < target_scroll_position:
                self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                current_scroll_position += scroll_step
                time.sleep(0.01)  # Fast scrolling
            
            time.sleep(1)  # Allow content to load
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if current_scroll_position + self.driver.execute_script("return window.innerHeight") >= new_height:
                break

    def scrape_products(self, global_category=False):
        products = self.driver.find_elements(By.CSS_SELECTOR, self.css_product_card)
        data = []
        position = 1
        date_today = self.file_manager.date_today
        product_site = self.file_manager.site
        
        for product in products:
            product_data = self.extract_product_data(product, position, date_today, product_site, global_category)
            data.append(product_data)
            position += 1
        
        return pd.DataFrame(data, columns=['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto'])

    def extract_product_data(self, product, position, date_today, product_site, global_category=False):
        product_title = product.find_element(By.TAG_NAME, 'a').text
        product_url = product.find_element(By.TAG_NAME, 'a').get_attribute('href')

        try:
            product_price = product.find_element(By.CSS_SELECTOR, self.css_tour_price).text
        except:
            product_price = "N/A"

        try:
            product_discount_price = product.find_element(By.CSS_SELECTOR, self.css_tour_price_discount).text
            if product_discount_price == "from":
                product_discount_price = "N/A"
        except:
            product_discount_price = "N/A"

        if product_discount_price != 'N/A' :
            product_discount_price, product_price = product_price, product_discount_price

        product_ratings = product.find_element(By.CSS_SELECTOR, self.css_ratings).text
        
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
            product_title, product_url, product_price, product_ratings, product_review_count,
            product_discount_price, date_today, position, product_category, product_site, self.city
        ]
    def save_to_csv(self, df):

        self.quit_driver(self.driver)
        # Save the DataFrame to CSV using paths from FilePathManager
        file_path = self.file_manager.get_file_paths()['file_path_done_city']
        df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
        self.logger.logger_done.info(f"Rows: {len(df)} Data saved to {file_path}")

    def is_city_already_done(self):
        file_path = self.file_manager.get_file_paths()['file_path_done_city']
        return os.path.exists(file_path)  # Check if the file already exists
    def is_today_already_done(self):
        file_path_output = self.file_manager.get_file_paths()['file_path_output']
        return os.path.exists(file_path_output)  # Check if the file already exists

     # New method to combine CSV files into a single Excel file
    def combine_csv_to_xlsx(self):
        csv_files_locations = self.file_manager.get_file_paths()['output']
        archive_folder = self.file_manager.get_file_paths()['archive_folder']
        date_today = self.file_manager.get_file_paths()['date_today']
        file_path_output = self.file_manager.get_file_paths()['file_path_output']
        # Get all CSV files with the specified date prefix in the output directory
        csv_files = [file for file in os.listdir(csv_files_locations) if file.endswith('.csv') and file.startswith(date_today)]

        # Check if no CSV files were found
        if not csv_files:
            self.logger.logger_info.info(f"No CSV files found with the date prefix '{date_today}'")
            return
        # Specify the output Excel file path and name
        # Ensure the archive folder exists
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)    

        writer = pd.ExcelWriter(file_path_output, engine='xlsxwriter')
        
        for csv_file in csv_files:
            csv_path = os.path.join(csv_files_locations, csv_file)
            
            # Generate a sheet name based on the CSV file name
            sheet_name = os.path.splitext(csv_file)[0]
            sheet_name = sheet_name.split(date_today + '-')[1].split(f'-{self.file_manager.site}')[0]
            
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

class ProductScraperMusment:
    def __init__(self, url, city, css_selectors, file_manager, logger, provider=False):
        self.logger = logger
        self.url = url
        self.city = city
        self.file_manager = file_manager
        self.css_currency = css_selectors['currency']
        self.css_currency_list = css_selectors['currency_list']
        self.css_products_count = css_selectors['products_count']
        self.css_product_card = css_selectors['product_card']
        self.css_tour_price = css_selectors['tour_price']
        self.css_tour_price_discount = css_selectors['tour_price_discount']
        self.css_ratings = css_selectors['ratings']
        self.css_review_count = css_selectors['review_count']
        self.css_category_label = css_selectors['category_label']
        self.css_view_more_button = css_selectors['view_more_button']
        self.css_cookies_banner_decline = css_selectors['cookies_banner']
        self.css_sort_by = css_selectors['sort_by']
        self.css_option_rating = css_selectors['option_rating']
        self.css_option_popularity = css_selectors['option_popularity']
        self.js_shadow_root = css_selectors['js_script_for_shadow_root']
        if provider:
            self.css_provider = css_selectors['provider']
                                                
        self.driver = self.initilize_driver()
        self.wait = WebDriverWait(self.driver, 10)  # You can adjust the timeout value

        self.logger.logger_info.info("Successfully initiated ProductScraper for city: %s", self.city)


    def initilize_driver(self) -> WebDriver:
        try:
            self.logger.logger_info.info("Initializing the Chrome driver and logging into the website")

            # Setting up Chrome options
            options = webdriver.ChromeOptions()
            # options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.add_argument('--blink-settings=imagesEnabled=false')

            # Initialize the Chrome driver
            driver = webdriver.Chrome(options=options)
            driver.maximize_window()
            
            return driver

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred during login: {e}")
            raise
        
    def quit_driver(self, driver: WebDriver) -> None:
        driver.quit()    
    
    def get_url(self):
        self.driver.get(self.url)
        time.sleep(1)
        self.close_cookies_banner()

    def select_currency(self):
        currency_button = self.driver.find_element(By.CSS_SELECTOR, self.css_currency)
        if "eur" not in currency_button.text.lower():
            currency_button.click()
            currency_list = self.driver.find_elements(By.CSS_SELECTOR, self.css_currency_list)
            for currency in currency_list:
                if 'eur' in currency.text.lower():
                    currency.click()
                    time.sleep(2)
                    break

    def change_currency_gyg(self):
        currency_switcher_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.css_switcher_button))) #"//a[@class='option option-currency']"
        # hover over the currency switcher button to show the menu
        actions = ActionChains(self.driver)
        actions.move_to_element(currency_switcher_button).perform()
        currency_switcher_button .click()
        # wait for the EUR currency option to be clickable
        currency_option = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.css_currency_suymbol.replace("SYMBOL", self.currency_symbol)))) #"//li[@class='currency-modal-picker__item-parent item__currency-modal item__currency-modal--EUR']"
        # click on the EUR currency option to change the currency
        currency_option.click()
    def select_currency_gyg(self):
            EUR_City = [
            "Amsterdam", "Athens", "Barcelona", "Berlin", "Dublin", "Dubrovnik", "Florence", "Istanbul",
            "Krakow", "Lisbon", "Madrid", "Milan", "Naples", "Paris", "Porto", "Rome", "Palermo", "Venice",
            "Taormina", "Capri", "Sorrento", "Mount-Etna", "Mount-Vesuvius", "Herculaneum", "Amalfi-Coast",
            "Pompeii"
            ]

            USD_City = [
                "Las-Vegas", "New-York-City", "Cancun", "Dubai"
            ]

            GBP_City = [
                "Edinburgh", "London"
            ]

            if self.city in EUR_City:
                self.currency_symbol = "EUR"
            elif self.city in USD_City:
                self.currency_symbol = "USD"
            elif self.city in GBP_City:
                self.currency_symbol = "GBP"
            #   VERIFY IF THE CURRENCY IS CORRECT
            login_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.css_login_button))) # Create new 

            login_button.click()

            currency = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.css_currency))) # swithc with currency
            currency = currency.text.strip()
            
            if self.city in EUR_City:
                if 'EUR' in currency:
                     pass
                self.change_currency_gyg()
            
            elif self.city in USD_City:
                if 'USD' in currency:
                     pass
                self.change_currency_gyg()

            elif self.city in GBP_City:
                if 'GBP' in currency:
                     pass
                self.change_currency_gyg()

    def get_product_count(self):
        products_count_selenium = self.driver.find_element(By.CSS_SELECTOR, self.css_products_count)
        if 'Loading' in products_count_selenium.get_attribute('innerHTML'):
            time.sleep(1.5)
        products_count_selenium = self.driver.find_element(By.CSS_SELECTOR, self.css_products_count)
        products_count = int(products_count_selenium.get_attribute('innerHTML').split(' ')[0])
        return products_count
    
    def get_provider_name(self):

        provider_name = self.driver.find_element(By.CSS_SELECTOR, self.css_provider)
        return provider_name
        
    def _save_dataframe(self, df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            workbook.strings_to_urls = False
            df.to_excel(writer, index=False, sheet_name='AllLinks')
        with open(self.file_manager.get_file_paths()['file_path_xlsx_operator'], 'wb') as f:
            f.write(output.getvalue())

    def load_all_products_by_button(self, products_count, scroll_step=-100):
        

        current_scroll_position = self.driver.execute_script("return document.body.scrollHeight")

        while current_scroll_position > 0:
            self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
            current_scroll_position += scroll_step
            time.sleep(0.01)  # Fast scrolling
                # Scroll the button into view



        current_count_of_products = 0

        
        
        while current_count_of_products < products_count * 0.8:

            self.logger.logger_info.info(f"Current count of products: {current_count_of_products} Products count: {products_count} 80% --> {products_count*0.8}")
            current_count_of_products = len(self.driver.find_elements(By.CSS_SELECTOR, self.css_product_card))
            try:
                view_more_button = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, self.css_view_more_button)))
            except:
                if current_count_of_products > 400 or current_count_of_products > products_count * 0.8:
                    break
            # Scroll the button into view
            
            # Scroll the button into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", view_more_button)

            # Use JavaScript to click the button
            self.driver.execute_script("arguments[0].click();", view_more_button)
            # view_more_button.click()
            time.sleep(1.5)

        

    def load_all_products_by_url(self, products_count, scroll_attempts=5, scroll_step=200):
        # Load the page with all products
        self.driver.get(f"{self.url}?limit={products_count}")
        time.sleep(3)
        
        total_height = self.driver.execute_script("return document.body.scrollHeight") * 0.9
        target_scroll_increment = total_height / scroll_attempts
        current_scroll_position = 0

        for _ in range(scroll_attempts):
            target_scroll_position = current_scroll_position + target_scroll_increment
            
            while current_scroll_position < target_scroll_position:
                self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                current_scroll_position += scroll_step
                time.sleep(0.01)  # Fast scrolling
            
            time.sleep(1)  # Allow content to load
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if current_scroll_position + self.driver.execute_script("return window.innerHeight") >= new_height:
                break

    def close_cookies_banner(self):
        time.sleep(1)
        # Get the shadow root and then find the button
        shadow_root = self.driver.execute_script(self.js_shadow_root)
        decline_button = shadow_root.find_element(By.CSS_SELECTOR, self.css_cookies_banner_decline)
        decline_button.click()
    
    def sort_products_by_popularity(self):

        button_sort_by = self.driver.find_element(By.CSS_SELECTOR, self.css_sort_by)
        self.driver.execute_script(f"window.scrollTo(0, {button_sort_by.location['y']/2});")
        button_sort_by.click()
        self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, self.css_option_rating))).click()
        self.driver.find_element(By.CSS_SELECTOR, self.css_option_popularity).click()
        time.sleep(2)
    
    def scrape_products(self, global_category=False):
        
        # self.sort_products_by_popularity()

        products = self.driver.find_elements(By.CSS_SELECTOR, self.css_product_card)
        data = []
        position = 1
        date_today = self.file_manager.date_today
        product_site = self.file_manager.site
        
        for product in products:
            product_data = self.extract_product_data(product, position, date_today, product_site, global_category)
            data.append(product_data)
            position += 1
        
        return pd.DataFrame(data, columns=['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto'])

    def extract_product_data(self, product, position, date_today, product_site, global_category=False):
        product_title = product.find_element(By.TAG_NAME, 'a').text
        product_url = product.find_element(By.TAG_NAME, 'a').get_attribute('href')
        
        try:
            product_price = product.find_element(By.CSS_SELECTOR, self.css_tour_price).text
        except:
            product_price = "N/A"

        try:
            product_discount_price = product.find_element(By.CSS_SELECTOR, self.css_tour_price_discount).text
            if product_discount_price == "from":
                product_discount_price = "N/A"
        except:
            product_discount_price = "N/A"

        if product_discount_price != 'N/A' :
            product_discount_price, product_price = product_price, product_discount_price

        try:
            product_ratings = product.find_element(By.CSS_SELECTOR, self.css_ratings).text.split("/")[0]
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
            product_title, product_url, product_price, product_ratings, product_review_count,
            product_discount_price, date_today, position, product_category, product_site, self.city
        ]
    def save_to_csv(self, df):

        self.quit_driver(self.driver)
        # Save the DataFrame to CSV using paths from FilePathManager
        file_path = self.file_manager.get_file_paths()['file_path_done_city']
        df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
        self.logger.logger_done.info(f"Rows: {len(df)} Data saved to {file_path}")

    def is_today_already_done(self):
        file_path_output = self.file_manager.get_file_paths()['file_path_output']
        return os.path.exists(file_path_output)  # Check if the file already exists
    
    def is_city_already_done(self):
        file_path = self.file_manager.get_file_paths()['file_path_done_city']
        return os.path.exists(file_path)  # Check if the file already exists
    

     # New method to combine CSV files into a single Excel file
    def combine_csv_to_xlsx(self):
        csv_files_locations = self.file_manager.get_file_paths()['output']
        archive_folder = self.file_manager.get_file_paths()['archive_folder']
        date_today = self.file_manager.get_file_paths()['date_today']
        file_path_output = self.file_manager.get_file_paths()['file_path_output']
        # Get all CSV files with the specified date prefix in the output directory
        csv_files = [file for file in os.listdir(csv_files_locations) if file.endswith('.csv') and file.startswith(date_today)]

        # Check if no CSV files were found
        if not csv_files:
            self.logger.logger_info.info(f"No CSV files found with the date prefix '{date_today}'")
            return
        # Specify the output Excel file path and name
        # Ensure the archive folder exists
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)    

        writer = pd.ExcelWriter(file_path_output, engine='xlsxwriter')
        
        for csv_file in csv_files:
            csv_path = os.path.join(csv_files_locations, csv_file)
            
            # Generate a sheet name based on the CSV file name
            sheet_name = os.path.splitext(csv_file)[0]
            sheet_name = sheet_name.split(date_today + '-')[1].split(f'-{self.file_manager.site}')[0]
            
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

             

# %%
class AzureBlobUploader:
    def __init__(self, file_manager, logger):
        self.file_manager = file_manager
        self.storage_account_name = self.file_manager.get_file_paths()['storage_account_name']
        self.storage_account_key = self.file_manager.get_file_paths()['storage_account_key']
        self.container_name_raw = self.file_manager.get_file_paths()['container_name_raw']
        self.container_name_refined = self.file_manager.get_file_paths()['container_name_refined']
        self.blob_name = self.file_manager.get_file_paths()['blob_name']
        self.file_path_output = self.file_manager.get_file_paths()['file_path_output']
        self.logger = logger
        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key};EndpointSuffix=core.windows.net"

        self.logger.logger_info.info("Sucessfuly initiated AzureBlobUploader")

    def upload_excel_to_azure_storage_account(self):
        """
        Uploads the Excel file to Azure Blob Storage under the "raw" container.
        """
        try:
            # Create a BlobServiceClient object using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

            # Get a reference to the container
            container_client = blob_service_client.get_container_client(self.container_name_raw)

            # Upload the file to Azure Blob Storage
            with open(self.file_path_output, "rb") as file:
                container_client.upload_blob(name=self.blob_name, data=file)
            
            self.logger.logger_done.info("File uploaded successfully to Azure Blob Storage (raw).")

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while uploading to raw storage: {e}")
    def is_valid_date(self, date_str):
        try:
            pd.to_datetime(date_str, format='%Y-%m-%d', errors='raise')
            return True
        except ValueError:
            return False
        
    def transform_upload_to_refined(self):
        """
        Transforms and uploads the Excel file to Azure Blob Storage under the "refined" container.
        """
        exclude_sheets = ['Sheet1', 'Data', 'Re-Run', 'DONE']
        output_file_path = "temp_file.xlsx"  # Temporary file for transformation

        try:
            # Read the Excel file into a Pandas DataFrame
            excel_data = pd.read_excel(self.file_path_output, sheet_name=None)

            # Write the transformed data to a new Excel file
            with pd.ExcelWriter(output_file_path) as writer:
                for sheet_name, df in excel_data.items():
                    if sheet_name in exclude_sheets:
                        continue

                     # Check 'Data zestawienia' for valid date formats
                    df['Data zestawienia'] = df['Data zestawienia'].astype(str)
                    
                    # Filter rows where 'Data zestawienia' does not have a valid date
                    invalid_rows = df[~df['Data zestawienia'].apply(self.is_valid_date)]
                    
                    # Log sheet name and number of invalid rows if found
                    if not invalid_rows.empty:
                        self.logger.logger_err.error(f"Sheet {sheet_name} has {len(invalid_rows)} invalid date entries in 'Data zestawienia' column.")
                        continue

                    # Convert 'Data zestawienia' to YYYY-MM-DD format if valid
                    df['Data zestawienia'] = pd.to_datetime(df['Data zestawienia']).dt.strftime('%Y-%m-%d')

                    # Transform the DataFrame (add your transformation logic here)
                    df['Data zestawienia'] = df['Data zestawienia'].astype('str')
                    df['IloscOpini'] = df['IloscOpini'].astype(str)
                    df['IloscOpini'] = df['IloscOpini'].fillna(0)
                    df['IloscOpini'] = df['IloscOpini'].str.replace('(', '').str.replace(')','')
                    df['IloscOpini'] = df['IloscOpini'].apply(lambda x: int(float(x.replace('K', '')) * 1000) if isinstance(x, str) and 'K' in x else x)

                    df['Opinia'] = df['Opinia'].astype(str)
                    df['Opinia'] = df['Opinia'].fillna('N/A')
                    df['Opinia'] = df['Opinia'].map(lambda x: x.replace("NEW", '') if isinstance(x, str) else x)

                    df = df[df['Tytul'] != 'Tytul']
                    df = df[df['Data zestawienia'] != 'Data zestawienia']
                    df = df[df['Data zestawienia'].str.len() > 4]

                    df['Cena'] = df['Cena'].str.lower()
                    df['Cena'] = df['Cena'].map(lambda x: x.split('from')[-1] if isinstance(x, str) and 'from' in x else x)
                    df['Cena'] = df['Cena'].apply(lambda x: str(x).replace('€', '').replace('$', '').replace('£', '').strip() if isinstance(x, str) else x)
                    df['Cena'] = df['Cena'].map(lambda x: x.split('per person')[0] if isinstance(x, str) and 'per person' in x.lower() else x)
                    df['Cena'] = df['Cena'].map(lambda x: x.split('per group')[0] if isinstance(x, str) and 'per group' in x.lower() else x)

                    df['Przecena'] = df['Przecena'].apply(lambda x: str(x).replace('€', '').replace('$', '').replace('£', '').strip() if isinstance(x, str) else x)
                    df['Przecena'] = df['Przecena'].map(lambda x: x.split('per person')[0] if isinstance(x, str) and 'per person' in x.lower() else x)
                    df['Przecena'] = df['Przecena'].map(lambda x: x.split('per group')[0] if isinstance(x, str) and 'per group' in x.lower() else x)


                    # Apply str.replace only if the value is a string


                    df.to_excel(writer, sheet_name=sheet_name, index=False)
    # Upload the transformed Excel file to Azure Blob Storage
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            container_client = blob_service_client.get_container_client(self.container_name_refined)

            with open(output_file_path, "rb") as data:
                container_client.upload_blob(name=self.blob_name, data=data)
            
            self.logger.logger_done.info("File uploaded successfully to Azure Blob Storage (refined).")

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while transforming and uploading to refined storage: {e}")
            
        finally:
            # Clean up the temporary file
            if os.path.exists(output_file_path):
                os.remove(output_file_path)

class EmailSenderAlerts:
    def __init__(self, email_adress, product, alert_date, price_before, price_after) -> None:
        self.email_adress = email_adress
        self.product = product
        self.alert_date = alert_date
        self.price_before = price_before
        self.price_after = price_after
        self.access_key = "UN7iDkL+01/1HUHqRVgxYIxUZ4nGh6JUnKUW+x5CE5jGPgR9DLkKb4/EEgX74s1iKinxnaRANqRk6TNDzhyZ5w=="

   
    def main(self):
        try:
            connection_string = f"endpoint=https://cs-emailsender-myotas.germany.communication.azure.com/;accesskey={self.access_key}"
            client = EmailClient.from_connection_string(connection_string)

            message = {
                "senderAddress": "DoNotReply@6befcbca-8357-4801-8832-a8e8ffcf5b4c.azurecomm.net",
                "recipients":  {
                    "to": [{"address": f"{self.email_adress}" }],
                },
                "content": {
                    "subject": f"MyOTAs - Price Update for Product {self.product}",
                    "plainText": f"Alert: The price for product ABC123 has changed from {self.price_before} to {self.price_after} for {self.alert_date}. 
                    
                    
                    MyOTAs Team",
                }
            }

            poller = client.begin_send(message)
            result = poller.result()

        except Exception as ex:
            print(ex)
# %%



