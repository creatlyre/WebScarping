# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.webdriver import WebDriver
from bs4 import BeautifulSoup
import time
import pandas as pd
import datetime
import os
import shutil
import logging
import traceback
import re
from azure.storage.blob import BlobServiceClient
import io
# from azure.communication.email import EmailClient




# %%
class FilePathManager:
    def __init__(self, site, city, manual_overdrive_date=False, manual_date='2024-09-30'):
        self.site = site
        self.city = city
        self.date_today = datetime.date.today().strftime("%Y-%m-%d")
        if manual_overdrive_date:
            self.date_today = manual_date # For fixed date testing

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
            'file_path_xlsx_operator': self.file_path_xlsx_operator,
        }

class FilePathManagerFuturePrice(FilePathManager):
        def __init__(self, site, city, adults, language, manual_overdrive_date=False, manual_date='2024-09-30'):
            super().__init__(site, city, manual_overdrive_date, manual_date)
            self.adults = adults
            self.language = language
            self.output = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/{self.site}/future_price'

            self.extraction_date = datetime.datetime.now().strftime('%Y-%m-%d %H:00:00')
            self.extraction_date_save_format = f"{self.extraction_date.replace(' ', '_').replace(':','-')}_{self.language}_{self.adults}"
            # Set the path of the local file
            # Azure Storage containers and blob name
            self.container_name_raw = f"raw/future_price/{self.site}"
            self.container_name_refined = f"refined/future_price/{self.site}"
            self.output_file_path = f"{self.output}/{self.extraction_date_save_format}_future_price.xlsx" # AKA output_file_path
            self.blob_name = fr'{self.extraction_date_save_format}_future_price.xlsx'
# %%
# LoggerManager class to handle logging configuration and operations
class LoggerManager:
    def __init__(self, file_manager, application = "daily"):
        self.logs_path = file_manager.logs_path
        self.ensure_log_folder_exists()  # Ensure log folder exists

        # Create logger objects for error, info, and done logs
        self.logger_err = logging.getLogger(f'Error_logger')
        self.logger_err.setLevel(logging.DEBUG)

        self.logger_info = logging.getLogger(f'Info_logger')
        self.logger_info.setLevel(logging.DEBUG)

        self.logger_done = logging.getLogger(f'Done_logger')
        self.logger_done.setLevel(logging.DEBUG)

        # Create handlers
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.DEBUG)

        # Dynamically create paths for each log type based on current year/month
        current_log_path = self.get_current_log_path()
        self.fh_error = logging.FileHandler(os.path.join(current_log_path, f'{application}_error_logs.log'))
        self.fh_error.setLevel(logging.DEBUG)

        self.fh_info = logging.FileHandler(os.path.join(current_log_path, f'{application}_info_logs.log'))
        self.fh_info.setLevel(logging.INFO)

        self.fh_done = logging.FileHandler(os.path.join(current_log_path, f'{application}_done_logs.log'))
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
class LoggerManagerFuturePrice(LoggerManager):
    def __init__(self, file_manager, application="future_price"):
        super().__init__(file_manager, application)
        
        current_log_path = self.get_current_log_path()
        self.logger_statistics = logging.getLogger('Statistics_logger')
        self.logger_statistics.setLevel(logging.DEBUG)
        self.fh_statistics = logging.FileHandler(os.path.join(current_log_path, f'{application}_statistics_logs.log'))
        self.fh_statistics.setLevel(logging.INFO)

        self.logger_statistics.addHandler(self.ch)
        self.logger_statistics.addHandler(self.fh_statistics)
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
                    "plainText": f"Alert: The price for product ABC123 has changed from {self.price_before} to {self.price_after} for {self.alert_date}. MyOTAs Team",
                }
            }

            poller = client.begin_send(message)
            result = poller.result()

        except Exception as ex:
            print(ex)
# %%
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

class ScraperHeadout(ScraperBase):
    def __init__(self, url, city, css_selectors, file_manager, logger):
        super().__init__(url, city, css_selectors, file_manager, logger)

    def select_currency(self):
        currency_button = self.driver.find_element(By.CSS_SELECTOR, self.css_currency)
        if "EUR" not in currency_button.get_attribute('innerHTML'):
            currency_button.click()
            currency_list = self.driver.find_elements(
                By.CSS_SELECTOR, self.css_currency_list
            )
            for currency in currency_list:
                if 'EUR' in currency.get_attribute('innerHTML'):
                    currency.click()
                    break

    def get_product_count(self):
        products_count_selenium = self.driver.find_element(
            By.CSS_SELECTOR, self.css_products_count
        )
        if 'Loading' in products_count_selenium.get_attribute('innerHTML'):
            time.sleep(1.5)
        products_count_selenium = self.driver.find_element(
            By.CSS_SELECTOR, self.css_products_count
        )
        products_count = int(
            products_count_selenium.get_attribute('innerHTML').split(' ')[0]
        )
        return products_count

    def load_all_products(self, products_count, scroll_attempts=5, scroll_step=200):
        self.driver.get(f"{self.url}?limit={products_count}")
        time.sleep(3)

        total_height = self.driver.execute_script(
            "return document.body.scrollHeight"
        ) * 0.9
        target_scroll_increment = total_height / scroll_attempts
        current_scroll_position = 0

        for _ in range(scroll_attempts):
            target_scroll_position = current_scroll_position + target_scroll_increment

            while current_scroll_position < target_scroll_position:
                self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                current_scroll_position += scroll_step
                time.sleep(0.01)  # Fast scrolling

            time.sleep(1)  # Allow content to load
            new_height = self.driver.execute_script(
                "return document.body.scrollHeight"
            )
            if current_scroll_position + self.driver.execute_script(
                "return window.innerHeight"
            ) >= new_height:
                break

    def extract_product_data(
        self, product, position, global_category=False
    ):
        product_title = product.find_element(By.TAG_NAME, 'a').text
        product_url = product.find_element(By.TAG_NAME, 'a').get_attribute('href')

        try:
            product_price = product.find_element(
                By.CSS_SELECTOR, self.css_tour_price
            ).text
        except:
            product_price = "N/A"

        try:
            product_discount_price = product.find_element(
                By.CSS_SELECTOR, self.css_tour_price_discount
            ).text
            if product_discount_price == "from":
                product_discount_price = "N/A"
        except:
            product_discount_price = "N/A"

        if product_discount_price != 'N/A':
            product_discount_price, product_price = product_price, product_discount_price

        product_ratings = product.find_element(
            By.CSS_SELECTOR, self.css_ratings
        ).text

        try:
            product_review_count = product.find_element(
                By.CSS_SELECTOR, self.css_review_count
            ).text
        except:
            product_review_count = "N/A"

        try:
            product_category = product.find_element(
                By.CSS_SELECTOR, self.css_category_label
            ).text
        except:
            product_category = "N/A"

        if global_category:
            product_category = "Global"

        return [
            product_title, product_url, product_price, product_ratings,
            product_review_count, product_discount_price, self.date_today, position,
            product_category, self.site, self.city
        ]

class ScraperMusement(ScraperBase):
    def __init__(self, url, city, css_selectors, file_manager, logger, provider=False):
        super().__init__(url, city, css_selectors, file_manager, logger)

        # Update the css_selectors with Musement-specific selectors
        self.css_selectors = css_selectors
        # Assign Musement-specific selectors to instance variables
        self.css_view_more_button = self.css_selectors.get('view_more_button')
        self.css_cookies_banner_decline = self.css_selectors.get('cookies_banner')
        self.css_sort_by = self.css_selectors.get('sort_by')
        self.css_option_rating = self.css_selectors.get('option_rating')
        self.css_option_popularity = self.css_selectors.get('option_popularity')
        self.js_shadow_root = self.css_selectors.get('js_script_for_shadow_root')
        if provider:
            self.css_provider = self.css_selectors.get('provider')
        self.provider = provider
        self.wait = WebDriverWait(self.driver, 10)


    def get_url(self):
        super().get_url()
        self.close_cookies_banner()

    def close_cookies_banner(self):
        time.sleep(1)
        shadow_root = self.driver.execute_script(self.js_shadow_root)
        decline_button = shadow_root.find_element(
            By.CSS_SELECTOR, self.css_cookies_banner_decline
        )
        decline_button.click()

    def select_currency(self):
        currency_button = self.driver.find_element(By.CSS_SELECTOR, self.css_currency)
        if "eur" not in currency_button.text.lower():
            currency_button.click()
            currency_list = self.driver.find_elements(
                By.CSS_SELECTOR, self.css_currency_list
            )
            for currency in currency_list:
                if 'eur' in currency.text.lower():
                    currency.click()
                    time.sleep(2)
                    break

    def get_product_count(self):
        products_count_selenium = self.driver.find_element(
            By.CSS_SELECTOR, self.css_products_count
        )
        if 'Loading' in products_count_selenium.get_attribute('innerHTML'):
            time.sleep(1.5)
        products_count_selenium = self.driver.find_element(
            By.CSS_SELECTOR, self.css_products_count
        )
        products_count = int(
            products_count_selenium.get_attribute('innerHTML').split(' ')[0]
        )
        return products_count

    def get_provider_name(self):
        try:
            provider_name = self.driver.find_element(
            By.CSS_SELECTOR, self.css_provider
            )
        except:
            provider_name = "Not Found"
        return provider_name

    def load_all_products_by_button(self, products_count, scroll_step=-100):
        current_scroll_position = self.driver.execute_script(
            "return document.body.scrollHeight"
        )

        while current_scroll_position > 0:
            self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
            current_scroll_position += scroll_step
            time.sleep(0.01)

        current_count_of_products = 0

        while current_count_of_products < products_count * 0.8:
            current_count_of_products = len(
                self.driver.find_elements(By.CSS_SELECTOR, self.css_product_card)
            )
            self.logger.logger_info.info(
                f"Current count of products: {current_count_of_products} "
                f"Products count: {products_count} 80% --> {products_count*0.8}"
                f"Will finish the while loop in this iteration"
            )
            try:
                view_more_button = self.wait.until(
                    EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, self.css_view_more_button)
                    )
                )
            except:
                if current_count_of_products > 400 or current_count_of_products > products_count * 0.8:
                    self.logger.logger_info.info(f"Cound't find view more button che")
                    break

            self.driver.execute_script(
                "arguments[0].scrollIntoView(true);", view_more_button
            )
            self.driver.execute_script("arguments[0].click();", view_more_button)
            time.sleep(1.5)

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
        product_title = product.find_element(By.TAG_NAME, 'a').text
        product_url = product.find_element(By.TAG_NAME, 'a').get_attribute('href')

        try:
            product_price = product.find_element(
                By.CSS_SELECTOR, self.css_tour_price
            ).text
        except:
            product_price = "N/A"

        try:
            product_discount_price = product.find_element(
                By.CSS_SELECTOR, self.css_tour_price_discount
            ).text
            if product_discount_price == "from":
                product_discount_price = "N/A"
        except:
            product_discount_price = "N/A"

        if product_discount_price != 'N/A':
            product_discount_price, product_price = product_price, product_discount_price

        try:
            product_ratings = product.find_element(
                By.CSS_SELECTOR, self.css_ratings
            ).text.split("/")[0]
        except:
            product_ratings = "N/A"

        try:
            product_review_count = product.find_element(
                By.CSS_SELECTOR, self.css_review_count
            ).text
        except:
            product_review_count = "N/A"

        try:
            product_category = product.find_element(
                By.CSS_SELECTOR, self.css_category_label
            ).text
        except:
            product_category = "N/A"

        if global_category:
            product_category = "Global"

        return [
            product_title, product_url, product_price, product_ratings,
            product_review_count, product_discount_price, self.date_today, position,
            product_category, self.site, self.city
        ]
    
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
            discount_element = price_element.find_all('span', {'class': 'activity-price__text'})

            if len(discount_element) == 2:
                price = discount_element[0].get_text(strip=True)
                discount = discount_element[1].get_text(strip=True)
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
                datetime.datetime.now().strftime('%Y-%m-%d'),
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

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and transforms the scraped DataFrame for consistency and accuracy.

        Args:
            df (pd.DataFrame): The raw scraped DataFrame.

        Returns:
            pd.DataFrame: The cleaned and transformed DataFrame.
        """
        try:
            # Clean 'Cena' by extracting the numeric part
            df['Cena'] = df['Cena'].apply(lambda x: x.split(' ')[-1] if isinstance(x, str) and ' ' in x else x)

            # Clean 'Przecena' by extracting the numeric part after 'From'
            df['Przecena'] = df['Przecena'].apply(lambda x: x.split('From')[1] if isinstance(x, str) and 'From' in x else x)

            # Convert 'IloscOpini' to integer by removing parentheses and commas
            df['IloscOpini'] = df['IloscOpini'].apply(
                lambda x: int(re.sub(r'[^\d]', '', x)) if isinstance(x, str) and re.search(r'\d', x) else x
            )

            # Add or clean 'VPN_City' column if necessary
            if 'VPN_City' not in df.columns:
                df['VPN_City'] = ''

            self.logger.logger_info.info("Data cleaning and transformation completed.")
            return df

        except Exception as e:
            self.logger.logger_err.error(f"Error during data cleaning: {e}")
            raise

class ScraperGYG_FuturePrice(ScraperGYG):
    pass
