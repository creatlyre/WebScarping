# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.webdriver import WebDriver
import time
import pandas as pd
import datetime
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
        if self.url != 'N/A':
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
    
   
    def all_links_excelfile(self, excel_file_path, file_path):
        # Extract the date from the Excel file name
        date_from_filename = os.path.basename(excel_file_path).split(' - ')[1].split(".")[0]

        # Read all sheets from the Excel file into a single DataFrame
        df_day = pd.concat(pd.read_excel(excel_file_path, sheet_name=None), ignore_index=True)

        # Rename columns in df_day to match df_oper
        df_day.rename(columns={
                'Tytul': 'Tytul',
                'Tytul URL': 'Link',
                'Miasto': 'City',
                'IloscOpini': 'Reviews',
                'Data zestawienia': 'Date input'
            }, inplace=True)
        df_day['Date update'] = df_day['Date input']

        df_day['Link'] = df_day['Link'].str.lower()

        # Drop the columns from df_day that are not in df_oper
        df_day = df_day[['Tytul', 'Link', 'City', 'Reviews', 'Date input', 'Date update']]

        # Remove duplicates based on the 'Link' column
        df_day = df_day.drop_duplicates(subset=['Link'])

        

        # Read the CSV file into a DataFrame
        # df_oper = pd.read_csv(file_path.replace('.csv', '.xlsx'))
        df_oper = pd.read_excel(file_path)
        df_oper['Link'] = df_oper['Link'].str.lower()
        # Update the 'Reviews' in df_oper from df_day
        df_oper_updated = pd.merge(df_oper, df_day[['Link', 'Reviews']], on='Link', how='left')
        df_oper_updated['Reviews'] = df_oper_updated['Reviews_y'].combine_first(df_oper_updated['Reviews_x'])
        df_oper_updated.drop(columns=['Reviews_x', 'Reviews_y'], inplace=True)

        # Update 'Date update' for matched links
        df_oper_updated.loc[df_oper_updated['Reviews'].notnull(), 'Date update'] = datetime.datetime.strptime(date_from_filename, '%Y-%m-%d')

        # Merge df_oper on top of df_day
        merged_df = pd.concat([df_oper_updated, df_day], ignore_index=True)

        # Drop duplicates while keeping all rows from df_oper
        merged_df = merged_df.drop_duplicates(subset='Link', keep='first')
        merged_df = merged_df[~merged_df['Link'].isnull()]
        merged_df['Link'] = merged_df['Link'].astype(str)
        # Fill empty 'Operator' column entries with 'ToDo'
        merged_df['Operator'] = merged_df['Operator'].fillna('ToDo')
        # Clean GYG file
        if 'GYG' in file_path:
            merged_df['Reviews'] = merged_df['Reviews'].apply(lambda x: str(x).lower().replace('(', '').replace(')', '').replace('reviews', '') if len(str(x)) > 0 else '0')
            merged_df['uid'] = merged_df['Link'].apply(lambda x: str(x).split('-')[-1].replace('/', ''))
        elif "Headout" in file_path:
            merged_df['Reviews'] = merged_df['Reviews'].fillna(0)
            merged_df['Reviews'] = merged_df['Reviews'].str.replace('(', '').str.replace(')','')
            merged_df['Reviews'] = merged_df['Reviews'].apply(lambda x: int(float(x.replace('K', '')) * 1000) if isinstance(x, str) and 'K' in x else x)
            merged_df['uid'] = merged_df['Link'].apply(lambda x: str(x).split('-')[-1].replace('/', ''))
        elif "Musement" in file_path:
            merged_df['Reviews'] = merged_df['Reviews'].apply(lambda x: str(x).lower().replace('(', '').replace(')', '').replace('reviews', '') if len(str(x)) > 0 else '0')
            merged_df['uid'] = merged_df['Link'].apply(lambda x: str(x).split('-')[-1].replace('/', ''))
        else:
            merged_df['uid'] = merged_df['Link'].apply(lambda x: str(x).split('/')[-1])

        
        # Save the resulting DataFrame to a new file
        output_file_path = os.path.join(file_path)
        # merged_df.to_excel(output_file_path, index=False)
    # Use XlsxWriter as the engine to write the Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            workbook.strings_to_urls = False
            merged_df.to_excel(writer, index=False, sheet_name='AllLinks')

        with open(output_file_path, 'wb') as f:
            f.write(output.getvalue())
            
        self.logger.logger_info.info(f"Processed data saved to {output_file_path}")
