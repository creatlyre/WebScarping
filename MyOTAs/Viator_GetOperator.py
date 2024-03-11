# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import numpy as np
import datetime
from selenium.webdriver.common.action_chains import ActionChains
import xlsxwriter
from openpyxl import Workbook, load_workbook
import os
import shutil
import logging
import traceback
import re
from azure.storage.blob import BlobServiceClient
# from undetected_chromedriver import Chrome, ChromeOptions
# from user_agent import generate_user_agent
# import ctypes  # An included library with Python install.   
import random
import requests
import json
import concurrent.futures

# eyJhbGciOiJSUzI1NiIsImtpZCI6IjY3YmFiYWFiYTEwNWFkZDZiM2ZiYjlmZjNmZjVmZTNkY2E0Y2VkYTEiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiV29qdGVrIEJhbG9uIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FBY0hUdGZCODM1WVhSalRJeEl4WmxyTnBaRXpWQk9hZmUyMUFmU1dZZXNnUGc9czk2LWMiLCJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vZXhhMi1mYjE3MCIsImF1ZCI6ImV4YTItZmIxNzAiLCJhdXRoX3RpbWUiOjE2ODY2NTg5MDYsInVzZXJfaWQiOiJEcWRXRDhRdloyUTkzcTR4WFhWWlFWUk8wSEMyIiwic3ViIjoiRHFkV0Q4UXZaMlE5M3E0eFhYVlpRVlJPMEhDMiIsImlhdCI6MTY4NjY1OTA2MSwiZXhwIjoxNjg2NjYyNjYxLCJlbWFpbCI6IndvamJhbDNAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZ29vZ2xlLmNvbSI6WyIxMTUwNTc1NjgzNzI4NjQ1MzA0NTciXSwiZW1haWwiOlsid29qYmFsM0BnbWFpbC5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJnb29nbGUuY29tIn19.IAOh_U2LXNXGk1jqG3q6m9utI79QVMDtCuUcDBSH5TEKPmMCEdW962qOZN6J8wfMzexHX1cWoqGcXYBmjLcjQKBhhQoAUAdYjxEivrLHe8Hi37bIwXrEX9mvAKD1wE71Sq1sbB3B9xU51lTsH88l7P0pq9LDgbaKkJCljvvzJ186BTbX9Qw0CF4gma1XjJ1W3Nmd0BK2pE9y0b3arF_V8bSME6BeR4Ls1yKLM9da-MCN5y-IkwGVB6j78Qrt-4_emtAhxjkcYlzauOtEM8dZ0NzblgSxY-hdG_sG-Clg0gM6fxXRQSQJYjqHNgwY7sjAP885JUWbtjWjoXKvdJn_iA

# %%
date_today = datetime.date.today().strftime("%Y-%m-%d")
# date_today = '2023-10-19'
date_yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
output_viator = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Viator/All Links'
file_path_done =fr'{output_viator}/{date_today}-DONE-Viator.csv'  
archive_folder = fr'{output_viator}/Archive'

file_path_done_archive =fr'{archive_folder}/{date_yesterday}-DONE-Viator.csv'  
file_path_output = fr"{output_viator}/AllLinksViator - {date_today}.xlsx"
file_path_output_processed = fr"{output_viator}/All Links Viator - {date_today}.xlsx"
file_path_output_processed_csv = fr"{output_viator}/All Links Viator - {date_today}.csv"
file_path_csv_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_Groups.csv"
file_path_all_links_send_to_scraper = fr"{output_viator}\SupplierExtract - {date_today}.csv"
file_path_all_links_send_to_scraper_finished = fr"{output_viator}\SupplierExtractFinished - {date_today}.csv"
link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/Viator_links.csv'
all_links_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/AllViator_links.csv'
# Set the path of the local file
local_file_path = file_path_output
# local_file_path = f"{output_viator}/AllLinksViator - {date_today}.xlsx"

# Set the name of your Azure Storage account and the corresponding access key
storage_account_name = "storagemyotas"
storage_account_key = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="

# Set the name of the container and the desired blob name
container_name_raw = "raw/all_links/viator"
container_name_refined = "refined/all_links/viator"

blob_name = fr'Viator - {date_today}.xlsx'
# file_path_logs_processed = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed/{blob_name.split(".")[0]}'

mapping_currency = {'COP\xa0': 'COP (Colombian Peso)', 'HK$': 'HKD (Hong Kong Dollar)', 
                    '¥': 'JPY (Japanese Yen)', 'DKK': 'DKK (Danish Krone)', 'R$': 'BRL (Brazilian Real)',
                    '₹': 'INR (Indian Rupee)', 'MX$': 'MXN (Mexican Peso)', 'ZAR\xa0': 'ZAR (South African Rand)',
                    'PEN\xa0': 'PEN (Peruvian Sol)', 'NZ$': 'NZD (New Zealand Dollar)', '€': 'EUR (Euro)',
                    'CA$': 'CAD (Canadian Dollar)', 'Â£': 'GBP (British Pound Sterling)',
                    'PEN': 'PEN (Peruvian Sol)', 'SEK\xa0': 'SEK (Swedish Krona)', 'NOK': 'NOK (Norwegian Krone)',
                    '$': 'USD (United States Dollar)', 'COP': 'COP (Colombian Peso)', 
                    'NT$': 'TWD (New Taiwan Dollar)', '£': 'GBP (British Pound Sterling)',
                    'â‚¬': 'EUR (Euro)', 'Â¥': 'JPY (Japanese Yen)',
                    'â‚¹': 'INR (Indian Rupee)', 'SEK': 'SEK (Swedish Krona)', 'ZAR': 'ZAR (South African Rand)',
                    'CHF': 'CHF (Swiss Franc)', 'â‚´': 'UAH (Ukrainian Hryvnia)', 'zÅ‚': 'PLN (Polish Zloty)',
                    'Ð»Ð²': 'BGN Bulgarian Lev', 'US$': 'USD (United States Dollar)', 'lei': 'RON (Romanian Leu)',
                    'zł': 'PLN (Polish Zloty)','$U': 'UYU (Uruguayan Peso)', 'COL$': 'COP (Colombian Peso)', 
                    '₴': 'UAH (Ukrainian Hryvnia)',
                    'CHF': 'CHF (Swiss Franc)', 'zł': 'PLN (Polish Zloty)', 'R$': 'BRL (Brazilian Real)',
                    'CL$': 'CLP (Chilean Peso)', 'Rp': 'IDR (Indonesian Rupiah)', 'AR$': 'ARS (Argentine Peso)',
                    '฿': 'THB (Thai Baht)', 'Kč': 'CZK (Czech Koruna)', 'lei': 'RON (Romanian Leu)',
                    '₺': 'TRY (Turkish Lira)', 'A$': 'AUD (Australian Dollar)', 'Ft': 'HUF (Hungarian Forint)',
                    '€': 'EUR (Euro)', '£': 'GBP (British Pound Sterling)', '₹': 'INR (Indian Rupee)',
                    'US$': 'USD (United States Dollar)', 'лв': 'BGN (Bulgarian Lev)',
                    'COL$': 'COP (Colombian Peso)', 'lei': 'RON (Romanian Leu)', 'C$': 'NIO (Nicaraguan Cordoba)',
                    '₺': 'TRY (Turkish Lira)', 'AR$': 'ARS (Argentine Peso)', 'A$': 'AUD (Australian Dollar)',
                    'лв': 'BGN (Bulgarian Lev)', 'Ft': 'HUF (Hungarian Forint)', 'DKK': 'DKK (Danish Krone)',
                    '₪': 'ILS (Israeli Shekel)', '€.': 'EUR (Euro)', '₴': 'UAH (Ukrainian Hryvnia)',
                    'R$': 'BRL (Brazilian Real)', '₹': 'INR (Indian Rupee)', 'zł': 'PLN (Polish Zloty)',
                    'US$': 'USD (United States Dollar)', '€': 'EUR (Euro)', '$U': 'UYU (Uruguayan Peso)',
                    'Kč': 'CZK (Czech Koruna)', 'SEK': 'SEK (Swedish Krona)', '£': 'GBP (British Pound Sterling)',
                    'E£': 'EGP (Egyptian Pound)', 'CL$': 'CLP (Chilean Peso)'}


currency_list = []
API_KEY = '8c36bc42cd11c738c1baad3e2000b40c'

# %%
EUR_City = [
    'Madrid',
    'Florence',
    'Capri',
    'Naples',
    'Taormina',
    'Mount-Etna',
    'Bali',
    'Porto',
    'Krakow',
    'Barcelona',
    'Athens',
    'Palermo',
    'Paris',
    'Dubrovnik',
    'Berlin',
    'Istanbul',
    'Adelaide',
    'Venice',
    'Amsterdam',
    'Cairns-and-the-Tropical-North',
    'Sorrento',
    'Dublin',
    'Rome',
    'Perth',
    'Gold-Coast',
    'Amalfi-Coast',
    'Salta',
    'Bariloche',
    'Milan',
    'Hobart',
    'Mount-Vesuvius',
    'Reykjavik',
    'Pompeii',
    'Vienna',
    'Herculaneum',
    'Lisbon',
    'Brisbane',
    'Marrakech',
    'Mt-Vesuvius',
    'Buenos-Aires',
    'Cartagena',
    'Mendoza',
    'Prague',
    'Rio-de-Janeiro'
]

USD_City = [
    'Oahu',
    'New-York-City',
    'Miami',
    'Cancun',
    'Vancouver',
    'Cappadocia',
    'Las-Vegas',
    'Niagara-Falls-and-Around',
    'Toronto',
    'Dubai',
    'Montreal',
    'San-Francisco',
    'Maui',
    'Punta-Cana',
    'Quebec-City',
    'Queenstown',
    'Singapore',
    'Tokyo'
]

GBP_City = [
    'Belfast',
    'Killarney',
    'Galway',
    'Lanzarote',
    'Edinburgh',
    'Manchester',
    'England',
    'London'
]

# %% [markdown]
# *Code below extract the supplier name from the html content*

# %%
# Setting up logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler('viator_getoperator.log'),
                               logging.StreamHandler()])



class Scraper:
#     with open("config.json", 'r', encoding='utf-8') as file:
#         config = json.load(file)
#     api_key = config['api_key']
#     file_path_csv_operator = config['file_path_csv_operator']
#     file_path_all_links_send_to_scraper = congi['file_path_all_links_send_to_scraper']
    
    
    def __init__(self, api_key, file_path_csv_operator, file_path_all_links_send_to_scraper):
        self.API_KEY = api_key
        self.file_path_csv_operator = file_path_csv_operator
        self.file_path_all_links_send_to_scraper = file_path_all_links_send_to_scraper
        self.recursive_calls = 0
        logging.info("Scraper initialized with API key and file paths.")

    def _load_dataframe(self, file_path):
        """Load data from CSV into a dataframe."""
        return pd.read_csv(file_path)

    def _save_dataframe(self, df, file_path, header=True, mode='w'):
        """Save dataframe to CSV."""
        df.to_csv(file_path, index=False, header=header , mode=mode)

    def send_url_to_process_supplier_name(self):
        """Send URLs to the processing service and update the CSV with the response."""
        # Load dataframe to process
        dataframe_to_process = self._load_dataframe(self.file_path_csv_operator)
        dataframe_to_process = dataframe_to_process[dataframe_to_process['Operator'] == 'ToDo']

        # Load the already processed URLs if file exists
        if os.path.exists(self.file_path_all_links_send_to_scraper):
            processed_data = pd.read_csv(self.file_path_all_links_send_to_scraper)
            processed_urls = processed_data['UrlRequest'].unique()
        else:
            processed_urls = []

        # print('To process URL wich will be send')
        # display(dataframe_to_process)
        country_codes = ["us","en"]


        # Filter out URLs that have already been processed
        dataframe_to_process = dataframe_to_process[~dataframe_to_process['Link'].isin(processed_urls)]

         # Initialize progress tracking variables
        total_urls = len(dataframe_to_process)
        processed_count = 0
        for _, row in dataframe_to_process.iterrows():
            processed_count += 1
            url = row['Link']
            random_country_code = random.choice(country_codes)
            url_request = requests.post(url = 'https://async.scraperapi.com/jobs', 
                                        json={'apiKey': self.API_KEY, 
                                              'country_code': random_country_code,
                                              'url': url })
            self._handle_url_request_response(url_request, url)

            # Log the processing status
            percent_done = (processed_count / total_urls) * 100
            logging.info(f"Processing {processed_count}/{total_urls} row. Done {percent_done:.2f}%")

    def _handle_url_request_response(self, response, url):
        """Handle the response from the URL request."""
        if response.status_code == 200:
            try:
                status_url = response.json()['statusUrl']
                data_send_df = pd.DataFrame({
                    'UrlRequest': [url],
                    'UrlResponse': [status_url],
                    'Status': 'running',
                    'Operator': 'ToDo'
                })
                self._save_dataframe(data_send_df, 
                                     self.file_path_all_links_send_to_scraper,
                                     header=not os.path.exists(self.file_path_all_links_send_to_scraper),
                                     mode='a')
                
#                 logging.info(f"Processed URL: {url} with status URL: {status_url}")
            except ValueError:
                logging.warning("JSON could not be decoded for URL: %s", url)
#                 print("JSON could not be decoded")    
        else:
            logging.error(f"HTTP request returned code: {response.status_code} for URL: {url}")
#             print(f"HTTP request returned code: {response.status_code}")
            
            

    def check_status_and_add_to_file_path(self):
        """Check the status of URLs and update the CSV."""
        all_links = self._load_dataframe(self.file_path_all_links_send_to_scraper)
#         print('all_links in check_status_and_add_to_file_path')
#         display(all_links)
        df_links = all_links[all_links['Status'] == 'running']
#         print('df_links in check_status_and_add_to_file_path')
#         display(df_links)
        
        previous_hash = None   # Store the hash of the dataframe for change detection
    
        while len(df_links[df_links['Status'] == 'running']) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self._get_status, url): url for url in df_links['UrlResponse']}
                for future in concurrent.futures.as_completed(futures):
                    url = futures[future]
                    status = future.result()
                    # Update the status of these rows in the original dataframe
                    all_links.loc[all_links['UrlResponse'] == url, 'Status'] = status
                    df_links.loc[df_links['UrlResponse'] == url, 'Status'] = status
                    # print(url, status, len(df_links[df_links['Status'] == 'running']))
                    # Remove the processed URL from the futures dictionary if its status is 'finished'
                    if status == 'finished':
                        del futures[future]
                        logging.info(f"Finished processing URL: {url}. Left to process: {len(df_links[df_links['Status'] == 'running'])}") 

                    else:
                        logging.debug(f"URL: {url} is still runningm Rows to process{len(df_links[df_links['Status'] == 'running'])}")
                    

            # Refresh the df_links dataframe to pick only 'running' URLs
            df_links = df_links[df_links['Status'] == 'running']
            # Check if the dataframe has changed
            current_hash = hash(df_links.to_string())
            if previous_hash != current_hash:
                self._save_dataframe(all_links, self.file_path_all_links_send_to_scraper)
                self.extract_supplier_name()
                logging.info("Detected changes in df_links and saved the updated dataframe.")
                previous_hash = current_hash
            else:
                logging.info("No changes in df_links. ")
            
#             if len(df_links[df_links['Status'] == 'running']) <= 3 and previous_hash == current_hash:
#                 if self.recursive_calls < 5:  # Check against threshold
#                     logging.info("Low number of 'running' URLs detected. Triggering further processing...")
#                     self.extract_supplier_name()
#                     self.send_url_to_process_supplier_name(150)
#                     self.recursive_calls += 1
#                     logging.info(f"Starting recursive call. Current count: {self.recursive_calls}")
#                     self.check_status_and_add_to_file_path(150)
#                 else:
#                     logging.warning("Maximum recursive call threshold reached. Not triggering further processing.")
                
#             print('df_links afterwards remvoed running')
#             display(df_links)
        # Save the entire dataframe back to the CSV, overwriting the original file
        self._save_dataframe(all_links, self.file_path_all_links_send_to_scraper)
        logging.info(f"Updated {len(df_links)} links in the dataframe and saved.")
        self.recursive_calls -= 1  # Decrement the counter after processing
        logging.info(f"Recursive calls count: {self.recursive_calls}")

        return f'Updated {len(df_links)} links'


    def _get_status(self, url):
        """Retrieve the status for a given URL."""
        try:
            response = requests.get(url)
            return response.json()['status']
        except Exception as e:
#             print(f"Error while fetching URL: {url}, Error: {e}")
            logging.error(f"Error while fetching URL: {url}, Error: {e}")
            return 'error'
        
    def extract_supplier_name(self):
        """Extract supplier name from the URLs and update the CSV."""
        all_links_df = self._load_dataframe(self.file_path_all_links_send_to_scraper)
        operator_csv = self._load_dataframe(self.file_path_csv_operator)
        df = all_links_df[(all_links_df['Status'] == 'finished') & (all_links_df['Operator'] == 'ToDo')]
        counter = 1
        counter = 1
        # Preparing session for HTTPS requests
        session = requests.Session()

        for _, row in df.iterrows():
            supplier_name = self._get_supplier_name_from_url(session, row['UrlResponse'])
            logging.info(f"Extracted supplier name: {supplier_name} for URL: {row['UrlResponse']}")
            all_links_df.loc[all_links_df['UrlResponse'] == row['UrlResponse'], 'Operator'] = supplier_name
            operator_csv.loc[operator_csv['Link'] == row['UrlRequest'], 'Operator'] = supplier_name
            counter +=1
            print(counter)
            if counter % 50 == 0:
                print(counter, counter % 50)
                logging.info('Saving files...')
                self._save_dataframe(all_links_df, self.file_path_all_links_send_to_scraper)
                self._save_dataframe(operator_csv, self.file_path_csv_operator)

        self._save_dataframe(all_links_df, self.file_path_all_links_send_to_scraper)
        self._save_dataframe(operator_csv, self.file_path_csv_operator)
    

    def _get_supplier_name_from_url(self, session, url):
        """Extract the supplier name from a given URL."""
        results = session.get(url)
        soup = BeautifulSoup(results.content, 'html.parser')
        split_supplier = str(soup).split('supplierName')
        for supplier in split_supplier:
            try:
                supplier_name_array = supplier.split('timeZone')
                if len(supplier_name_array[0]) <= 100:
                    return ''.join(filter(lambda x: x.isalpha() or x.isspace(), supplier_name_array[0]))
            except:
                logging.error('Time zone not found in the extracted supplier details from URL: %s', url)
        return None
    




# %%
# Create an instance of the Scraper class
def main():
    scraper = Scraper(api_key=API_KEY, 
                    file_path_csv_operator=file_path_csv_operator, 
                    file_path_all_links_send_to_scraper=file_path_all_links_send_to_scraper)

    # Read the operator CSV and get the count of 'ToDo' links
    operator_csv = pd.read_csv(scraper.file_path_csv_operator)
    print(f"There are {len(operator_csv[operator_csv['Operator'] == 'ToDo'])} links to do")

    # Continue processing as long as there are 'ToDo' links
    while len(operator_csv[operator_csv['Operator'] == 'ToDo']) > 0:
        print("send_url_to_process_supplier_name")
        scraper.send_url_to_process_supplier_name()
        print("check_status_and_add_to_file_path")
        scraper.check_status_and_add_to_file_path()
        print("extract_supplier_name")
        scraper.extract_supplier_name()
        operator_csv = pd.read_csv(scraper.file_path_csv_operator)
        print(f"There are {len(operator_csv[operator_csv['Operator'] == 'ToDo'])} links to do")


# %%



