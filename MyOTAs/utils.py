import os
import logging
import logging.handlers
import time
import shutil
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from azure.storage.blob import BlobServiceClient
import json

# --- Constants ---

# File paths
date_today = time.strftime("%Y-%m-%d")
output_viator_base = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Viator'
output_viator_daily = os.path.join(output_viator_base, 'Daily')
output_viator_all_links = os.path.join(output_viator_base, 'All Links')
archive_folder_daily = os.path.join(output_viator_daily, 'Archive')
archive_folder_all_links = os.path.join(output_viator_all_links, 'Archive')

# --- API Keys ---
API_KEY_SCRAPERAPI = '8c36bc42cd11c738c1baad3e2000b40c'
API_KEY_ZENROWS = '56ed5b7f827aa5c258b3f6d3f57d36999aa949e8'
API_KEY_FIXER = 'acfed48df1159d37fa4305e5e95c234f' # botoslaw1

# --- Azure Configuration ---
STORAGE_ACCOUNT_NAME = "storagemyotas"
STORAGE_ACCOUNT_KEY = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="
CONTAINER_NAME_RAW_DAILY = "raw/daily/viator"
CONTAINER_NAME_REFINED_DAILY = "refined/daily/viator"
CONTAINER_NAME_RAW_ALL_LINKS = "raw/all_links/viator"
CONTAINER_NAME_REFINED_ALL_LINKS = "refined/all_links/viator"

# --- Mappings and Lists ---
MAPPING_CURRENCY = {
    'COP\xa0': 'COP (Colombian Peso)', 'HK$': 'HKD (Hong Kong Dollar)', '¥': 'JPY (Japanese Yen)',
    'DKK': 'DKK (Danish Krone)', 'R$': 'BRL (Brazilian Real)', '₹': 'INR (Indian Rupee)',
    'MX$': 'MXN (Mexican Peso)', 'ZAR\xa0': 'ZAR (South African Rand)', 'PEN\xa0': 'PEN (Peruvian Sol)',
    'NZ$': 'NZD (New Zealand Dollar)', '€': 'EUR (Euro)', 'CA$': 'CAD (Canadian Dollar)',
    'Â£': 'GBP (British Pound Sterling)', 'PEN': 'PEN (Peruvian Sol)', 'SEK\xa0': 'SEK (Swedish Krona)',
    'NOK': 'NOK (Norwegian Krone)', '$': 'USD (United States Dollar)', 'COP': 'COP (Colombian Peso)',
    'NT$': 'TWD (New Taiwan Dollar)', '£': 'GBP (British Pound Sterling)', 'â‚¬': 'EUR (Euro)',
    'Â¥': 'JPY (Japanese Yen)', 'â‚¹': 'INR (Indian Rupee)', 'SEK': 'SEK (Swedish Krona)',
    'ZAR': 'ZAR (South African Rand)', 'CHF': 'CHF (Swiss Franc)', 'â‚´': 'UAH (Ukrainian Hryvnia)',
    'zÅ‚': 'PLN (Polish Zloty)', 'Ð»Ð²': 'BGN Bulgarian Lev', 'US$': 'USD (United States Dollar)',
    'lei': 'RON (Romanian Leu)', 'zł': 'PLN (Polish Zloty)','$U': 'UYU (Uruguayan Peso)',
    'COL$': 'COP (Colombian Peso)', '₴': 'UAH (Ukrainian Hryvnia)', 'CL$': 'CLP (Chilean Peso)',
    'Rp': 'IDR (Indonesian Rupiah)', 'AR$': 'ARS (Argentine Peso)', '฿': 'THB (Thai Baht)',
    'Kč': 'CZK (Czech Koruna)', '₺': 'TRY (Turkish Lira)', 'A$': 'AUD (Australian Dollar)',
    'Ft': 'HUF (Hungarian Forint)', 'лв': 'BGN (Bulgarian Lev)', 'C$': 'NIO (Nicaraguan Cordoba)',
    '₪': 'ILS (Israeli Shekel)', '€.': 'EUR (Euro)', 'E£': 'EGP (Egyptian Pound)'
}

EUR_CITY = [
    'Madrid', 'Florence', 'Capri', 'Naples', 'Taormina', 'Mount-Etna', 'Bali', 'Porto', 'Krakow',
    'Barcelona', 'Athens', 'Palermo', 'Paris', 'Dubrovnik', 'Berlin', 'Istanbul', 'Adelaide', 'Venice',
    'Amsterdam', 'Cairns-and-the-Tropical-North', 'Sorrento', 'Dublin', 'Rome', 'Perth', 'Gold-Coast',
    'Amalfi-Coast', 'Salta', 'Bariloche', 'Milan', 'Hobart', 'Mount-Vesuvius', 'Reykjavik', 'Pompeii',
    'Vienna', 'Herculaneum', 'Lisbon', 'Brisbane', 'Marrakech', 'Mt-Vesuvius', 'Buenos-Aires', 'Cartagena',
    'Mendoza', 'Prague', 'Rio-de-Janeiro', 'Heraklion', 'Sintra'
]

USD_CITY = [
    'Oahu', 'New-York-City', 'Miami', 'Cancun', 'Vancouver', 'Cappadocia', 'Las-Vegas',
    'Niagara-Falls-and-Around', 'Toronto', 'Dubai', 'Montreal', 'San-Francisco', 'Maui',
    'Punta-Cana', 'Quebec-City', 'Queenstown', 'Singapore', 'Tokyo'
]

GBP_CITY = [
    'Belfast', 'Killarney', 'Galway', 'Lanzarote', 'Edinburgh', 'Manchester', 'England', 'London'
]

# --- Logging ---

def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup as many loggers as you want"""
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# --- ZenRows Scraper ---

class ZenRowsScraper:
    BASE_URL = 'https://api.zenrows.com/v1/'

    def __init__(self, api_key, retries=3, backoff_factor=1):
        self.api_key = api_key
        self.session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def get(self, url, params=None):
        """Makes a GET request to the ZenRows API."""
        if params is None:
            params = {}

        all_params = {'apikey': self.api_key, 'url': url}
        all_params.update(params)

        try:
            response = self.session.get(self.BASE_URL, params=all_params, timeout=120)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Request to {url} failed: {e}")
            return None

# --- Azure Blob Storage ---

def upload_to_blob(connection_string, container_name, blob_name, data):
    """Uploads data to a specific blob in Azure Storage."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Successfully uploaded {blob_name} to {container_name}.")
    except Exception as e:
        logging.error(f"Failed to upload to Azure Blob Storage: {e}")

def upload_file_to_blob(connection_string, container_name, blob_name, file_path):
    """Uploads a local file to a specific blob in Azure Storage."""
    with open(file_path, "rb") as data:
        upload_to_blob(connection_string, container_name, blob_name, data)

# --- Other Utilities ---

def get_exchange_rates(api_key, date, base_currency='EUR'):
    """Gets exchange rates from Fixer.io."""
    url = f"http://data.fixer.io/api/{date}?access_key={api_key}&base={base_currency}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get('rates')
    else:
        logging.error(f"Failed to get exchange rates: {response.text}")
        return None

def combine_csv_to_xlsx(csv_dir, xlsx_path, date_prefix):
    """Combines multiple CSV files from a directory into a single XLSX file."""
    csv_files = [f for f in os.listdir(csv_dir) if f.startswith(date_prefix) and f.endswith('.csv')]
    if not csv_files:
        logging.warning(f"No CSV files found with prefix '{date_prefix}' in '{csv_dir}'")
        return

    with pd.ExcelWriter(xlsx_path, engine='xlsxwriter') as writer:
        for csv_file in csv_files:
            try:
                sheet_name = os.path.splitext(csv_file)[0].replace(f"{date_prefix}-", "").replace("-Viator", "")
                df = pd.read_csv(os.path.join(csv_dir, csv_file))
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            except Exception as e:
                logging.error(f"Failed to process {csv_file}: {e}")
    logging.info(f"Successfully combined {len(csv_files)} CSV files into {xlsx_path}")