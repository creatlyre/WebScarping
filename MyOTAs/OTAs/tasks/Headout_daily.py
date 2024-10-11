import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_headout import ScraperHeadout
from file_management.file_path_manager import FilePathManager, DetermineDebugRun
from logger.logger_manager import LoggerManager
from uploaders.azure_blob_uploader import AzureBlobUploader
from backup_vm.stop_vm import StopVM
import json
# %%


# %%
css_selectors = {
    'currency': 'button[class="elementText"]',
    'currency_list': 'div[class*="symbol-bold"]',
    'products_count': 'span[class*="product-count-text"]',
    'show_more_button': 'a[data-qa-marker*="loading-button"]',
    'product_card': 'div[id*="product-card-container"]',
    'tour_price': 'span[class*="tour-price"]',
    'tour_price_discount': 'div[class="tour-scratch-price"]',
    'ratings': 'span[class*="rating-count"]',
    'review_count': 'span[class*="review-count"]',
    'category_label': 'span[class*="booster-label"]'
}
js_selectors = {
    'js_script_for_shadow_root': 'return document.querySelector("msm-cookie-banner").shadowRoot',

}

site = "Headout"
file_manager_logger = FilePathManager(site, "NA")
logger = LoggerManager(file_manager_logger)
DEBUG = DetermineDebugRun()
# %%
# Load the config from the JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
config = config.get(site)

# Access the city from the config
cities = config.get('settings').get('city')

for city in cities:
    url = config.get('settings').get('url').replace("city", city)

    if DEBUG.debug:
        file_manager = FilePathManager(site, city, True, '1111-11-11')
    else:
        file_manager = FilePathManager(site, city)
    scraper = ScraperHeadout(url, city, css_selectors, file_manager, logger)
    
    
    if scraper.is_city_already_done():
        logger.logger_info.info(f"Data for {city} already exists. Skipping...")
        continue
    elif scraper.is_today_already_done():
        logger.logger_info.info(f"Data for today already exists. Exitng...")
        break

    scraper.get_url()
    scraper.select_currency()
    products_count = scraper.get_product_count()
    scraper.load_all_products(products_count)
    df = scraper.scrape_products(global_category=True)
    scraper.save_to_csv(df)
    if DEBUG.debug:
        break
    
scraper.combine_csv_to_xlsx()

# %%
# Initialize the AzureBlobUploader with storage account details
blob_uploader = AzureBlobUploader(file_manager, logger)
blob_uploader.upload_excel_to_azure_storage_account()
blob_uploader.transform_upload_to_refined()

# %%



# %%



