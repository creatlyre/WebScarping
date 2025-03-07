# %%
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_civitatis import ScraperCivitatis
from file_management.file_path_manager import FilePathManager, DetermineDebugRun
from logger.logger_manager import LoggerManager
from uploaders.azure_blob_uploader import AzureBlobUploader
from backup_vm.stop_vm import StopVM
import json


# %%
css_selectors = {
    'currency': 'span[id="currencySelectorButton"]',
    'currency_list': 'span[data-testid="page-nav__currency_EUR"]',
    'products_count': 'div[class*="search-result"]',
    'view_more_button': 'button[data-test-id="search-component-test-btn"]',
    'show_more_button': 'a[data-qa-marker*="loading-button"]',
    'product_card': 'div[id*="activitiesItem"]',
    'tour_price': 'span[class="comfort-card__price__text"]',
    'tour_price_discount': 'div[class="comfort-card__price__old-text"]',
    'ratings': 'span[class="m-rating--text"]',
    'review_count': 'span[class="text--rating-total"]',
    'category_label': 'span[class*="_feature-category"]',
    'js_script_for_shadow_root': 'return document.querySelector("msm-cookie-banner").shadowRoot',
    'cookies_banner': 'button[data-test*="decline-cookies"]',
    'sort_by': 'select[data-test-id="search-component-sort-selector"]',
    'option_rating': 'option[value*="rating"]',
    'option_popularity': 'option[value*="relevance-city"]'
}


site = "Civitatis"
file_manager_logger = FilePathManager(site, "NA")
logger = LoggerManager(file_manager_logger)
DEBUG = DetermineDebugRun(check_for_debug=False)
# %%


# %%
# Load the config from the JSON file
with open(os.path.join(project_root, file_manager_logger.config_path), 'r') as config_file:
    config = json.load(config_file)
config = config.get(site)

# Access the city from the config
cities = config.get('settings').get('city')

for city in cities:
    base_url = config.get('settings').get('url').replace("city", city)

    if DEBUG.debug:
        file_manager = FilePathManager(site, city, True, '1111-11-11')
    else:
        file_manager = FilePathManager(site, city)

    scraper = ScraperCivitatis(base_url, city, css_selectors, file_manager, logger)

    # Check if data for the city is already collected
    if scraper.is_city_already_done():
        logger.logger_info.info(f"Data for {city} already exists. Skipping...")
        continue
    elif scraper.is_today_already_done():
        logger.logger_info.info(f"Data for today already exists. Exiting...")
        break

    # Global Scraping (Without Category Filter)
    scraper.get_url()
    scraper.select_currency()
    products_count = scraper.get_product_count()

    df_global = scraper.scrape_products(products_count=products_count, global_category=True)
    df_global['category'] = "Global"  # Label as 'Global' for reference
    scraper.save_to_csv(df_global)

    # Scrape for Each Category in the City
    if city in config['settings']:
        category_data = config['settings'][city]  # Get category mapping

        for category_id, category_name in category_data.items():
            category_url = f"{base_url}?allowedCategoryIds={category_id}"  # Modify URL for category filtering
            logger.logger_info.info(f"Scraping {category_name} ({category_id}) for {city}...")

            scraper = ScraperCivitatis(category_url, city, css_selectors, file_manager, logger)

            scraper.get_url()
            scraper.select_currency()
            products_count = scraper.get_product_count()

            df_category = scraper.scrape_products(products_count=products_count, global_category=False)
            df_category['category'] = category_name  # Assign category name for reference

            scraper.save_to_csv(df_category)

    if DEBUG.debug:
        break  # Exit after first iteration for testing
    
scraper.combine_csv_to_xlsx()

# %%
# Initialize the AzureBlobUploader with storage account details
blob_uploader = AzureBlobUploader(file_manager, logger)
blob_uploader.upload_excel_to_azure_storage_account()
blob_uploader.transform_upload_to_refined()


