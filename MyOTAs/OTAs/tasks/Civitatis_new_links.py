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
    "expand_categories_and_others": 'div[class="o-collapsible is-expanded"]',
    "expand_categories_and_others_header": 'div[class="o-collapsible__header"]',
    "categories_show_more": 'span[class="m-checklist__view-more"]',
    "categories_box_element": 'ul[class*="m-checklist"]',
    "categories": 'li[class="_enabled"]',
}


site = "Civitatis"
file_manager_logger = FilePathManager(site, "NA")
logger = LoggerManager(file_manager_logger)
DEBUG = DetermineDebugRun(check_for_debug=False)
# %%


# %%
config_path = os.path.join(project_root, file_manager_logger.config_path)
# Load the config from the JSON file
with open(config_path, 'r') as config_file:
    config = json.load(config_file)  # Load full JSON (Headout, Musement, Civitatis, etc.)

# Ensure "Civitatis" exists in the config and is a dictionary
if "Civitatis" not in config:
    config["Civitatis"] = {"settings": {"city": [], "url": "https://www.civitatis.com/en/city/"}}

# Get the Civitatis settings
civitatis_config = config["Civitatis"]

# Ensure "settings" exists in Civitatis
if "settings" not in civitatis_config:
    civitatis_config["settings"] = {"city": [], "url": "https://www.civitatis.com/en/city/"}

# Access cities from the config
cities = civitatis_config["settings"].get("city", [])

################################################
MANUAL = False
if MANUAL:
    cities = ["Sintra"]
################################################

for city in cities:
    url = civitatis_config["settings"].get("url").replace("city", city)

    if DEBUG.debug:
        file_manager = FilePathManager(site, city, True, '1111-11-11')
    else:
        file_manager = FilePathManager(site, city)

    scraper = ScraperCivitatis(url, city, css_selectors, file_manager, logger, new_links=True)

    scraper.get_url()
    scraper.handle_cookies()
    categories_section = scraper.define_categiores_section()
    scraper.load_hidden_categories(categories_section)
    categories_list = scraper.extract_categories(categories_section)

    # Extract category data into a dictionary {category_id_text: category_name}
    extracted_categories = {}
    for category in categories_list:
        category_id_text, category_name = scraper.extract_category_data(category)
        extracted_categories[category_id_text] = category_name

    # Ensure city exists in config and is stored as a dictionary
    if city not in civitatis_config["settings"] or not isinstance(civitatis_config["settings"][city], dict):
        civitatis_config["settings"][city] = {}  # Convert city to dictionary if needed

    # Append new categories (Avoid overwriting existing ones)
    civitatis_config["settings"][city].update(extracted_categories)

    if DEBUG.debug:
        break

# **Save the updated full config (without overwriting other OTAs)**
with open(config_path, 'w') as config_file:
    json.dump(config, config_file, indent=4)




