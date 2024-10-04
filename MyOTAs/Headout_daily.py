# %%
import json
import common_functions

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
file_manager_logger = common_functions.FilePathManager(site, "NA")
logger = common_functions.LoggerManager(file_manager_logger)

# %%
# Load the config from the JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
config = config.get(site)

# Access the city from the config
cities = config.get('settings').get('city')

for city in cities:
    url = config.get('settings').get('url').replace("city", city)
    file_manager = common_functions.FilePathManager(site, city)
    scraper = common_functions.ScraperHeadout(url, city, css_selectors, file_manager, logger)
    
    
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
    
scraper.combine_csv_to_xlsx()

# %%


# %%
# Initialize the AzureBlobUploader with storage account details
blob_uploader = common_functions.AzureBlobUploader(file_manager, logger)
blob_uploader.upload_excel_to_azure_storage_account()
blob_uploader.transform_upload_to_refined()

# %%



# %%



