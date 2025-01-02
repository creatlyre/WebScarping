import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_musement import ScraperMusement
from file_management.file_path_manager import FilePathManager, DetermineDebugRun
from logger.logger_manager import LoggerManager
import pandas as pd

# %%


# %%
site = "Musement"
css_selectors = {
    'currency': 'div[data-test*="dropdown-currency"]',
    'currency_list': 'section[class*="row-start-center"]',
    'products_count': 'span[data-test-id*="search-component-activity-count-text"]',
    'view_more_button': 'button[data-test-id="search-component-test-btn"]',
    'show_more_button': 'a[data-qa-marker*="loading-button"]',
    'product_card': 'div[data-test*="ActivityCard"]',
    'tour_price': 'span[data-test="realPrice"]',
    'tour_price_discount': 'div[class="tour-scratch-price"]',
    'ratings': 'div[data-test="reviewTest"]',
    'review_count': 'p[class*="reviewsNumber"]',
    'category_label': 'div[data-test="main-category"]',
    'js_script_for_shadow_root': 'return document.querySelector("msm-cookie-banner").shadowRoot',
    'cookies_banner': 'button[data-test*="decline-cookies"]',
    'sort_by': 'select[data-test-id="search-component-sort-selector"]',
    'option_rating': 'option[value*="rating"]',
    'option_popularity': 'option[value*="relevance-city"]',
    'provider': 'div[class*="src-shared_component-blockBody"]'
}


# %%
file_manager = FilePathManager(site, "NA")
logger = LoggerManager(file_manager)
file_path_xlsx_operator = file_manager.get_file_paths()['file_path_xlsx_operator']


# %%

df = pd.read_excel(file_path_xlsx_operator)
counter = 1
for index, row in df.iterrows():
    
    url = row['Link']
    # Log the current row being processed
    logger.logger_info.info(f"Processing row {index} with URL: {url}")

    if row['Operator'] != "ToDo":
        logger.logger_info.info(f"Skipping row {index} as the URL is not 'ToDo'.")
        continue
    try:
        scraper = ScraperMusement(url, None, css_selectors,  file_manager, logger, provider=True)
        # Log the initiation of the scraping process
        logger.logger_info.info(f"Initialized scraper for URL: {url}")
        scraper.get_url()
        
        provider_name = scraper.get_provider_name()
        if provider_name == 'Not Found':
            logger.logger_done.info(f"Provider not found for row {index}: url: {url}")
            continue
        # Log that the provider name was successfully fetched
        logger.logger_done.info(f"Provider name fetched for row {index}: {provider_name.text}")
            
        df.at[index, 'Operator'] = provider_name.text

    except Exception as e:
        # Log any errors encountered during the scraping process
        logger.logger_err.error(f"Error processing row {index} with URL {url}: {str(e)}")
        df.at[index, 'Operator'] = "NotFound"
    finally:
        # Ensure that the driver is closed
        scraper.quit_driver()
        counter += 1
        if counter % 50 == 0:
            logger.logger_done.info(f"Already process {counter} saving progress.")
            scraper.save_dataframe(df, file_path_xlsx_operator)
        logger.logger_done.info(f"Closed scraper for URL: {url}")

scraper.save_dataframe(df, file_path_xlsx_operator)

