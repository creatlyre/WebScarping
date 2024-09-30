# %%
# refactored_file.py

import json
from commmon_functions_gyg import GYG_Scraper
import logging
import traceback
import sys
import os
import pandas as pd

# Get the current working directory instead of using __file__
current_dir = os.getcwd()

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(current_dir, '..')))

# Now you can import the modules
import common_functions
import Azure_stopVM



# %%


# %%
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
    'js_script_for_shadow_root': ' document.querySelector("msm-cookie-banner").shadowRoot',
    'cookies_banner': 'button[data-test*="decline-cookies"]',
    'sort_by': 'select[data-test-id="search-component-sort-selector"]',
    'option_rating': 'option[value*="rating"]',
    'option_popularity': 'option[value*="relevance-city"]'
}

# %%
def main():
    """
    Main function to execute the GYG scraping workflow.
    This function initializes the necessary managers, loads the links from the link file,
    and orchestrates the scraping and uploading processes.
    """
    try:
        # Initialize site and file manager
        site = "GYG"
        file_manager = common_functions.FilePathManager(site, "NA")  # 'NA' can be a default city or placeholder
        logger = common_functions.LoggerManager(file_manager)
        
        logger.logger_info.info(f"Starting scraping process for site: {site}")

        # Load all links and categories from the link file
        link_file_path = file_manager.get_file_paths()['link_file']
        if not os.path.exists(link_file_path):
            logger.logger_err.error(f"Link file '{link_file_path}' does not exist. Exiting.")
            return
        
        df_links = pd.read_csv(link_file_path)
        logger.logger_info.info(f"Loaded {len(df_links)} links from '{link_file_path}'.")

        # Initialize the scraper with the file manager and logger
        scraper = GYG_Scraper(file_manager, logger)
        
        try:
            # Execute the daily scraping run with the loaded links
            result = scraper.daily_run_gyg(df_links=df_links)
            if result == 'Done':
                logger.logger_info.info("Scraping already completed for today. No action needed.")
            
        except Exception as e:
            scraper.handle_error_and_rerun(e)
            logger.logger_err.error("An error occurred during the scraping process.")
            return  # Exit after handling the error
        
        # After scraping all links, proceed to upload the consolidated Excel file to Azure
        try:
            scraper.upload_excel_to_azure_storage_account()
            logger.logger_info.info("Uploaded the consolidated Excel file to Azure Blob Storage (raw container).")
        except Exception as e:
            scraper.handle_error_and_rerun(e)
            logger.logger_err.error("Failed to upload the Excel file to Azure Blob Storage (raw container).")
        
        # Transform the Excel file and upload the refined version to Azure
        try:
            scraper.transform_upload_to_refined()
            logger.logger_info.info("Transformed and uploaded the refined Excel file to Azure Blob Storage (refined container).")
        except Exception as e:
            scraper.handle_error_and_rerun(e)
            logger.logger_err.error("Failed to transform and upload the refined Excel file to Azure Blob Storage.")
        
        logger.logger_done.info("All scraping and uploading tasks completed successfully.")
    
    except Exception as e:
        # Catch any unforeseen errors in the main workflow
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"An unexpected error occurred in the main workflow: {e}")
        logging.error(traceback.format_exc())
    if 'backup' in os.getcwd():
        script_name = 'Viator_daily.py'

        check_if_viator_running = Azure_stopVM.check_if_script_is_running(script_name)
        if check_if_viator_running:
            logger.logger_done.info(f"{script_name} is currently running.")
        else:
            logger.logger_done.info(f"{script_name} is not running. Stoping VM")
            Azure_stopVM.stop_vm()

if __name__ == "__main__":
    main()
    



# %%


# %%

# ##################DEBUG CURRENCY SWITCHER




# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
# driver.maximize_window()
# # Define the URL of the website we want to scrape
# start_time = time.time()
# total_pages = 0
# #     CHECK IF FILE PATH EXISIT IF SO CHECK THE DATA INSIDE
# #         print(index, row)
# page = 1
# max_pages = 9999
# data = []
# position = 0
# url_time = time.time()

# url = f'https://www.getyourguide.com/s?q=Amsterdam&p=1'

# driver.get(url)
# time.sleep(1)
# #     VERIFY IF THE CURRENCY IS CORRECT
# login_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
# login_button.click()
# # currency = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Select Currency']")))
# currency = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
# currency
# html = driver.page_source
# soup = BeautifulSoup(html, 'html.parser')

# %%
# currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
# # hover over the currency switcher button to show the menu
# actions = ActionChains(driver)
# actions.move_to_element(currency_switcher_button).perform()
# currency_switcher_button .click()
# # wait for the EUR currency option to be clickable
# eur_currency_option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//li[@class='currency-modal-picker__item-parent item__currency-modal item__currency-modal--EUR']")))
# # click on the EUR currency option to change the currency
# eur_currency_option.click()

# html = driver.page_source
# soup = BeautifulSoup(html, 'html.parser')

# tour_items = soup.select("[data-test-id=vertical-activity-card]")
# len(tour_items)
# title = tour_items[0].find('p', {'class': 'vertical-activity-card__title'}).text.strip()
# price = tour_items[0].find('div', {'class': 'baseline-pricing__value'}).text.strip()


