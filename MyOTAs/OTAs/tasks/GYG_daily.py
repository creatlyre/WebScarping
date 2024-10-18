# %%
import logging
import traceback
import pandas as pd
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_gyg import ScraperGYG
from file_management.file_path_manager import FilePathManager, DetermineDebugRun
from logger.logger_manager import LoggerManager
from uploaders.azure_blob_uploader import AzureBlobUploader
from backup_vm.stop_vm import StopVM


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
    DEBUG = DetermineDebugRun()
    activity_per_page = 16

    
    try:
        # Initialize site and file manager
        site = "GYG"
        if DEBUG.debug:
            file_manager = FilePathManager(site, "NA", True, "2000-10-10")  # 'NA' can be a default city or placeholder
        else:
            file_manager = FilePathManager(site, "NA")  # 'NA' can be a default city or placeholder
            
        logger = LoggerManager(file_manager)
        
        logger.logger_info.info(f"Starting scraping process for site: {site}")

        # Load all links and categories from the link file
        link_file_path = file_manager.get_file_paths()['link_file']
        if not os.path.exists(link_file_path):
            logger.logger_err.error(f"Link file '{link_file_path}' does not exist. Exiting.")
            return
        
        df_links = pd.read_csv(link_file_path)
        logger.logger_info.info(f"Loaded {len(df_links)} links from '{link_file_path}'.")
        if DEBUG.debug:
            df_links = df_links[df_links['Run'] == 1].iloc[0:10]
            activity_per_page = 400
        
        # Initialize the scraper with the file manager and logger
        scraper = ScraperGYG("Daily", "Daily", css_selectors, file_manager, logger, activity_per_page)
        
        # Execute the daily scraping run with the loaded links
        while True:
            try:
                result = scraper.daily_run_gyg(df_links=df_links)
            except Exception as e:
                scraper.handle_error_and_rerun(e)
                logger.logger_err.error("An error occurred during the scraping process.")
        
            if result == "Done":
                break
        
        
        blob_uploader = AzureBlobUploader(file_manager, logger)
        # After scraping all links, proceed to upload the consolidated Excel file to Azure
        try:
            blob_uploader.upload_excel_to_azure_storage_account()
            logger.logger_info.info("Uploaded the consolidated Excel file to Azure Blob Storage (raw container).")
        except Exception as e:
            scraper.handle_error_and_rerun(e)
            logger.logger_err.error("Failed to upload the Excel file to Azure Blob Storage (raw container).")
        
        # Transform the Excel file and upload the refined version to Azure

        # Initialize the AzureBlobUploader with storage account details

        try:
            blob_uploader.transform_upload_to_refined()
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

        check_if_viator_running = StopVM.check_if_script_is_running(script_name)
        if check_if_viator_running:
            logger.logger_done.info(f"{script_name} is currently running.")
        else:
            logger.logger_done.info(f"{script_name} is not running. Stoping VM")
            StopVM.stop_vm()

if __name__ == "__main__":
    main()
    