API_KEY = "56ed5b7f827aa5c258b3f6d3f57d36999aa949e8"

import traceback
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
import datetime
from scrapers.scraper_tripadvisor import TripadvisorScraper
from file_management.file_path_manager import FilePathManager
from logger.logger_manager import LoggerManager
from uploaders.azure_blob_uploader import AzureBlobUploader




css_selectors = {
           'total_products': 'div.Ci',
           'products_list': 'section[data-automation*="SingleFlexCardSection"]',
           'product_link': 'a[href]',
           'product_title': 'h3.biGQs._P.fiohW.ngXxk',
           'product_reviews_amount': 'span.biGQs._P.pZUbB.osNWb',
           'product_reviews_rating': 'svg.UctUV',
           'product_price': 'div[data-automation="cardPrice"]',
           'product_discount': 'div[data-automation="cardStrikeThroughPrice"]',
           'product_text': 'span.SwTtt',
           'currency_language_button': 'button[aria-label*="Currency:"]',
           'category': 'a.Fmbdu.B-.G_.KoOWI',
           'supplier_section': 'div[class*="qyzqH f k w"]',
           'supplier': 'div[class*="biGQs _P"]',    
}
def main():
    try:
        site = 'Tripadvisor'
        date_today = datetime.datetime.now().strftime("%Y-%m-%d")
        file_manager = FilePathManager(site, "NA", manual_overdrive_date=False, manual_date='2025-01-15') 
        dummy_logger = LoggerManager(file_manager, application="tripadvisor_daily")

        scraper = TripadvisorScraper(API_KEY, file_manager, date_today, css_selectors)
        if scraper.load_existing_data() == "Done":
            scraper.logger.logger_info.info("No scraping needed as the run is already completed.")
        else:        
            collected_data = scraper.scrape()

            if collected_data:
                scraper.logger.logger_info.info("Scrape method returned True. Proceeding with data consolidation.")
                scraper.combine_csv_to_xlsx()
                scraper.logger.logger_done.info(f"Scraping statistics - Cost: {scraper.accumulated_cost}, Products Collected: {scraper.accumulated_products_collected}, Time Taken: {scraper.accumulated_time}")
            else:
                scraper.logger.logger_err.warning("Scrape method returned False. Check logs for details.")
    except Exception as e:
        # Initialize a basic logger if possible, or print to stderr
        try:
            # Attempt to use the same logger setup as in the class
            
            dummy_logger.logger_err.error(f"Unhandled exception in main: {e}")
            dummy_logger.logger_err.error(traceback.format_exc())
        except Exception:
            # Fallback to printing if logger fails
            print(f"Unhandled exception in main: {e}", file=sys.stderr)
            traceback.print_exc()
        sys.exit(1)  # Exit with error code


    blob_uploader = AzureBlobUploader(file_manager, scraper.logger)
    try:
        blob_uploader.upload_excel_to_azure_storage_account()
        scraper.logger.logger_info.info("Uploaded the consolidated Excel file to Azure Blob Storage (raw container).")
    except Exception as e:
        scraper.handle_error_and_rerun(e)
        scraper.logger.logger_err.error("Failed to upload the Excel file to Azure Blob Storage (raw container).")

    try:
        blob_uploader.transform_upload_to_refined()
        scraper.logger.logger_info.info("Transformed and uploaded the refined Excel file to Azure Blob Storage (refined container).")
    except Exception as e:
        scraper.handle_error_and_rerun(e)
        scraper.logger.logger_err.error("Failed to transform and upload the refined Excel file to Azure Blob Storage.")
    
    scraper.logger.logger_done.info("All scraping and uploading tasks completed successfully.")

if __name__ == "__main__":
    main()
    