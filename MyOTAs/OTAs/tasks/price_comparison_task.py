# tasks/price_comparison_task.py

import os
import sys
import pandas as pd


# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

# Import necessary modules
from analytics.azure_blob_price_comparator import AzureBlobPriceComparator
from file_management.file_path_manager import FilePathManager
from logger.logger_manager import LoggerManager
from notifications.email_sender_alerts import EmailSenderAlerts

def read_alerts_data_from_csv(csv_file_path):
    """
    Reads URLs from an Excel file.
    """
    try:
        df = pd.read_csv(csv_file_path)
        
        return df
    except Exception as e:
        print(f"Error reading URLs from Excel file: {e}")
        return []

def main():
    for site in ['GYG', 'Viator', 'Musement', 'Headout']:
        # Initialize file manager and logger
        file_manager = FilePathManager(site, "NA")
        logger = LoggerManager(file_manager, f'{site}_price_alerts')
        comparator = AzureBlobPriceComparator(file_manager, logger)

        csv_file_path = file_manager.alerts_csv_file_path

        # Choose one method based on your preference
        df = read_alerts_data_from_csv(csv_file_path)
        df = df[df['OTA'] == site]
        if not df.any:
            logger.logger_done.info(f"No URLs found to process for {site}.")
            return

        for _, row in df.iterrows():
            url = row['URL']
            email = row['Email']
            
            logger.logger_info.info(f"Comparing prices for URL: {url}")
            status, result = comparator.compare_prices(url, site)
            if status == 'success':
                message = result['message']
                product = result['product']
                product_url = result['product_url']
                price_yesterday = result['price_yesterday']
                price_today = result['price_today']
                date_today = result['date_today']
                # Proceed with further processing, e.g., sending an email
                logger.logger_info.info(f"Price comparison successful: {message}")
                EmailSenderAlerts(email, product, product_url, date_today, price_yesterday, price_today, logger)
            elif status == 'no_data':
                logger.logger_info.info(result)
            elif status == 'no_change':
                logger.logger_info.info(result)
            elif status == 'error':
                logger.logger_err.error(result)
            else:
                logger.logger_err.error(f"Unexpected status: {status}")

        logger.close_logger()

if __name__ == "__main__":
    # site = 'GYG'
    # file_manager = FilePathManager(site, "NA")
    # logger = LoggerManager(file_manager, f'{site}_price_alerts')
    # EmailSenderAlerts('wojbal3@gmail.com', 'TEST PRODUCT', 'URL', '2024-11-29', '159', '179', logger)
    main()
