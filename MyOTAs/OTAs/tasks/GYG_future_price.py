import os
import sys
import json
import datetime

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.future_price.scraper_gyg_future_price import ScraperGYGFuturePrice
from file_management.file_path_manager_future_price import FilePathManagerFuturePrice
from logger.logger_manager_future_price import LoggerManagerFuturePrice

def main():
    site = "GYG"
    city = "NA"  # Not applicable

    # Load the link file path from FilePathManager
    temp_file_manager = FilePathManagerFuturePrice(site, city, adults='2', language='en')
    link_file_path = temp_file_manager.link_file_path

    with open(link_file_path) as f:
        config = json.load(f)

    combinations = set()
    try:
        # Initialize the scraper and driver
        scraper = ScraperGYGFuturePrice("", "", {}, temp_file_manager, None)
        scraper.driver = scraper.initialize_driver()

        for site_info in config['urls']:
            url = site_info['url']
            viewer = site_info["viewer"]
            for config_item in site_info['configurations']:
                adults = config_item['adults']
                language = config_item['language']
                schedules = config_item['schedules']

                schedule, max_days = scraper.get_highest_order_schedule(schedules)
                if schedule.lower() == "no schedule for today":
                    print(f"URL: {url} is not scheduled for today to run")
                    continue

                # Update file manager and logger with current adults and language
                file_manager = FilePathManagerFuturePrice(site, city, adults, language)
                logger = LoggerManagerFuturePrice(file_manager)
                scraper.file_manager = file_manager
                scraper.logger = logger
                scraper.adults = adults
                scraper.language = language

                today_file_in_archive = scraper.check_if_today_done_on_schedule(url=url, schedule=schedule)
                if today_file_in_archive:
                    logger.logger_done.info(f"File in archive for URL: {url}, Adults: {adults}, Language: {language} ")
                else:
                    logger.logger_done.info(f"Running script for URL: {url}, Adults: {adults}, Language: {language}, Max Days: {max_days}")
                    scraper.get_future_price(url=url, viewer=viewer, max_days_to_complete=int(max_days))
                combinations.add((adults, language))
    except Exception as e:
        print(f"An error occurred: {e}")


    for adults, language in combinations:
        # Update file manager and logger for each combination
        file_manager = FilePathManagerFuturePrice(site, city, adults, language)
        logger = LoggerManagerFuturePrice(file_manager)
        scraper.file_manager = file_manager
        scraper.logger = logger
        scraper.adults = adults
        scraper.language = language

        scraper.process_csv_files()
        scraper.upload_excel_to_azure_storage_account()
        scraper.transform_upload_to_refined()

    print("All tasks completed successfully.")

if __name__ == "__main__":
    main()
