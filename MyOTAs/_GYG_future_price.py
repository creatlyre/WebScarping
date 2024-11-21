import os
import sys
import time
import datetime
import logging
import traceback
import shutil
import glob
import json
import calendar
import re

import pandas as pd
import numpy as np
from openpyxl import Workbook, load_workbook

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException,
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

from azure.storage.blob import BlobServiceClient

# Import ConfigReader and FilePathManagerFuturePrice classes
from OTAs.file_management.config_manager_future_price import ConfigReader
from OTAs.file_management.file_path_manager_future_price import FilePathManagerFuturePrice
from OTAs.logger.logger_manager_future_price import LoggerManagerFuturePrice

# Constants - Replace with your actual paths and credentials
SITE = 'GYG'
OUTPUT_GYG = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/GYG/future_price'
LOGS_PATH = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/GYG/future_price'
FILE_PATH_LOGS_PROCESSED = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed'

STORAGE_ACCOUNT_NAME = 'storagemyotas'
STORAGE_ACCOUNT_KEY = 'vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=='
CONTAINER_NAME_RAW = 'raw/future_price/gyg'
CONTAINER_NAME_REFINED = 'refined/future_price/gyg'


class Config:
    """
    Configuration class to store settings for the scraper.
    """
    def __init__(self, adults, language):
        self.adults = adults
        self.language = language
        # Define date variables
        self.date_today = datetime.date.today().strftime("%Y-%m-%d")
        current_hour = datetime.datetime.now().strftime('%H:00:00')
        self.extraction_date = f"{self.date_today} {current_hour}"
        self.extraction_date_save_format = f"{self.extraction_date.replace(' ', '_').replace(':', '-')}_{self.language}_{self.adults}"
        # Define file paths
        self.output_gyg = OUTPUT_GYG
        self.logs_path = LOGS_PATH
        self.local_file_path = f"{self.output_gyg}/{self.extraction_date}_future_price.xlsx"
        self.blob_name = f"{self.extraction_date_save_format}_future_price.xlsx"
        self.file_path_logs_processed = os.path.join(FILE_PATH_LOGS_PROCESSED, self.blob_name.split(".")[0])
        self.output_file_path = os.path.join(self.output_gyg, f"{self.extraction_date_save_format}_future_price.xlsx")
        self.archive_folder = os.path.join(self.output_gyg, 'Archive')


def initilize_driver():
    """
    Initialize the Selenium WebDriver.
    """
    try:
        logger.logger_info.info("Initializing the Chrome driver.")
        # Setting up Chrome options
        options = webdriver.ChromeOptions()
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        # Initialize the Chrome driver
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        logger.logger_done.info("Chrome driver initialized successfully.")
        return driver

    except Exception as e:
        logger.logger_err.error(f"An error occurred during driver initialization: {e}")
        raise


def quit_driver(driver):
    """
    Quit the Selenium WebDriver.
    """
    try:
        driver.quit()
        logger.logger_done.info("Chrome driver quit successfully.")
    except Exception as e:
        logger.logger_err.error(f"An error occurred while quitting the driver: {e}")


def save_and_erase_dataframe(df, url_city_id, url_unique_identifier, config):
    """
    Save the DataFrame to a CSV file and reset it.
    """
    try:
        file_path = os.path.join(config.output_gyg, f'{config.extraction_date_save_format}-{url_city_id}-GYG.csv')
        df['city'] = url_city_id.split('-')[0]
        df['uid'] = url_unique_identifier
        df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
        logger.logger_done.info(f'Successfully saved {len(df)} rows to {file_path}')
        return pd.DataFrame()
    except Exception as e:
        logger.logger_err.error(f"An error occurred while saving DataFrame: {e}")
        raise


def extract_options(driver, option_details, activity_title, language, adults_amount, url, viewer, extraction_date):
    """
    Extract options from the webpage.
    """
    list_of_items = []
    try:
        time.sleep(1)
        option_date = driver.current_url.split('date_from=')[-1].split('&')[0]

        for option in option_details:
            option_title = option.find_element(By.CLASS_NAME, 'activity-option__title').text
            option_time_range = option.find_element(By.CLASS_NAME, 'activity-option-date starting-times__date starting-times__date--mb-lg').text if option.find_elements(By.CLASS_NAME, 'activity-option-date starting-times__date starting-times__date--mb-lg') else 'Not listed'
            try:
                try:
                    option_price_total = option.find_element(By.CLASS_NAME, 'activity-option-price-wrapper__price').text
                    option_price_per_person = float(option_price_total.replace('€', '').replace(',', '').strip()) / float(adults_amount)
                except:
                    try:
                        option_price_total = option.find_element(By.CLASS_NAME, 'activity-option__cart-message-text--future-availability').text
                    except:
                        option_price_total = option.find_element(By.CLASS_NAME, 'activity-option-cart-message-wrapper').text
                    option_price_per_person = 'Not Available'
            except Exception as e:
                logger.logger_err.info(f"Error parsing price: {e}")
                option_price_total = 'Not Available'
                option_price_per_person = 'Not Available'

            spots_left = option.find_element(By.CSS_SELECTOR, "span[data-test-id*='activity-option-is-x-spots-left']").text if option.find_elements(By.CSS_SELECTOR, "span[data-test-id*='activity-option-is-x-spots-left']") else 'N/A'

            list_of_items.append({
                'extraction_date': extraction_date,
                'date': option_date,
                'title': activity_title,
                'tour_option': option_title,
                'time_range': option_time_range,
                'total_price': option_price_total,
                'price_per_person': option_price_per_person,
                'language': language,
                'adults': adults_amount,
                'spots_left': spots_left,
                'title_url': url,
                'viewer': viewer
            })
            logger.logger_info.info(f'Extracted option: {option_title}')
        return list_of_items
    except Exception as e:
        logger.logger_err.error(f"An error occurred in extract_options for URL {url} on date {option_date}: {e}")
        raise


def process_days_not_available(activity_title, language, adults_amount, option_date, url, viewer, extraction_date):
    """
    Process days that are not available and create a list item.
    """
    try:
        list_of_items = [{
            'extraction_date': extraction_date,
            'date': option_date,
            'title': activity_title,
            'tour_option': "N/A",
            'time_range': "N/A",
            'total_price': "N/A",
            'price_per_person': "N/A",
            'language': language,
            'adults': adults_amount,
            'spots_left': "N/A",
            'title_url': url,
            'viewer': viewer
        }]
        return list_of_items
    except Exception as e:
        logger.logger_err.error(f"An error occurred in process_days_not_available for URL {url} on date {option_date}: {e}")
        raise


def get_future_price(driver, url, viewer, language, adults_amount, max_days_to_complete, config):
    """
    Main function to get future prices from the website.
    """
    try:
        logger.logger_info.info(f"Starting price extraction for URL: {url}")
        start_time_one_link = time.time()

        url_id = url
        url_unique_identifier = url.split('.com/')[-1].split('-')[ -1].replace('/', '')
        url_city_id = url.split('.com/')[-1].split('/')[0]

        start_collection_date = config.date_today
        date_today_obj = datetime.datetime.strptime(start_collection_date, "%Y-%m-%d")
        url_details = f'?lang={language}&date_from={start_collection_date}&_pc=1,{adults_amount}'
        full_url = url + url_details

        # Calculate picked_max_date_obj
        picked_max_date_obj = date_today_obj + datetime.timedelta(days=max_days_to_complete)
        picked_max_date = picked_max_date_obj.strftime('%Y-%m-%d')

        # Calculate months to complete
        current_date = datetime.datetime.now()
        year_diff = picked_max_date_obj.year - current_date.year
        month_diff = picked_max_date_obj.month - current_date.month
        if year_diff > 0:
            month_to_complete = month_diff + (12 * year_diff) + 1
        else:
            month_to_complete = month_diff + 1
        current_year = current_date.year

        driver.get(full_url)
        logger.logger_info.info(f'Navigated to URL: {full_url} UNIQUE ID: {url_unique_identifier}')
        logger.logger_info.info(f'Months to complete: {month_to_complete} Picked Max Date {picked_max_date}')

        is_done, max_date_done = check_if_current_day_done_or_partly_done(url_city_id, url_unique_identifier, config)
        if is_done:
            if max_date_done >= picked_max_date_obj.date():
                logger.logger_done.info(f'URL was already processed today up to date: {max_date_done}')
                return
            else:
                logger.logger_info.info(f'Continuing from last processed date: {max_date_done}')
                full_url = full_url.replace(start_collection_date, max_date_done.strftime('%Y-%m-%d'))
                driver.get(full_url)

        change_currency(driver, url)
        check_and_click_only_essential(driver, url)

        activity_title = get_activity_title(driver, url)
        if not activity_title:
            logger.logger_err.error("Activity title not found. Skipping URL.")
            return

        booking_tile = WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-track='booking-assistant']")))
        calendar_picker = booking_tile.find_element(By.CSS_SELECTOR, "section[class='ba-dropdown ba-date-picker']")
        check_availability_button = WebDriverWait(booking_tile, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[class*='js-check-availability']")))

        time.sleep(2)
        driver.execute_script("arguments[0].click();", check_availability_button)

        # Handle potential date unavailability
        try:
            WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CLASS_NAME, 'dayContainer')))
            months = driver.find_elements(By.CLASS_NAME, 'dayContainer')
            empty = True
            for month in months:
                days_available = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day-available]")
                if len(days_available) == 0:
                    continue
                for day in days_available:
                    if day.text == "" or len(day.text) == 0:
                        logger.logger_info.info("Day was empty")
                        continue
                    ActionChains(driver).move_to_element(day).perform()
                    day.click()
                    driver.execute_script("arguments[0].click();", check_availability_button)
                    empty = False
                    break
                if not empty:
                    break
        except TimeoutException:
            logger.logger_info.info('Current date is available.')
            pass

        # After clicking, wait for options to show up
        WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.TAG_NAME, 'details')))
        option_details = driver.find_elements(By.TAG_NAME, 'details')
        list_of_items = extract_options(driver, option_details, activity_title, language, adults_amount, url_id, viewer, config.extraction_date)
        df = pd.DataFrame(list_of_items)
        save_date_to_file = driver.current_url.split('date_from=')[-1].split('&')[0]
        logger.logger_done.info(f'Saving date: {save_date_to_file} to file')
        df = save_and_erase_dataframe(df, url_city_id, url_unique_identifier, config)

        # Click on calendar picker to select dates
        try:
            calendar_picker.click()
        except Exception as e:
            check_for_modal_window_to_close(driver, calendar_picker)

        WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.CLASS_NAME, 'dayContainer')))

        # Iterate through months and days
        current_year = current_date.year
        for i in range(month_to_complete):
            months = driver.find_elements(By.CLASS_NAME, 'dayContainer')
            month_index = 0
            month = months[month_index]
            current_month = driver.find_elements(By.CLASS_NAME, "flatpickr-current-month")[month_index].text.strip()

            if current_month == "January":
                current_year += 1

            days_to_complete = []
            days_available = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day-available]")
            for day in days_available:
                if len(day.text) > 0:
                    day_date_str = f"{current_month} {day.text.strip()}, {current_year}"
                    day_date_obj = datetime.datetime.strptime(day_date_str, '%B %d, %Y')
                    if date_today_obj <= day_date_obj <= picked_max_date_obj:
                        if is_done and day_date_obj.date() >= max_date_done:
                            days_to_complete.append(day_date_str)
                        elif not is_done:
                            days_to_complete.append(day_date_str)

            # Process days not available
            days_not_available_elements = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day]")
            days_not_available = get_days_not_available(date_today_obj, picked_max_date_obj, current_year, current_month, days_not_available_elements)

            for day_na in days_not_available:
                list_of_items = process_days_not_available(activity_title, language, adults_amount, day_na, url_id, viewer, config.extraction_date)
                df = pd.DataFrame(list_of_items)
                logger.logger_done.info(f'Saving date: {day_na} to file: not available date')
                df = save_and_erase_dataframe(df, url_city_id, url_unique_identifier, config)

            # Iterate through days to complete
            for day in days_to_complete:
                try:
                    day_element = month.find_element(By.CSS_SELECTOR, f"span[aria-label*='{day}']")
                    day_element.click()
                except StaleElementReferenceException:
                    months = driver.find_elements(By.CLASS_NAME, 'dayContainer')
                    month = months[month_index]
                    day_element = month.find_element(By.CSS_SELECTOR, f"span[aria-label*='{day}']")
                    day_element.click()
                except Exception as e:
                    logger.logger_err.error(f"An error occurred while selecting day {day} for url {url}: {e}")
                    continue

                WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.TAG_NAME, 'details')))
                option_details = driver.find_elements(By.TAG_NAME, 'details')
                time.sleep(1
                )
                list_of_items = extract_options(driver, option_details, activity_title, language, adults_amount, url_id, viewer, config.extraction_date)
                df = pd.DataFrame(list_of_items)
                save_date_to_file = driver.current_url.split('date_from=')[-1].split('&')[0]
                logger.logger_done.info(f'Saving date: {save_date_to_file} to file')
                df = save_and_erase_dataframe(df, url_city_id, url_unique_identifier, config)
                check_for_modal_window_to_close(driver, calendar_picker)

            # Navigate to next month
            next_month_button = driver.find_element(By.CSS_SELECTOR, "span[class='flatpickr-next-month']")
            next_month_button.click()
            time.sleep(0.5)

        end_time_one_link = time.time()
        logger.logger_statistics.info(f"Time required for {max_days_to_complete} days: {(end_time_one_link - start_time_one_link):.2f} seconds")

    except Exception as e:
        logger.logger_err.error(f"An error occurred in get_future_price for URL {url}: {e}")
        logger.logger_err.error(traceback.format_exc())

def get_days_not_available(date_today_obj, picked_max_date_obj, current_year, current_month, days_not_available_elements):
    days_not_available = []
    for day_na in days_not_available_elements:
        if day_na.text.strip():
            day_na_date_str = f"{current_month} {day_na.text.strip()}, {current_year}"
            day_na_date_obj = datetime.datetime.strptime(day_na_date_str, '%B %d, %Y')
            if day_na_date_obj >= date_today_obj and day_na_date_obj <= picked_max_date_obj:
                day_na_str = day_na_date_obj.strftime('%Y-%m-%d')
                days_not_available.append(day_na_str)
    return days_not_available


def check_for_modal_window_to_close(driver, calendar_picker):
    """
    Check for modal window and close it if present.
    """
    try:
        modal_close_button = driver.find_element(By.CSS_SELECTOR, "button[class='tfe-modal-header__close']")
        modal_close_button.click()
        time.sleep(2)
        driver.execute_script("arguments[0].scrollIntoView(true);", calendar_picker)
        calendar_picker.click()
    except NoSuchElementException:
        driver.execute_script("arguments[0].scrollIntoView(true);", calendar_picker)
        calendar_picker.click()
    except Exception as e:
        logger.logger_err.error(f"An error occurred in check_for_modal_window_to_close: {e}")


def change_currency(driver, url):
    """
    Change currency to Euro if not already set.
    """
    try:
        # Click on the profile icon to open the dropdown
        profile_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']"))
        )
        profile_button.click()

        currency_selector = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'select[id="footer-currency-selector"]'))
        )
        select = Select(currency_selector)
        selected_option = select.first_selected_option
        current_currency = selected_option.text.strip()
        logger.logger_info.info(f"Current currency: {current_currency}")

        if "EUR" not in current_currency:
            desired_currency_text = 'Euro (€)'
            select.select_by_visible_text(desired_currency_text)
            logger.logger_info.info(f"Currency changed to {desired_currency_text}")
            time.sleep(2)
        else:
            logger.logger_info.info("Currency is already set to Euro.")
    except Exception as e:
        logger.logger_err.error(f"Failed to change currency for URL {url}: {e}")


def check_and_click_only_essential(driver, url):
    """
    Handle the cookie consent popup by clicking 'Only Essential'.
    """
    try:
        # Wait for the cookie consent popup to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".ot-sdk-container"))
        )
        # Attempt to find and click the 'Only Essential' button
        only_essential_button = driver.find_element(By.ID, "onetrust-reject-all-handler")
        if only_essential_button:
            only_essential_button.click()
            logger.logger_info.info("Clicked 'Only Essential' on cookie consent.")
    except TimeoutException:
        # Log if the popup is not found within the wait period
        logger.logger_info.info("Cookie consent popup not found.")
    except NoSuchElementException:
        # Log as information if the button is not found
        logger.logger_info.info("The 'Only Essential' button was not found.")
    except Exception as e:
            logger.logger_err.error(f"An unexpected error occurred while handling cookie consent for URL {url}: {e}")



def get_activity_title(driver, url):
    """
    Get the activity title from the page.
    """
    try:
        activity_title = driver.find_element(By.CSS_SELECTOR, "h1[data-track='activity-title']").text
        logger.logger_info.info(f"Activity title: {activity_title}")
        return activity_title
    except NoSuchElementException:
        logger.logger_err.error(f"Activity title element not found for URL {url}.")
        return None
    except Exception as e:
        logger.logger_err.error(f"An error occurred while getting activity title for URL {url}: {e}")
        return None


def check_if_current_day_done_or_partly_done(url_city_id, url_unique_identifier, config):
    """
    Check if the data for the current day has already been processed.
    """
    try:
        date_part = config.extraction_date_save_format.split('_')[0]
        fixed_part = config.extraction_date_save_format.split('_', 2)[2]
        file_pattern = os.path.join(config.output_gyg, f'{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv')
        file_pattern_archive = os.path.join(config.archive_folder, f'{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv')

        matching_files = glob.glob(file_pattern) + glob.glob(file_pattern_archive)
        logger.logger_info.info(f'Found {len(matching_files)} matching files.')

        if not matching_files:
            return False, None

        combined_df = pd.concat([pd.read_csv(file) for file in matching_files], ignore_index=True)
        filtered_df = combined_df[(combined_df['uid'] == url_unique_identifier) & (~combined_df['total_price'].isna())]

        if filtered_df.empty:
            return False, None

        if 'date' in filtered_df.columns:
            filtered_df['date'] = pd.to_datetime(filtered_df['date'])
            max_date = filtered_df['date'].max()
            if pd.notnull(max_date):
                return True, max_date.date()
        return False, None
    except Exception as e:
        logger.logger_err.error(f"An error occurred while checking processed data: {e}")
        return False, None


def process_csv_files(folder_path, adults, language, config):
    """
    Combine individual CSV files into a single Excel file.
    """
    try:
        output_file_path = config.output_file_path
        if os.path.exists(output_file_path):
            logger.logger_info.info("Output file already exists. Skipping processing.")
            return

        archive_path = config.archive_folder

        combined_df = pd.DataFrame()

        for filename in os.listdir(folder_path):
            if filename.startswith(config.date_today) and filename.endswith('.csv') and f"{language}_{str(adults)}" in filename:
                file_path = os.path.join(folder_path, filename)
                df = pd.read_csv(file_path)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                shutil.move(file_path, os.path.join(archive_path, filename))
                logger.logger_info.info(f"Processed and archived file: {filename}")

        combined_df.to_excel(output_file_path, index=False)
        logger.logger_done.info(f"Combined data saved to {output_file_path}.")
    except Exception as e:
        logger.logger_err.error(f"An error occurred while processing CSV files: {e}")
        raise


def upload_excel_to_azure_storage_account(local_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name):
    """
    Upload the Excel file to Azure Blob Storage (raw container).
    """
    try:
        logger.logger_info.info("Uploading file to Azure Blob Storage (raw).")
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name_raw)

        with open(local_file_path, "rb") as file:
            container_client.upload_blob(name=blob_name, data=file, overwrite=True)
        logger.logger_done.info("File uploaded successfully to Azure Blob Storage (raw).")
    except Exception as e:
        logger.logger_err.error(f"An error occurred while uploading to Azure Blob Storage: {e}")
        raise


def transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name):
    """
    Transform the data and upload to Azure Blob Storage (refined container).
    """
    try:
        logger.logger_info.info("Transforming data and uploading to Azure Blob Storage (refined).")
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
        df = pd.read_excel(local_file_path)

        # Data transformations
        df['extraction_date'] = df['extraction_date'].astype('str')
        df['date'] = df['date'].astype('str')
        df['availability'] = df['total_price'].apply(extract_date_from_price)
        df['message'] = df['total_price'].apply(dynamic_message_option)
        df['total_price'] = df['total_price'].apply(set_to_long_price_to_nan)
        df['price_per_person'] = df['price_per_person'].replace('Not Available', np.nan)
        df['total_price'] = df['total_price'].str.replace(r'[$€£]', '', regex=True).str.replace(',', '').str.strip()
        df['city'] = df['city'].str.title()

        output_file_path = "temp_modified_excel.xlsx"
        df.to_excel(output_file_path, index=False)

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name_refined)
        with open(output_file_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        logger.logger_done.info("File uploaded successfully to Azure Blob Storage (refined).")
    except Exception as e:
        logger.logger_err.error(f"An error occurred: {e}")
        raise
    finally:
        if os.path.exists(output_file_path):
            os.remove(output_file_path)


def extract_date_from_price(text):
    """
    Extract date from the 'total_price' field if it contains a 'Next available date' message.
    """
    if 'Next available date:' in str(text):
        date_part = text.split('Next available date: ')[1].strip()
        try:
            date_obj = datetime.datetime.strptime(date_part, "%A, %B %d, %Y")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError as e:
            logger.logger_err.error(f"Date format mismatch: {e}")
            return np.nan
    else:
        return np.nan


def dynamic_message_option(option):
    """
    Generate a dynamic message based on the 'total_price' field.
    """
    messages = {
        'Please select 1 participants or fewer for this activity.': 'Set adults to 1',
        'Maximum 2 adults allowed per booking': 'Adults set to 2'
    }
    for pattern, message in messages.items():
        if pattern in str(option):
            return message
    return np.nan


def set_to_long_price_to_nan(text):
    """
    Set overly long or empty 'total_price' fields to NaN.
    """
    text = str(text)
    if len(text) > 15 or len(text) == 0 or text.lower() == 'nan':
        return np.nan
    else:
        return text


def get_highest_order_schedule(schedules):
    """
    Determine the highest order schedule that should run today.
    """
    try:
        today = datetime.datetime.today()
        day = today.day
        month_length = calendar.monthrange(today.year, today.month)[1]

        sorted_schedules = sorted(schedules.items(), key=lambda x: int(x[0]), reverse=False)
        for freq, value in sorted_schedules:
            frequency = int(freq)
            if should_run_today(day, month_length, frequency):
                return freq, value
        return "No schedule for today", None
    except Exception as e:
        logger.logger_err.error(f"An error occurred while determining schedule: {e}")
        return "No schedule for today", None


def should_run_today(day, month_length, frequency):
    """
    Determine if the script should run today based on the schedule.
    """
    if frequency == 1:
        return day == 2
    else:
        interval = month_length // frequency
        for i in range(frequency):
            if day == 1 + i * interval:
                return True
    return False


def check_if_today_done_on_schedule_in_csv(url, config):
    """
    Check if today's data for the given URL has already been processed.
    """
    try:
        url_unique_identifier = url.split('.com/')[-1].split('-')[-1].replace('/', '')
        url_city_id = url.split('.com/')[-1].split('/')[0]

        date_part = config.extraction_date_save_format.split('_')[0]
        fixed_part = config.extraction_date_save_format.split('_', 2)[2]
        file_pattern = os.path.join(config.archive_folder, f'{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv')

        matching_files = glob.glob(file_pattern)
        logger.logger_info.info(f'Checking if today\'s data is already processed for URL: {url}')

        for file_path in matching_files:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df = df[df['uid'] == url_unique_identifier]
                if not df.empty:
                    logger.logger_info.info("Today's data is already processed.")
                    return True
        return False
    except Exception as e:
        logger.logger_err.error(f"An error occurred while checking today's data: {e}")
        return False


def main():
    """
    Main execution function.
    """
    # sample_config = Config("N/A", "N/A")
    # define_logging(sample_config.logs_path)
    global logger
    try:
        # Initialize file path manager and config reader
        file_manager = FilePathManagerFuturePrice(SITE, "N/A", "N/A", "N/A")
        config_reader = ConfigReader(file_manager.config_file_path)
        urls = config_reader.get_urls_by_ota(SITE)
        logger = LoggerManagerFuturePrice(file_manager, 'gyg_future_price')

        driver = initilize_driver()
        combinations = set()
        for item in urls:
            url = item['url']
            viewer = item['viewer']
            for cfg in item['configurations']:
                adults = cfg['adults']
                language = cfg['language']
                schedules = cfg['schedules']

                frequency, max_days = config_reader.get_highest_order_schedule(schedules)
                if frequency == "No schedule for today":
                    logger.logger_done.info(f"URL: {url} is not scheduled for today.")
                    continue

                config = Config(adults, language)
                today_file_in_archive = check_if_today_done_on_schedule_in_csv(url, config)
                if today_file_in_archive:
                    logger.logger_done.info(f"Data already processed for URL: {url}, Adults: {adults}, Language: {language}")
                else:
                    logger.logger_done.info(f"Processing URL: {url}, Adults: {adults}, Language: {language}, Frequency: {frequency}, Max Days: {max_days}")
                    get_future_price(driver, url, viewer, language, adults, max_days, config)
                    combinations.add((adults, language))

        quit_driver(driver)
        for adults, language in combinations:
            config = Config(adults, language)
            process_csv_files(config.output_gyg, adults, language, config)
            upload_excel_to_azure_storage_account(config.output_file_path, STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, CONTAINER_NAME_RAW, config.blob_name)
            transform_upload_to_refined(config.output_file_path, STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, CONTAINER_NAME_REFINED, config.blob_name)
        logger.logger_done.info("Script execution completed successfully.")

    except Exception as e:
        logger.logger_err.error(f"An error occurred in main execution for URL {url if 'url' in locals() else 'N/A'}: {e}")
        logger.logger_err.error(traceback.format_exc())


if __name__ == "__main__":
    main()