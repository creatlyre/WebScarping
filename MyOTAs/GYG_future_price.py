# %%
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import time
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import numpy as np
import datetime
from selenium.webdriver.common.action_chains import ActionChains
import xlsxwriter
from openpyxl import Workbook, load_workbook
import os
import shutil
import logging
import traceback
import re
import csv
from azure.storage.blob import BlobServiceClient
import argparse
import sys
import glob
from selenium.webdriver.support.ui import Select
from OTAs.file_management.config_manager_future_price import ConfigReader
from OTAs.file_management.file_path_manager_future_price import FilePathManagerFuturePrice


import calendar
import json

# %%
# File paths


# date_today = datetime.date.today().strftime("%Y-%m-%d")
# output_gyg = r'output/GYG'
# archive_folder = fr'{output_gyg}/Archive'
# file_path_done =fr'output/GYG/{date_today}-DONE-GYG.csv'  
# file_path_output = fr"output/GYG - {date_today}.xlsx"
# link_file = fr'resource/GYG_links.csv'
# avg_file = fr'resource/avg-gyg.csv'
# re_run_path = fr'output/GYG/{date_today}-ReRun-GYG.csv'
# folder_path_with_txt_to_count_avg = 'Avg/GYG'
def constant_file_path():
    global output_gyg, archive_folder, link_file, logs_path, storage_account_name, storage_account_key, container_name_raw, container_name_refined,  link_file_path

    link_file_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/LinksFuturePrice_GYG.json'
    output_gyg = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/GYG/future_price'
    archive_folder = fr'{output_gyg}/Archive'
    link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/LinksFuturePrice_GYG.csv'
    logs_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/GYG/future_price'

    # Set the name of your Azure Storage account and the corresponding access key
    storage_account_name = "storagemyotas"
    storage_account_key = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="

    # Set the name of the container and the desired blob name
    container_name_raw = "raw/future_price/gyg"
    container_name_refined = "refined/future_price/gyg"
    

def configure_dates_and_file_names(adutls, language):
    global date_today, extraction_date, extraction_date_save_format, local_file_path, blob_name,  file_path_logs_processed, output_file_path
    # Define the automatic date setting as today's date
    automatic_date_today = datetime.date.today().strftime("%Y-%m-%d")

    # Manually set date (uncomment to use manual date)
    # date_today = '2024-12-15'

    # Check if date_today exists; if not, fall back to automatic date
    try:
        date_today
    except NameError:
        date_today = automatic_date_today

    # Set extraction_date to use date_today with the current hour
    current_hour = datetime.datetime.now().strftime('%H:00:00')
    extraction_date = f"{date_today} {current_hour}"
    extraction_date_save_format = f"{extraction_date.replace(' ', '_').replace(':', '-')}_{language}_{adutls}"

    # Set the path of the local file

    local_file_path = f"{output_gyg}/{extraction_date}_future_price.xlsx"
    blob_name = fr'{extraction_date_save_format}_future_price.xlsx'
    file_path_logs_processed = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed/{blob_name.split(".")[0]}'
    output_file_path = os.path.join(output_gyg, f"{extraction_date_save_format}_future_price.xlsx")


# %%
EUR_City = [
    "Amsterdam", "Athens", "Barcelona", "Berlin", "Dublin", "Dubrovnik", "Florence", "Istanbul",
    "Krakow", "Lisbon", "Madrid", "Milan", "Naples", "Paris", "Porto", "Rome", "Palermo", "Venice",
    "Taormina", "Capri", "Sorrento", "Mount-Etna", "Mount-Vesuvius", "Herculaneum", "Amalfi-Coast",
    "Pompeii"
]

USD_City = [
    "Las-Vegas", "New-York-City", "Cancun", "Dubai"
]

GBP_City = [
    "Edinburgh", "London"
]

# %%
def define_logging():
    global logger_err, logger_info, logger_done, logger_statistics
    # create logger object
    logger_err = logging.getLogger('Error_logger')
    logger_err.setLevel(logging.DEBUG)
    logger_info = logging.getLogger('Info_logger')
    logger_info.setLevel(logging.DEBUG)
    logger_done = logging.getLogger('Done_logger')
    logger_done.setLevel(logging.DEBUG)
    logger_statistics = logging.getLogger('Satistics_logger')
    logger_statistics.setLevel(logging.DEBUG)


    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create file handler for error logs and set level to debug
    fh_error = logging.FileHandler(fr'{logs_path}/error_logs.log')
    fh_error.setLevel(logging.DEBUG)

    # create file handler for info logs and set level to info
    fh_info = logging.FileHandler(fr'{logs_path}/info_logs.log')
    fh_info.setLevel(logging.INFO)

    # create file handler for info logs and set level to info
    fh_done = logging.FileHandler(fr'{logs_path}/done_logs.log')
    fh_done.setLevel(logging.INFO)

    fh_statistics = logging.FileHandler(fr'{logs_path}/statistics_logs.log')
    fh_statistics.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to handlers
    ch.setFormatter(formatter)
    fh_error.setFormatter(formatter)
    fh_info.setFormatter(formatter)
    fh_done.setFormatter(formatter)
    fh_statistics.setFormatter(formatter)

    # add handlers to logger
    logger_err.addHandler(ch)
    logger_err.addHandler(fh_error)
    logger_info.addHandler(ch)
    logger_info.addHandler(fh_info)
    logger_done.addHandler(ch)
    logger_done.addHandler(fh_done)
    logger_statistics.addHandler(ch)
    logger_statistics.addHandler(fh_statistics)

# %%
def handle_error_and_rerun(error):
#     recipient_error = 'wojbal3@gmail.com'
    tb = traceback.format_exc()
    logger_err.error('An error occurred: {} on {}'.format(str(error), tb))
#     subject = f'Error occurred - {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}'
#     message = f'<html><body><p>Error occurred: {str(error)} on {tb}</p></body></html>'
#     send_email(subject, message, recipient_error)

# %%
def create_log_done(log_type):
    global file_path_logs_processed
    if log_type == 'Raw':
        with open(f'{file_path_logs_processed}-raw.txt', 'w') as file:
            file.write('Done')
    elif log_type == 'Refined':
        with open(f'{file_path_logs_processed}-refined.txt', 'w') as file:
            file.write('Done')

# %%
def initilize_driver() -> WebDriver:
    try:
        logger_info.info("Initializing the Chrome driver and logging into the website")

        # Setting up Chrome options
        options = webdriver.ChromeOptions()
        # options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--blink-settings=imagesEnabled=false')

        # Initialize the Chrome driver
        # service = Service()
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        
        return driver

    except Exception as e:
        logger_err.error(f"An error occurred during login: {e}")
        raise
    
def quit_driver(driver: WebDriver) -> None:
    driver.quit()    

# %%
def get_first_days_of_current_and_next_six_months() -> list:
    try:
        logger_info.info("Starting to get first days of current and next six months")

        # Current date
        current_date = datetime.now()

        # List to store first days of the current and next six months
        first_days = []

        # Add the first day of the current month
        first_day_current_month = datetime(current_date.year, current_date.month, 1)
        first_days.append(first_day_current_month.strftime("%Y-%m-%d"))

        # Add the first days of the next six months
        for month in range(1, 7):
            # Calculate the first day of each future month
            year = current_date.year
            month_num = current_date.month + month

            # Adjust for year change
            if month_num > 12:
                month_num -= 12
                year += 1

            first_day = datetime(year, month_num, 1)
            first_days.append(first_day.strftime("%Y-%m-%d"))

        logger_done.info("Successfully retrieved first days of current and next six months")
        return first_days

    except Exception as e:
        logger_err.error(f"Error in get_first_days_of_current_and_next_six_months: {e}")
        raise
    

# %%
def change_currency(driver):
# Wait for the currency selector dropdown to be clickable
    currency_selector = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'select[id="footer-currency-selector"]'))
    )

    # Create a Select object for the dropdown
    select = Select(currency_selector)

    # Get the currently selected option
    selected_option = select.first_selected_option
    current_currency = selected_option.text.strip()
    logger_info.info(f"Current currency selected: {current_currency}")

    # Change currency if it does not match the desired currency
    if "EUR" not in current_currency:
        logger_info.info(f"Changing currency to EUR.")
        desired_currency_text = 'Euro (€)'

        try:
            select.select_by_visible_text(desired_currency_text)
            logger_info.info(f"Selected currency '{desired_currency_text}' successfully.")
            time.sleep(2)  # Wait for the currency change to take effect
        except Exception as e:
            logger_err.error(f"Failed to select currency '{desired_currency_text}': {e}")
    else:
        logger_info.info(f"Currency already set to {desired_currency_text} for city '{city}'.")

def save_and_erase_dataframe(df: pd.DataFrame, url_city_id, url_unique_identifier) -> pd.DataFrame:
    ## SAVE
    file_path = fr'{output_gyg}/{extraction_date_save_format}-{url_city_id}-GYG.csv' 
    df['city'] = url_city_id.split('-')[0]
    df['uid'] = url_unique_identifier
    df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
    logger_done.info(f'Sucessfully upserted {len(df)} rows to {file_path}')
    return pd.DataFrame()
def extract_options(driver, option_detials, activity_title, language, adults_amount, url, viewer) -> list:
    list_of_items = []
    #iterate through all options
    time.sleep(1)
    option_date = driver.current_url.split('date_from=')[-1].split('&')[0]
    
    for option in option_detials:
        option_title = option.find_element(By.CLASS_NAME, 'activity-option__title').text
        try:
            option_time_range = option.find_element(By.CLASS_NAME, 'activity-option__start-time-range').text
        except:
            option_time_range = 'Not listed'
        try:
            option_price_total = option.find_element(By.CLASS_NAME, 'activity-option-price-wrapper__price').text
            option_price_per_person = float(option.find_element(By.CLASS_NAME, 'activity-option-price-wrapper__price').text.replace('€', '').replace(',', '').strip()) / float(adults_amount)
        except:
            try:
                option_price_total = option.find_element(By.CLASS_NAME, 'activity-option__cart-message-text--future-availability').text
            except:
                option_price_total = option.find_element(By.CLASS_NAME, 'activity-option-cart-message-wrapper').text
            option_price_per_person = 'Not Available'
        try:
            spots_left = option.find_element(By.CSS_SELECTOR, "span[data-test-id*='activity-option-is-x-spots-left']").text
        except:
            spots_left = 'N/A'

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
        logger_done.info(f'Sucessfuly extracted | {option_title} | {option_time_range} | {option_price_total} | {option_price_per_person} |')
    return list_of_items

def process_days_not_available(option_date, activity_title, language, adults_amount, url, viewer) -> list:
    list_of_items = []
    list_of_items.append({
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
            }) 
    return list_of_items   

def activity_not_availabke_in_selected_language():
    logger_err.error("The language picker does not exisit and selected language is different from defauly english")



# %%
def check_if_current_day_done_or_partly_done(url_city_id, url_unique_identifier):
    """
    Check if the current day's task is done or partly done by searching for matching files,
    and find the maximum date for the given UID if found in multiple files.
    """
    # Extract the date part and the fixed part from the extraction_date_save_format
    date_part = extraction_date_save_format.split('_')[0]  # e.g., '2024-05-27'
    fixed_part = extraction_date_save_format.split('_', 2)[2]  # e.g., 'en_2' (language_adults)

    # Define file patterns for current folder and archive folder
    file_pattern = f'{output_gyg}/{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv'
    file_pattern_archive = f'{archive_folder}/{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv'

    logger_info.info(f'File patterns: {file_pattern}, {file_pattern_archive}')

    # Use glob to find all matching files
    matching_files = glob.glob(file_pattern) + glob.glob(file_pattern_archive)
    logger_info.info(f'Matching files: {matching_files}')

    if not matching_files:
        # No files found
        return False, None

    # Load all files into a single DataFrame
    combined_df = pd.DataFrame()
    for file_path in matching_files:
        if os.path.exists(file_path):
            temp_df = pd.read_csv(file_path)
            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

    if combined_df.empty:
        # No data in the combined DataFrame
        return False, None

    # Filter by the UID
    filtered_df = combined_df[combined_df['uid'] == url_unique_identifier]

    if filtered_df.empty:
        # UID not found in any file
        return False, None

    # Ensure 'date' column exists and convert to datetime
    if 'date' in filtered_df.columns:
        filtered_df['date'] = pd.to_datetime(filtered_df['date'], errors='coerce')
        # Find the maximum date
        max_date = filtered_df['date'].max()
        if pd.notnull(max_date):
            return True, max_date.date()  # Return as a date object for consistency

    # If no valid date is found, return False
    return False, None

def check_if_today_done_on_schedule_in_csv(url):

    url_unique_identifier = url.split('.com/')[-1].split('-')[-1].replace('/', '')
    url_city_id = url.split('.com/')[-1].split('/')[0]
    # Extract the date part and the fixed part from the extraction_date_save_format
    date_part = extraction_date_save_format.split('_')[0] # --> 2024-05-27
    fixed_part = extraction_date_save_format.split('_', 2)[2] # --> en_2 (language_adults)
    # Define the file pattern with wildcards for the hour and minute components
    file_pattern = f'{archive_folder}/{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv'
    logger_info.info(f'File_pattern: {file_pattern}')
    
    # Use glob to find all matching files
    matching_files = glob.glob(file_pattern)
    logger_info.info(f'matching_files: {matching_files}')
    ### To implement option to run more than ince a day if the shceuld is higher to run once again
    ### Rin based on scheuld which is number on which the calculte the number of runs per month
    for file_path in matching_files:
        # Check if the file exists
        if os.path.exists(file_path):
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            df = df[df['uid'] == url_unique_identifier]
            if len(df) == 0:
                return False
            else:
                return True
        # If the file does not exist or 'date' column is not found, return False and None
    return False
# %%
def check_for_modal_window_to_close(driver, calendar_picker):
    try:
        modal_close_button = driver.find_element(By.CSS_SELECTOR,"button[class='tfe-modal-header__close']")
        modal_close_button.click()
        time.sleep(2)
        driver.execute_script("arguments[0].scrollIntoView(true);", calendar_picker)
        calendar_picker.click()
    except:
        driver.execute_script("arguments[0].scrollIntoView(true);", calendar_picker)
        calendar_picker.click()

def check_and_click_only_essential(driver):
    try:
        # Wait until the cookie banner is visible
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".ot-sdk-container"))
        )
        
        # Check if the "Only Essential" button is present
        only_essential_button = driver.find_element(By.ID, "onetrust-reject-all-handler")
        
        if only_essential_button:
            # Click the "Only Essential" button
            only_essential_button.click()
            print("Clicked 'Only Essential'.")
        else:
            print("'Only Essential' button not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
# %%
def get_future_price(driver, url, viewer, language, adults_amount,  max_days_to_complete):
    logger_info.info(f"Adults amount: {adults_amount}")
    logger_info.info(f"Language: {language}")
    logger_info.info(f"Max days to complete: {max_days_to_complete}")
    start_time_one_link = time.time()
    # df_links = pd.read_csv(link_file)
    # df_links = df_links[df_links['run'] == 1]
    # driver = initilize_driver()

    # for _, row in df_links.iterrows():
    url_id = url
    url_unique_identifier = url.split('.com/')[-1].split('-')[-1].replace('/', '')
    url_city_id = url.split('.com/')[-1].split('/')[0]
    
    
    start_collection_date = date_today

    date_today_obj = (datetime.datetime.strptime(start_collection_date, "%Y-%m-%d"))
    url_detials = f'?lang={language}&date_from={start_collection_date}&_pc=1,{adults_amount}'
    
    url = url + url_detials

    # Set the number of days to complete the task
    #DEFINED IN FUCNTION CALL
    # language = 'en'
    # adults_amount = '2'
    # max_days_to_complete = 60

    # Calculate the future date by adding max_days_to_complete to the current date, then extract the day number
    # and convert it to a string. This represents the day number after the specified days are added to the current date.    
    picked_max_day_number = str((date_today_obj + datetime.timedelta(days=max_days_to_complete)).day)

    # Calculate the future date by adding max_days_to_complete to the current date
    # This is used to get a complete date object for further manipulation or formatting.
    picked_max_date_obj = (date_today_obj + datetime.timedelta(days=max_days_to_complete))

    # Format the future date object as a string in the 'YYYY-MM-DD' format
    # This provides a standard date format that can be used for display or storage.
    picked_max_date = picked_max_date_obj.strftime('%Y-%m-%d')

    # Calculate the number of months to complete the task by subtracting the current month from the month
    # of the future date and adding 1. This gives the total number of months covering the period from the current date to the future date.
    # Note: This calculation assumes the task spans within a single year and is intended for short-term calculations.
    current_date = datetime.datetime.now()

    year_diff = picked_max_date_obj.year - current_date.year
    month_diff = picked_max_date_obj.month - current_date.month

    if year_diff > 0:
        # Adjust for the next year
        month_to_complete = month_diff + (12 * year_diff) + 1
    else:
        month_to_complete = month_diff + 1

    current_year = datetime.datetime.now().year

    driver.get(url)
    logger_info.info(f'URL: {url} UNIQUE ID: {url_unique_identifier}')
    logger_info.info(f'Months to complete: {month_to_complete} Picked Max Date {picked_max_date}')

    #Check if the language is available for that activity

    is_done, max_date_done = check_if_current_day_done_or_partly_done(url_city_id=url_city_id, url_unique_identifier=url_unique_identifier)
    if is_done:
        # Check if max_Date from dataframe is max date to collect from webpage
        logger_info.info(f'TO REMOVE @@@ {max_date_done} == {picked_max_date}')
        if max_date_done == picked_max_date_obj.date() :
            logger_done.info(f'Url was already done today with max date: {max_date_done}')
            # continue
            return f'Url was already done today with max date: {max_date_done}'
        else:
            url = url.replace(start_collection_date, max_date_done.strftime('%Y-%m-%d'))
            driver.get(url)
    #     VERIFY IF THE CURRENCY IS CORRECT
    login_button = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
    login_button.click()

    # currency_switcher_button = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))

    change_currency(driver)
    #css selector for the box where is availability
    activity_title = driver.find_element(By.CSS_SELECTOR, "h1[data-track='activity-title']").text
    css_selector_booking_tile = f"div[data-track='booking-assistant']"
    #Initiate booking tile varaible 
    booking_tile = WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector_booking_tile)))
    #Initiate calendar picker 
    calendar_picker = booking_tile.find_element(By.CSS_SELECTOR,"section[class='ba-dropdown ba-date-picker']")

    # css selector for the button check availability
    css_selector_check_availability = f"button[class*='js-check-availability']"
    button_check_availability = WebDriverWait(booking_tile, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector_check_availability)))
    try:
        # Check if the element with the specified data-test-id exists
        element = driver.find_element(By.CSS_SELECTOR, '[data-test-id="activity-filters-primary-language-picker"]')
        if element:
            print("Element with data-test-id 'activity-filters-primary-language-picker' exists.")
    except NoSuchElementException:
        if language != "en":
            print("Element with data-test-id 'activity-filters-primary-language-picker' does not exist.")
            #Click  Check availability (once only)
            activity_not_availabke_in_selected_language()
            #Skipping the URL
            return 
        # continue
    time.sleep(2)
    check_and_click_only_essential(driver)
    driver.execute_script("arguments[0].click();", button_check_availability)
    

    try:
        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CLASS_NAME, 'dayContainer')))
        months = driver.find_elements(By.CLASS_NAME, 'dayContainer')
        for month in months:
            days_available = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day-available]")
            if len(days_available) == 0:
                continue
            for day in days_available:
                if day.text == "" or len(day.text) == 0:
                    print("Day was empty")
                    empty = True
                    continue
                # Scroll the day into view using JavaScript
                # driver.execute_script("arguments[0].scrollIntoView();", day)
                # Alternatively, you can use ActionChains to move to the element before clicking
                ActionChains(driver).move_to_element(day).perform()
                empty = False
                day.click()
                driver.execute_script("arguments[0].click();", button_check_availability)
                break
            if not empty:
                break
    except:
        print('Current date is avialable')
        pass
        # After click on the element wait for the option detials to show up
    WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.TAG_NAME, 'details')))
    option_detials = driver.find_elements(By.TAG_NAME, 'details')
    option_date = driver.current_url.split('date_from=')[-1].split('&')[0]
    current_date = option_date.split('-')[-1]
    print(f"Current date: {current_date}")
    list_of_items = extract_options(driver=driver, option_detials=option_detials, activity_title=activity_title, language=language, url=url_id, viewer=viewer, adults_amount=adults_amount)
    ### After extraction transform to dataframe and save it in the CSV file in case of any error in th future
    df = pd.DataFrame(list_of_items)
    # display(df)
    ## SAVE
    df = save_and_erase_dataframe(df, url_city_id, url_unique_identifier)
    try:
        calendar_picker.click()
    except:
        check_for_modal_window_to_close(driver, calendar_picker)
    #Wait until the calendar is fully loaded
    WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.CLASS_NAME, 'dayContainer')))
    months = driver.find_elements(By.CLASS_NAME, 'dayContainer')
    month_done = False


    month = months[0]
    current_month_done = False
    for i in range(0, month_to_complete):
        days_to_complete = []
        if current_month_done == True:
            months = driver.find_elements(By.CLASS_NAME, 'dayContainer')
            month = months[1]
            current_month = driver.find_elements(By.CLASS_NAME, "flatpickr-current-month")[1].text.strip()
            if current_month == "January":
                current_year = current_year + 1
            current_month_done = False
        else:
            current_month = driver.find_elements(By.CLASS_NAME, "flatpickr-current-month")[0].text.strip()

        days_available = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day-available]") 
        for day in days_available:
            current_month_done = True
            if len(day.text) > 0:
                try:
                    day_date_str = f"{current_month} {day.text.strip()}, {current_year}"
                    day_date_obj = datetime.datetime.strptime(day_date_str, '%B %d, %Y')
                    # Append date to complete in run based on maxium dates
                    if day_date_obj <= picked_max_date_obj and day_date_obj >= date_today_obj:
                        #If was done already some part remove days which are done
                        if is_done:
                            if day_date_obj.date() >= max_date_done:
                                days_to_complete.append(day_date_str)
                        else:
                            days_to_complete.append(day_date_str)
                except ValueError as e:
                    print(f"Error parsing date: {str(e)}")

        #### Get day which are not available
        days_not_available_elements = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day]")
        days_not_available = []
        for day_not_available in days_not_available_elements:
            if len(day_not_available.text) > 0 and int(day_not_available.text) >= int(date_today.split('-')[-1]):
                days_not_available.append(f"{current_year}-{(datetime.datetime.strptime(current_month, '%B').month):02d}-{int(day_not_available.text):02d}")
        
        for day in days_not_available:
            list_of_items = process_days_not_available(activity_title=activity_title, adults_amount=adults_amount, language=language, option_date=day, url=url_id, viewer=viewer)
            df = pd.DataFrame(list_of_items)
            df = save_and_erase_dataframe(df, url_city_id, url_unique_identifier)
        ####    
        for day in days_to_complete:
            try:
                day_js = month.find_element(By.CSS_SELECTOR, f"span[aria-label*='{day}']") 
                day_js.click()
            except:
                months = driver.find_elements(By.CLASS_NAME, 'dayContainer')
                month = months[0]
                day_js = month.find_element(By.CSS_SELECTOR, f"span[aria-label*='{day}']") 
                day_js.click()
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.TAG_NAME, 'details')))
            option_detials = driver.find_elements(By.TAG_NAME, 'details')
            time.sleep(2)
            list_of_items = extract_options(driver=driver, option_detials=option_detials, activity_title=activity_title, language=language, url=url_id, viewer=viewer, adults_amount=adults_amount)
            ### After extraction transform to dataframe and save it in the CSV file in case of any error in th future
            df = pd.DataFrame(list_of_items)
            # display(df)
            ## SAVE
            df = save_and_erase_dataframe(df, url_city_id, url_unique_identifier)
            check_for_modal_window_to_close(driver, calendar_picker)
            

            print(day, '----' , picked_max_day_number, day == picked_max_day_number)
            # if day_text == picked_max_day_number or day_text == month_last_day:
            #     i+=1
            #     break  
            
        i+=1
    end_time_one_link = time.time()
    logger_statistics.info(f"Time requried for {max_days_to_complete} days: {(end_time_one_link - start_time_one_link):.2f} Time reqruied for 1 day: {((end_time_one_link - start_time_one_link)/max_days_to_complete):.2f}")
    

    # quit_driver(driver)




# %%
# combine processed files:
def process_csv_files(folder_path, adults, language):
    # Determine today's date in the desired format    

    output_file_path = os.path.join(folder_path, f"{extraction_date_save_format}_future_price.xlsx")
    if os.path.exists(output_file_path):
        print("Output file already exists. Exiting function.")
        return
    # Ensure the archive directory exists
    archive_path = os.path.join(folder_path, "archive")
    os.makedirs(archive_path, exist_ok=True)
    
    # Initialize an empty DataFrame to hold all the data
    combined_df = pd.DataFrame()
    
    # Iterate over files in the folder
    for filename in os.listdir(folder_path):
        if filename.startswith(date_today) and filename.endswith('.csv') and f"{language}_{str(adults)}" in filename:
            file_path = os.path.join(folder_path, filename)
            
            # Read the CSV file and append its contents to the DataFrame
            df = pd.read_csv(file_path)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            
            # Move the processed file to the archive folder
            shutil.move(file_path, os.path.join(archive_path, filename))
    
    # Save the combined DataFrame to a new CSV file
    combined_df.to_excel(output_file_path, index=False)

    print(f"All data has been combined and saved to {output_file_path}.")


# %%

def upload_excel_to_azure_storage_account(local_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name):
    try:
        # Create a connection string to the Azure Storage account
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"

        # Create a BlobServiceClient object using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name_raw)

        # Upload the file to Azure Blob Storage
        with open(local_file_path, "rb") as file:
            container_client.upload_blob(name=blob_name, data=file)
        create_log_done('Raw')
        print("File uploaded successfully to Azure Blob Storage (raw).")

    except Exception as e:
        print(f"An error occurred: {e}")

# %%
def extract_date_from_price(text):
    if 'Next available date:' in str(text):
        # Extract the date using string manipulation
        date_part = text.split('Next available date: ')[1].strip()
        
        # Parse the date string into a datetime object
        try:
            date_obj = datetime.datetime.strptime(date_part, "%A, %B %d, %Y")
            # Format the datetime object to "YYYY-MM-DD"
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            print("Date format mismatch or error in parsing date.")
            return np.nan
    else:
        return np.nan
    
# Dynamic function to check the 'option' and set message
def dynamic_message_option(option):
    # Define patterns and their corresponding messages
    messages = {
        'Please select 1 participants or fewer for this activity.': 'set adults to 1',
        'Maximum 2 adults allowed per booking': 'adults set to 2'
        # Add more patterns and messages as needed
    }
    
    # Check each pattern and return the corresponding message if found
    for pattern, message in messages.items():
        if pattern in str(option):
            return message
    
    # Default return if no patterns matched
    return np.nan

def set_to_long_price_to_nan(text):
    text = str(text)
    if len(text) > 15 or len(text) == 0 or text == 'nan':
        return np.nan
    else:
        return text

# %%
def transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name):
    # Define the Azure Blob Storage connection details
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    # Read the Excel file into a Pandas DataFrame
    df = pd.read_excel(local_file_path)
    # city replacment if there are incorrect in url
    
    # Make changes to the df DataFrame as needed
    df['extraction_date'] = df['extraction_date'].astype('str')
    df['date'] = df['date'].astype('str')
    df['availability'] = df['total_price'].apply(extract_date_from_price)
    df['message'] = df['total_price'].apply(dynamic_message_option)
    df['total_price'] = df['total_price'].apply(set_to_long_price_to_nan)
    df['price_per_person'] = df['price_per_person'].replace('Not Available', np.nan)
    df['total_price'] = df['total_price'].str.replace(r'[$€£]', '', regex=True).str.replace(',', '').str.strip()
    # The first letter is capitalized
    df['city'] = df['city'].str.title()
    # df['total_price'] = df['total_price'].map(lambda x: x.split(x[0]) [1].strip() if not x[0].isnumeric() else x)
    # df['price_per_person'] = df['price_per_person'].map(lambda x: x.split(x[0])[1].strip() if not x[0].isnumeric() else x))

    # Save modified DataFrame to an Excel file temporarily
    output_file_path = "temp_modified_excel.xlsx"
    df.to_excel(output_file_path, index=False)
    # Create a connection to Azure Blob Storage
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name_refined)
        with open(output_file_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        print("File uploaded successfully to Azure Blob Storage (refined).")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Optional: Remove the temporary file if no longer needed
        os.remove(output_file_path)

    # Function for logging (create_log_done) needs to be defined or replaced
    create_log_done('Refined')

    return 'Added to Blob'


# %%
###
### TO DO: CONFIFURE THE SCRIPT BELOW TO RUN ON BETTER SCHEDULE. AS OF NOW IF THE WEEKLY UPDATE (4) WILL BE SAME DAY AS MONTHLY (1) THE WEEKLY UPDATE WILL BE DONE 3 TIMES A MONTH NOT 4
### WHERE IT SHOULD RUN ON BASED FREQEUNCY IF REQEUSTED WEEKLY IN A MONTH THE UDPATE SHOULD BE TRIGGERED 4 TIMES IN MONTH EVEN IF THE DAY IS THE SAME AS HIGEHR REFRESH, 
### THEN IT SHOUDL RUN LATER IN MONTH TO KEEP AMOIUNT OF REFREHSES AS REQESUTED PER CLIENT
###

def should_run_today(day, month_length, frequency):
    if frequency == 1:
        # Run only on the second day of the month
        if day == 2:
            return True
    else:
        # Run multiple times a month
        interval = month_length // frequency
        for i in range(frequency):
            if day == 1 + i * interval:
                return True
        return False

def calculate_execution_days(frequency, month_length):
    """
    Calculate the days of execution in the month based on the frequency.
    """
    execution_days = []
    if frequency == 1:
        # Only the first day of the month
        execution_days.append(1)
    else:
        # Run multiple times a month
        interval = month_length // frequency
        for i in range(frequency):
            execution_day = 1 + i * interval
            if execution_day <= month_length:
                execution_days.append(execution_day)
    return execution_days

def get_schedule_execution_days(schedules):
    """
    For each schedule frequency, determine the days of execution within the current month.
    """
    today = datetime.datetime.today()
    month_length = calendar.monthrange(today.year, today.month)[1]
    
    schedule_days = {}
    for freq in schedules.keys():
        frequency = int(freq)
        execution_days = calculate_execution_days(frequency, month_length)
        schedule_days[frequency] = execution_days
        print(f"Frequency {frequency} times/month has execution days: {execution_days}")
    
    return schedule_days

def get_highest_order_schedule(schedules):
    today = datetime.datetime.today()
    day = today.day
    month_length = calendar.monthrange(today.year, today.month)[1]
    
    # Sort schedules by frequency in ascending order
    sorted_schedules = sorted(schedules.items(), key=lambda x: int(x[0]), reverse=False)
    for freq, value in sorted_schedules:
        frequency = int(freq)
        if should_run_today(day, month_length, frequency):
            # logger_info.info(f"Today is a run day for frequency {frequency} times a month with value {value}")
            return freq, value

    return "No schedule for today", None


# %%
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Get future price based on input parameters")
#     parser.add_argument('--ADULTS', type=str, default='2', help='Amount of adults')
#     parser.add_argument('--LANGUAGE', type=str, default='en', help='Language preference')
#     parser.add_argument('--MAX_DAYS', type=int, default=2, help='Maximum days to complete')

#     args = parser.parse_args() 

#     print(args.ADULTS, args.LANGUAGE, args.MAX_DAYS)
#     configure_dates_and_file_names(args.ADULTS, args.LANGUAGE)
#     define_logging()
#     get_future_price(adults_amount=args.ADULTS, language=args.LANGUAGE, max_days_to_complete=args.MAX_DAYS)
#     process_csv_files(output_gyg)
#     upload_excel_to_azure_storage_account(output_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name)
#     transform_upload_to_refined(output_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name)

# %%


# %%
# constant_file_path()

# with open(link_file_path) as f:
#     config = json.load(f)

# define_logging()
# # driver = initilize_driver()
# combinations = set()
# for site in config['urls']:
#     url = site['url']
#     viewer = site["viewer"]
#     for config in site['configurations']:
#         adults = config['adults']
#         language = config['language']
#         schedules = config['schedules']
        
#         schedule, max_days = get_highest_order_schedule(schedules)
#         if schedule == "No schedule for today":
#             logger_done.info(f"URL: {url} is not scheduled for today to run")
#             continue
#         configure_dates_and_file_names(adults, language)
#         #Check if current day was done and its in Archive folder
#         today_file_in_arhcive = check_if_today_done_on_schedule(url=url, schedule=schedule)
#         if today_file_in_arhcive:
#             logger_done.info(f"File in archive for URL: {url}, Adults: {adults}, Language: {language} ")
#         else:
#             print(f"Running script for URL: {url}, Adults: {adults}, Language: {language}, Max Days: {max_days}")

# %%
constant_file_path()

site = 'GYG'
file_manager__config_path = FilePathManagerFuturePrice(site, "N/A", "N/A", "N/A")
config_reader = ConfigReader(file_manager__config_path.config_file_path)
urls = config_reader.get_urls_by_ota(site)

define_logging()
driver = initilize_driver()
combinations = set()
for item in urls:
    url = item['url']
    viewer = item["viewer"]
    for config in item['configurations']:
        adults = config['adults']
        language = config['language']
        schedules = config['schedules']
        
        frequency, max_days = config_reader.get_highest_order_schedule(schedules)
        if frequency.lower() == "no schedule for today":
            # logger_done.info(f"URL: {url} is not scheduled for today to run")
            continue  # Use 'continue' to process other configurations
       #Check if current day was done and its in Archive folder
        configure_dates_and_file_names(adults, language)
        today_file_in_arhcive = check_if_today_done_on_schedule_in_csv(url=url)
        if today_file_in_arhcive:
            logger_done.info(f"File in archive for URL: {url}, Adults: {adults}, Language: {language} ")
            
        else:
            logger_done.info(f"Running script for URL: {url}, Adults: {adults}, Language: {language}, Frequency: {frequency}, Max Days: {max_days}")
            get_future_price(driver=driver, adults_amount=adults ,language=language, max_days_to_complete=max_days, url=url, viewer=viewer)
            # Store the combination for later processing
        combinations.add((adults, language))

quit_driver(driver)

for adults, language in combinations:
    configure_dates_and_file_names(adults, language)
    process_csv_files(output_gyg, adults, language)
    upload_excel_to_azure_storage_account(output_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name)
    transform_upload_to_refined(output_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name)
# %%

# %%
#### DEBUG RUN FOR CHECKING DAYS WHEN PRODUCT WILL BE DONE
# constant_file_path()

# with open(link_file_path) as f:
#     config = json.load(f)
# combinations = set()
# for site in config['urls']:
#     url = site['url']
#     viewer = site["viewer"]
#     for config in site['configurations']:
#         adults = config['adults']
#         language = config['language']
#         schedules = config['schedules']
#         schedule_days = get_schedule_execution_days(schedules)
#         print(f"For configuration {url} - {adults} adults, schedule days are: {schedule_days}")

# %%
### DEBUG RUN
# constant_file_path()

# with open(link_file_path) as f:
#     config = json.load(f)
# # Initialize an empty set to store unique combinations
# combinations = set()

# # Loop through each site and its configurations
# for site in config['urls']:
#     url = site['url']
#     viewer = site["viewer"]
#     for config in site['configurations']:
#         adults = config['adults']
#         language = config['language']
#         schedules = config['schedules']
        
        
#         # Add the combination (adults, language) to the set
#         combinations.add((adults, language))

# # Output the unique combinations set
# print(combinations)

# %%

# adults= 4
# language
# configure_dates_and_file_names(adults, language)
# define_logging()
# schedule, max_days = get_highest_order_schedule(schedules)

# driver = initilize_driver()
# get_future_price(driver=driver, adults_amount=adults ,language=language, max_days_to_complete=max_days, url=url, viewer=viewer)

# %%
# adults = 6
# language = 'en'
# configure_dates_and_file_names(adults, language)
# process_csv_files(output_gyg, adults, language)
# upload_excel_to_azure_storage_account(output_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name)
# transform_upload_to_refined(output_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name)


# %%




