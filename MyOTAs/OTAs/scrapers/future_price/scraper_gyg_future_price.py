import os
import sys
import time
import datetime
import pandas as pd
import traceback
import logging
import shutil
import glob
import numpy as np

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_gyg import ScraperGYG
from file_management.file_path_manager_future_price import FilePathManagerFuturePrice
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select

class ScraperGYGFuturePrice(ScraperGYG):
    """
    A scraper class for GetYourGuide (GYG) website to extract future price data.
    Inherits from ScraperGYG and extends it for future price scraping.
    """
    def __init__(self, url, city, css_selectors, file_manager, logger, activity_per_page=16, provider=False, adults='2', language='en'):
        super().__init__(url, city, css_selectors, file_manager, logger, activity_per_page, provider)
        self.adults = adults
        self.language = language
        self.extraction_date = datetime.datetime.now().strftime('%Y-%m-%d %H:00:00')
        self.extraction_date_save_format = f"{self.extraction_date.replace(' ', '_').replace(':','-')}_{self.language}_{self.adults}"

    def handle_error_and_rerun(self, error):
        """
        Handles errors by logging them and implementing any necessary rerun logic.
        """
        tb = traceback.format_exc()
        self.logger.logger_err.error(f'An error occurred: {error}\nTraceback: {tb}')
        # Placeholder for additional error handling

    def change_currency(self):
        """
        Changes the currency to EUR if not already set.
        """
        try:
            # Wait for the currency selector dropdown to be clickable
            currency_selector = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'select[id="footer-currency-selector"]'))
            )

            # Create a Select object for the dropdown
            select = Select(currency_selector)

            # Get the currently selected option
            selected_option = select.first_selected_option
            current_currency = selected_option.text.strip()
            self.logger.logger_info.info(f"Current currency selected: {current_currency}")

            # Change currency if it does not match the desired currency
            desired_currency_text = 'Euro (€)'
            if "EUR" not in current_currency:
                self.logger.logger_info.info(f"Changing currency to EUR.")
                try:
                    select.select_by_visible_text(desired_currency_text)
                    self.logger.logger_info.info(f"Selected currency '{desired_currency_text}' successfully.")
                    time.sleep(2)  # Wait for the currency change to take effect
                except Exception as e:
                    self.logger.logger_err.error(f"Failed to select currency '{desired_currency_text}': {e}")
            else:
                self.logger.logger_info.info(f"Currency already set to {desired_currency_text}.")
        except Exception as e:
            self.logger.logger_err.error(f"Error changing currency: {e}")

    def save_and_erase_dataframe(self, df, url_city_id, url_unique_identifier):
        """
        Saves the DataFrame to a CSV file and resets it.
        """
        file_path = f"{self.file_manager.output}/{self.extraction_date_save_format}-{url_city_id}-GYG.csv"
        df['city'] = url_city_id.split('-')[0]
        df['uid'] = url_unique_identifier
        df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
        self.logger.logger_done.info(f"Successfully upserted {len(df)} rows to {file_path}")
        return pd.DataFrame()

    def extract_options(self, option_details, activity_title, url, viewer):
        """
        Extracts options from the activity details.
        """
        list_of_items = []
        # Iterate through all options
        time.sleep(1)
        option_date = self.driver.current_url.split('date_from=')[-1].split('&')[0]

        for option in option_details:
            option_title = option.find_element(By.CLASS_NAME, 'activity-option__title').text
            try:
                option_time_range = option.find_element(By.CLASS_NAME, 'activity-option__start-time-range').text
            except:
                option_time_range = 'Not listed'
            try:
                option_price_total = option.find_element(By.CLASS_NAME, 'activity-option-price-wrapper__price').text
                option_price_per_person = float(option.find_element(By.CLASS_NAME, 'activity-option-price-wrapper__price').text.replace('€', '').replace(',', '').strip()) / float(self.adults)
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
                'extraction_date': self.extraction_date,
                'date': option_date,
                'title': activity_title,
                'tour_option': option_title,
                'time_range': option_time_range,
                'total_price': option_price_total,
                'price_per_person': option_price_per_person,
                'language': self.language,
                'adults': self.adults,
                'spots_left': spots_left,
                'title_url': url,
                'viewer': viewer
            })
            self.logger.logger_done.info(f'Successfully extracted | {option_title} | {option_time_range} | {option_price_total} | {option_price_per_person} |')
        return list_of_items

    def process_days_not_available(self, option_date, activity_title, url, viewer):
        """
        Processes days that are not available.
        """
        list_of_items = []
        list_of_items.append({
            'extraction_date': self.extraction_date,
            'date': option_date,
            'title': activity_title,
            'tour_option': "N/A",
            'time_range': "N/A",
            'total_price': "N/A",
            'price_per_person': "N/A",
            'language': self.language,
            'adults': self.adults,
            'spots_left': "N/A",
            'title_url': url,
            'viewer': viewer
        })
        return list_of_items

    def activity_not_available_in_selected_language(self):
        """
        Logs that the activity is not available in the selected language.
        """
        self.logger.logger_err.error("The language picker does not exist and selected language is different from default English")

    def check_if_current_day_done_or_partly_done(self, url_city_id, url_unique_identifier):
        """
        Checks if the current day is already done or partly done.
        """
        date_part = self.extraction_date_save_format.split('_')[0]  # e.g., 2024-05-27
        fixed_part = self.extraction_date_save_format.split('_', 2)[2]  # e.g., en_2
        file_pattern = f'{self.file_manager.output}/{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv'
        matching_files = glob.glob(file_pattern)
        for file_path in matching_files:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df = df[df['uid'] == url_unique_identifier]
                if len(df) == 0:
                    return False, None
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    max_date = df['date'].max()
                    return True, max_date.date()
        return False, None

    def check_for_modal_window_to_close(self, calendar_picker):
        """
        Checks for a modal window to close before interacting with the calendar picker.
        """
        try:
            modal_close_button = self.driver.find_element(By.CSS_SELECTOR, "button[class='tfe-modal-header__close']")
            modal_close_button.click()
            time.sleep(2)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", calendar_picker)
            calendar_picker.click()
        except:
            self.driver.execute_script("arguments[0].scrollIntoView(true);", calendar_picker)
            calendar_picker.click()

    def get_future_price(self, url, viewer, max_days_to_complete,):
        """
        Main method to get future prices.
        """
        self.logger.logger_info.info(f"Adults amount: {self.adults}")
        self.logger.logger_info.info(f"Language: {self.language}")
        self.logger.logger_info.info(f"Max days to complete: {max_days_to_complete}")
        start_time_one_link = time.time()

        url_id = url
        url_unique_identifier = url.split('.com/')[-1].split('-')[-1].replace('/', '')
        url_city_id = url.split('.com/')[-1].split('/')[0]
        date_today_obj = datetime.datetime.now()

        start_collection_date = date_today_obj.strftime('%Y-%m-%d')
        url_details = f'?lang={self.language}&date_from={start_collection_date}&_pc=1,{self.adults}'

        url = url + url_details

        picked_max_date_obj = date_today_obj + datetime.timedelta(days=max_days_to_complete)
        picked_max_date = picked_max_date_obj.strftime('%Y-%m-%d')

        # Calculate months to complete
        year_diff = picked_max_date_obj.year - date_today_obj.year
        month_diff = picked_max_date_obj.month - date_today_obj.month
        if year_diff > 0:
            # Adjust for the next year
            month_to_complete = month_diff + (12 * year_diff) + 1
        else:
            month_to_complete = month_diff + 1

        current_year = date_today_obj.year

        self.driver.get(url)
        self.logger.logger_info.info(f'URL: {url} UNIQUE ID: {url_unique_identifier}')
        self.logger.logger_info.info(f'Months to complete: {month_to_complete} Picked Max Date {picked_max_date}')

        is_done, max_date_done = self.check_if_current_day_done_or_partly_done(url_city_id=url_city_id, url_unique_identifier=url_unique_identifier)
        if is_done:
            self.logger.logger_info.info(f'Already processed up to date: {max_date_done}')
            if max_date_done == picked_max_date_obj.date():
                self.logger.logger_done.info(f'Url was already done today with max date: {max_date_done}')
                return f'Url was already done today with max date: {max_date_done}'
            else:
                start_collection_date = max_date_done.strftime('%Y-%m-%d')
                url_details = f'?lang={self.language}&date_from={start_collection_date}&_pc=1,{self.adults}'
                url = url_id + url_details
                self.driver.get(url)

        # Verify if the currency is correct
        try:
            login_button = WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
            login_button.click()
        except Exception as e:
            self.logger.logger_err.error(f"Error clicking login button: {e}")

        self.change_currency()
        activity_title = self.driver.find_element(By.CSS_SELECTOR, "h1[data-track='activity-title']").text
        css_selector_booking_tile = f"div[data-track='booking-assistant']"
        booking_tile = WebDriverWait(self.driver, 60).until(EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector_booking_tile)))
        calendar_picker = booking_tile.find_element(By.CSS_SELECTOR, "section[class='ba-dropdown ba-date-picker']")

        css_selector_check_availability = f"button[class*='js-check-availability']"
        button_check_availability = WebDriverWait(booking_tile, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector_check_availability)))
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, '[data-test-id="activity-filters-primary-language-picker"]')
            if element:
                self.logger.logger_info.info("Language picker exists.")
        except NoSuchElementException:
            if self.language != "en":
                self.logger.logger_err.error("Language picker does not exist and selected language is different from default English.")
                self.activity_not_available_in_selected_language()
                return

        self.driver.execute_script("arguments[0].click();", button_check_availability)
        try:
            WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located((By.CLASS_NAME, 'dayContainer')))
            months = self.driver.find_elements(By.CLASS_NAME, 'dayContainer')
            for month in months:
                days_available = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day-available]")
                for day in days_available:
                    if day.text == "" or len(day.text) == 0:
                        continue
                    ActionChains(self.driver).move_to_element(day).perform()
                    day.click()
                    self.driver.execute_script("arguments[0].click();", button_check_availability)
                    break
                else:
                    continue
                break
        except:
            self.logger.logger_info.info('Current date is available')
            pass

        WebDriverWait(self.driver, 60).until(EC.visibility_of_element_located((By.TAG_NAME, 'details')))
        option_details = self.driver.find_elements(By.TAG_NAME, 'details')
        option_date = self.driver.current_url.split('date_from=')[-1].split('&')[0]
        list_of_items = self.extract_options(option_details=option_details, activity_title=activity_title, url=url_id, viewer=viewer)
        df = pd.DataFrame(list_of_items)
        df = self.save_and_erase_dataframe(df, url_city_id, url_unique_identifier)
        try:
            calendar_picker.click()
        except:
            self.check_for_modal_window_to_close(calendar_picker)

        WebDriverWait(self.driver, 60).until(EC.visibility_of_element_located((By.CLASS_NAME, 'dayContainer')))
        months = self.driver.find_elements(By.CLASS_NAME, 'dayContainer')
        current_month_done = False
        for i in range(month_to_complete):
            days_to_complete = []
            if current_month_done:
                months = self.driver.find_elements(By.CLASS_NAME, 'dayContainer')
                month = months[1]
                current_month = self.driver.find_elements(By.CLASS_NAME, "flatpickr-current-month")[1].text.strip()
                current_month_done = False
            else:
                current_month = self.driver.find_elements(By.CLASS_NAME, "flatpickr-current-month")[0].text.strip()

            month = months[0]
            days_available = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day-available]")
            for day in days_available:
                current_month_done = True
                if len(day.text) > 0:
                    try:
                        day_date_str = f"{current_month} {day.text.strip()}, {current_year}"
                        day_date_obj = datetime.datetime.strptime(day_date_str, '%B %d, %Y')
                        if day_date_obj <= picked_max_date_obj and day_date_obj >= date_today_obj:
                            if is_done:
                                if day_date_obj.date() > max_date_done:
                                    days_to_complete.append(day_date_str)
                            else:
                                days_to_complete.append(day_date_str)
                    except ValueError as e:
                        self.logger.logger_err.error(f"Error parsing date: {str(e)}")

            days_not_available_elements = month.find_elements(By.CSS_SELECTOR, "span[data-test-id=ba-calendar-day]")
            days_not_available = []
            for day_not_available in days_not_available_elements:
                if len(day_not_available.text) > 0 and int(day_not_available.text) >= int(start_collection_date.split('-')[-1]):
                    days_not_available.append(f"{current_year}-{(datetime.datetime.strptime(current_month, '%B').month):02d}-{int(day_not_available.text):02d}")

            for day in days_not_available:
                list_of_items = self.process_days_not_available(option_date=day, activity_title=activity_title, url=url_id, viewer=viewer)
                df = pd.DataFrame(list_of_items)
                df = self.save_and_erase_dataframe(df, url_city_id, url_unique_identifier)

            for day in days_to_complete:
                try:
                    day_js = month.find_element(By.CSS_SELECTOR, f"span[aria-label*='{day}']")
                    day_js.click()
                except:
                    months = self.driver.find_elements(By.CLASS_NAME, 'dayContainer')
                    month = months[0]
                    day_js = month.find_element(By.CSS_SELECTOR, f"span[aria-label*='{day}']")
                    day_js.click()
                WebDriverWait(self.driver, 60).until(EC.visibility_of_element_located((By.TAG_NAME, 'details')))
                option_details = self.driver.find_elements(By.TAG_NAME, 'details')
                time.sleep(2)
                list_of_items = self.extract_options(option_details=option_details, activity_title=activity_title, url=url_id, viewer=viewer)
                df = pd.DataFrame(list_of_items)
                df = self.save_and_erase_dataframe(df, url_city_id, url_unique_identifier)
                self.check_for_modal_window_to_close(calendar_picker)

            i += 1
        end_time_one_link = time.time()
        self.logger.logger_statistics.info(f"Time required for {max_days_to_complete} days: {(end_time_one_link - start_time_one_link):.2f} seconds. Time per day: {((end_time_one_link - start_time_one_link)/max_days_to_complete):.2f} seconds")

    def process_csv_files(self):
        """
        Processes the CSV files and combines them into an Excel file.
        """
        folder_path = self.file_manager.output
        adults = self.adults
        language = self.language
        extraction_date_save_format = self.extraction_date_save_format
        date_today = datetime.datetime.now().strftime("%Y-%m-%d")
        output_file_path = os.path.join(folder_path, f"{extraction_date_save_format}_future_price.xlsx")
        if os.path.exists(output_file_path):
            self.logger.logger_info.info("Output file already exists. Exiting function.")
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

        # Save the combined DataFrame to a new Excel file
        combined_df.to_excel(output_file_path, index=False)

        self.logger.logger_info.info(f"All data has been combined and saved to {output_file_path}.")

    def upload_excel_to_azure_storage_account(self):
        """
        Uploads the Excel file to Azure Blob Storage.
        """
        from azure.storage.blob import BlobServiceClient

        local_file_path = f"{self.file_manager.output}/{self.extraction_date_save_format}_future_price.xlsx"
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.file_manager.storage_account_name};AccountKey={self.file_manager.storage_account_key};EndpointSuffix=core.windows.net"

        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(self.file_manager.container_name_raw)
            with open(local_file_path, "rb") as file:
                container_client.upload_blob(name=self.file_manager.blob_name, data=file, overwrite=True)
            self.logger.logger_info.info("File uploaded successfully to Azure Blob Storage (raw).")
        except Exception as e:
            self.logger.logger_err.error(f"An error occurred during upload: {e}")

    def transform_upload_to_refined(self):
        """
        Transforms the data and uploads the refined file to Azure Blob Storage.
        """
        from azure.storage.blob import BlobServiceClient

        connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.file_manager.storage_account_name};AccountKey={self.file_manager.storage_account_key};EndpointSuffix=core.windows.net"
        local_file_path = f"{self.file_manager.output}/{self.extraction_date_save_format}_future_price.xlsx"
        df = pd.read_excel(local_file_path)

        # Transformations
        df['extraction_date'] = df['extraction_date'].astype('str')
        df['date'] = df['date'].astype('str')
        df['availability'] = df['total_price'].apply(self.extract_date_from_price)
        df['message'] = df['total_price'].apply(self.dynamic_message_option)
        df['total_price'] = df['total_price'].apply(self.set_to_long_price_to_nan)
        df['price_per_person'] = df['price_per_person'].replace('Not Available', np.nan)
        df['total_price'] = df['total_price'].str.replace(r'[$€£]', '', regex=True).str.replace(',', '').str.strip()
        df['city'] = df['city'].str.title()

        # Save modified DataFrame to an Excel file temporarily
        output_file_path = "temp_modified_excel.xlsx"
        df.to_excel(output_file_path, index=False)
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(self.file_manager.container_name_refined)
            with open(output_file_path, "rb") as data:
                container_client.upload_blob(name=self.file_manager.blob_name, data=data, overwrite=True)
            self.logger.logger_info.info("File uploaded successfully to Azure Blob Storage (refined).")
        except Exception as e:
            self.logger.logger_err.error(f"An error occurred during upload to refined: {e}")
        finally:
            os.remove(output_file_path)

    @staticmethod
    def extract_date_from_price(text):
        if 'Next available date:' in str(text):
            date_part = text.split('Next available date: ')[1].strip()
            try:
                date_obj = datetime.datetime.strptime(date_part, "%A, %B %d, %Y")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                print("Date format mismatch or error in parsing date.")
                return np.nan
        else:
            return np.nan

    @staticmethod
    def dynamic_message_option(option):
        messages = {
            'Please select 1 participants or fewer for this activity.': 'set adults to 1',
            'Maximum 2 adults allowed per booking': 'adults set to 2'
            # Add more patterns and messages as needed
        }
        for pattern, message in messages.items():
            if pattern in str(option):
                return message
        return np.nan

    @staticmethod
    def set_to_long_price_to_nan(text):
        text = str(text)
        if len(text) > 15 or len(text) == 0 or text == 'nan':
            return np.nan
        else:
            return text

    def get_highest_order_schedule(self, schedules):
        today = datetime.datetime.today()
        day = today.day
        month_length = (today.replace(month=today.month % 12 + 1, day=1) - datetime.timedelta(days=1)).day

        sorted_schedules = sorted(schedules.items(), key=lambda x: int(x[0]), reverse=False)
        for freq, value in sorted_schedules:
            frequency = int(freq)
            if self.should_run_today(day, month_length, frequency):
                return freq, value

        return "No schedule for today", None

    @staticmethod
    def should_run_today(day, month_length, frequency):
        if frequency == 1:
            if day == 1:
                return True
        else:
            interval = month_length // frequency
            for i in range(frequency):
                if day == 1 + i * interval:
                    return True
            return False

    def check_if_today_done_on_schedule(self, url, schedule):
        url_unique_identifier = url.split('.com/')[-1].split('-')[-1].replace('/', '')
        url_city_id = url.split('.com/')[-1].split('/')[0]
        date_part = self.extraction_date_save_format.split('_')[0]
        fixed_part = self.extraction_date_save_format.split('_', 2)[2]
        archive_folder = os.path.join(self.file_manager.output, "archive")
        file_pattern = f'{archive_folder}/{date_part}_*-*-*_{fixed_part}-{url_city_id}-GYG.csv'
        matching_files = glob.glob(file_pattern)
        for file_path in matching_files:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df = df[df['uid'] == url_unique_identifier]
                if len(df) == 0:
                    return False
                else:
                    return True
        return False
