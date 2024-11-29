import os
import sys
# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from urllib.parse import urlparse
from file_management.file_path_manager import FilePathManager
from logger.logger_manager import LoggerManager

class AzureBlobPriceComparator:
    def __init__(self, file_manager, logger):
        self.file_manager = file_manager
        self.logger = logger
        self.blob_name = self.file_manager.get_file_paths()['blob_name']
        self.file_path_output = self.file_manager.get_file_paths()['file_path_output']  # OUTPUT FILE PATH FOR DAILY UPDATES

        # Get storage account name and key from file_manager
        self.storage_account_name = self.file_manager.get_file_paths()['storage_account_name']
        self.storage_account_key = self.file_manager.get_file_paths()['storage_account_key']

        # Get container name
        self.container_name = self.file_manager.get_file_paths()['container_name_refined']

        self.connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={self.storage_account_name};"
            f"AccountKey={self.storage_account_key};"
            f"EndpointSuffix=core.windows.net"
        )

        # Create a BlobServiceClient
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

        self.logger.logger_info.info("Successfully initiated AzureBlobPriceComparator")

        # Initialize DataFrames
        self.today_df = None
        self.yesterday_df = None

        # Load Excel files once
        self.load_excel_files()

    def download_blob(self, blob_name):
        # Get the container client
        container_client = self.blob_service_client.get_container_client(self.container_name)

        # Download the blob
        blob_client = container_client.get_blob_client(blob_name)
        download_file_path = os.path.join(os.getcwd(), blob_name)

        try:
            with open(download_file_path, "wb") as download_file:
                download_stream = blob_client.download_blob()
                download_file.write(download_stream.readall())
            self.logger.logger_info.info(f"Downloaded blob: {blob_name}")
            return download_file_path
        except Exception as e:
            self.logger.logger_err.error(f"Error downloading blob {blob_name}: {e}")
            return None

    def load_excel_files(self):
        # Get today's date and yesterday's date
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        # Prepare blob names
        today_blob_name = self.blob_name
        array_today_blob_name = self.blob_name.split(' ')
        yesterday_blob_name = f"{array_today_blob_name[0]} - {yesterday.strftime('%Y-%m-%d')}.xlsx"

        self.logger.logger_info.info(f"Downloading today's blob: {today_blob_name}")
        self.logger.logger_info.info(f"Downloading yesterday's blob: {yesterday_blob_name}")

        today_file_path = self.download_blob(today_blob_name)
        yesterday_file_path = self.download_blob(yesterday_blob_name)

        if not today_file_path or not yesterday_file_path:
            self.logger.logger_err.error("Could not download one or both blobs. Exiting.")
            return

        try:
            # Read the files into DataFrames, excluding 'DONE' sheet
            today_excel = pd.read_excel(today_file_path, sheet_name=None)
            yesterday_excel = pd.read_excel(yesterday_file_path, sheet_name=None)

            # Exclude 'DONE' sheet and concatenate
            today_dfs = [df for name, df in today_excel.items() if name != 'DONE']
            yesterday_dfs = [df for name, df in yesterday_excel.items() if name != 'DONE']

            # Concatenate all sheets into one DataFrame
            self.today_df = pd.concat(today_dfs, ignore_index=True)
            self.yesterday_df = pd.concat(yesterday_dfs, ignore_index=True)

            # Drop duplicates based on 'Tytul URL'
            self.today_df = self.today_df.drop_duplicates(subset=['Tytul URL'])
            self.yesterday_df = self.yesterday_df.drop_duplicates(subset=['Tytul URL'])
            # Merge the data on 'Tytul URL'
            self.merged_df = pd.merge(
                self.today_df, self.yesterday_df, on='Tytul URL', suffixes=('_today', '_yesterday')
            )
            self.logger.logger_info.info("Successfully loaded and processed Excel files.")

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while loading Excel files: {e}")

        finally:
            # Clean up downloaded files
            if os.path.exists(today_file_path):
                os.remove(today_file_path)
            if os.path.exists(yesterday_file_path):
                os.remove(yesterday_file_path)

    def compare_prices(self, url, site):
        if self.today_df is None or self.yesterday_df is None:
            self.logger.logger_err.error("DataFrames not loaded. Cannot compare prices.")
            return 'error', 'DataFrames not loaded.'

        try:
            # Filter the DataFrames for the given URL
            merged_df = self.merged_df[self.merged_df['Tytul URL'] == url]

            if merged_df.empty:
                self.logger.logger_info.info(f"No data found for URL: {url}")
                return 'no_data', f"No data found for URL: {url}"

            # Ensure price columns are numeric
            merged_df['Cena_today'] = pd.to_numeric(merged_df['Cena_today'], errors='coerce')
            merged_df['Cena_yesterday'] = pd.to_numeric(merged_df['Cena_yesterday'], errors='coerce')

            # Compare the prices
            merged_df['Price_Changed'] = merged_df['Cena_today'] != merged_df['Cena_yesterday']

            # Get the products with price changes
            price_changes = merged_df[merged_df['Price_Changed']]
            price_threshold = 0.03  # 3% threshold for price difference

            if not price_changes.empty:
                self.logger.logger_info.info("Price change detected for the given URL:")
                for _, row in price_changes.iterrows():
                    product = row['Tytul_today']
                    product_url = row['Tytul URL']
                    price_yesterday = row['Cena_yesterday']
                    price_today = row['Cena_today']
                    # Handle date parsing
                    date_today_raw = row['Data zestawienia_today']
                    if isinstance(date_today_raw, datetime):
                        date_today = date_today_raw.strftime('%Y-%m-%d')
                    elif isinstance(date_today_raw, str):
                        try:
                            date_today = datetime.strptime(date_today_raw, '%Y-%m-%d').strftime('%Y-%m-%d')
                        except ValueError:
                            date_today = date_today_raw  # Use as is
                    else:
                        date_today = str(date_today_raw)

                    message = f"Product: {product}, Price Yesterday: {price_yesterday}, Price Today: {price_today}"
                    self.logger.logger_info.info(message)

                    if site == 'Viator':
                        # Check if price change is bigger than threshold
                        if price_yesterday == 0 or pd.isna(price_yesterday):
                            self.logger.logger_err.error("Price yesterday is zero or NaN, cannot compute percentage change.")
                            continue
                        price_change_percent = abs(price_today - price_yesterday) / price_yesterday
                        if price_change_percent > price_threshold:
                            # Price change is significant; return the data
                            data = {
                                'message': message,
                                'product': product,
                                'product_url': product_url,
                                'price_yesterday': price_yesterday,
                                'price_today': price_today,
                                'date_today': date_today
                            }
                            return 'success', data
                        else:
                            message = "No significant price changes detected for the given URL."
                            return 'no_change', message
                    else:
                        # For other sites, return the data regardless of the threshold
                        data = {
                            'message': message,
                            'product': product,
                            'product_url': product_url,
                            'price_yesterday': price_yesterday,
                            'price_today': price_today,
                            'date_today': date_today
                        }
                        return 'success', data

                # After processing all price changes
                message = "No significant price changes detected for the given URL."
                self.logger.logger_info.info(message)
                return 'no_change', message
            else:
                message = "No price changes detected for the given URL."
                self.logger.logger_info.info(message)
                return 'no_change', message

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while comparing prices: {e}")
            return 'error', f"An error occurred while comparing prices: {e}"


        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while comparing prices: {e}")

# %%
# url = 'https://www.getyourguide.com/athens-l91/acropolis-skip-the-line-walking-tour-t54919/'

# file_manager = FilePathManager('GYG', "NA")
# logger = LoggerManager(file_manager)
# comparator = AzureBlobPriceComparator(file_manager, logger)
# comparator.compare_prices(url)