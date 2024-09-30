# common_functions.py

import os
import datetime
import logging
import traceback
import pandas as pd
import shutil
from azure.storage.blob import BlobServiceClient


class FilePathManager:
    """
    Manages file paths and configurations for different sites and cities.
    """

    def __init__(self, site, city):
        """
        Initializes the FilePathManager with site and city information.

        Args:
            site (str): Name of the site (e.g., 'GYG', 'Headout', 'Musment').
            city (str): Name of the city.
        """
        self.site = site
        self.city = city
        self.date_today = datetime.date.today().strftime("%Y-%m-%d")

        # Define the file paths
        base_dir = 'path_to_your_base_directory'  # Update this to your base directory
        self.output = os.path.join(base_dir, f'{self.site}/Daily')
        self.archive_folder = os.path.join(self.output, 'Archive')
        self.file_path_done = os.path.join(self.output, f'{self.date_today}-DONE-{self.site}.csv')
        self.file_path_done_city = os.path.join(self.output, f'{self.date_today}-{self.city}-{self.site}.csv')
        self.file_path_output = os.path.join(self.output, f'{self.site} - {self.date_today}.xlsx')
        self.link_file = os.path.join(base_dir, f'Resource/{self.site}_links.csv')
        self.logs_path = os.path.join(base_dir, f'Logs/{self.site}')
        self.storage_account_name = ""
        self.storage_account_key = ""
        self.local_file_path = self.file_path_output
        self.container_name_raw = f"raw/daily/{self.site}"
        self.container_name_refined = f"refined/daily/{self.site}"
        self.blob_name = f'{self.site} - {self.date_today}.xlsx'
        self.file_path_logs_processed = os.path.join(
            base_dir, f'Logs/files_processed/{self.blob_name.split(".")[0]}')

    def get_file_paths(self):
        """
        Returns a dictionary of file paths.
        """
        return {
            'output': self.output,
            'archive_folder': self.archive_folder,
            'file_path_done': self.file_path_done,
            'file_path_done_city': self.file_path_done_city,
            'file_path_output': self.file_path_output,
            'link_file': self.link_file,
            'logs_path': self.logs_path,
            'local_file_path': self.local_file_path,
            'container_name_raw': self.container_name_raw,
            'container_name_refined': self.container_name_refined,
            'blob_name': self.blob_name,
            'file_path_logs_processed': self.file_path_logs_processed,
            "storage_account_name": self.storage_account_name,
            "storage_account_key": self.storage_account_key,
            "date_today": self.date_today,
        }


class LoggerManager:
    """
    Manages logging configurations and loggers.
    """

    def __init__(self, file_manager: FilePathManager):
        """
        Initializes the LoggerManager with file paths from FilePathManager.

        Args:
            file_manager (FilePathManager): An instance of FilePathManager.
        """
        self.logs_path = file_manager.logs_path
        self.ensure_log_folder_exists()  # Ensure log folder exists

        # Create logger objects for error, info, and done logs
        self.logger_err = logging.getLogger(f'{file_manager.site}_Error_logger')
        self.logger_err.setLevel(logging.DEBUG)

        self.logger_info = logging.getLogger(f'{file_manager.site}_Info_logger')
        self.logger_info.setLevel(logging.DEBUG)

        self.logger_done = logging.getLogger(f'{file_manager.site}_Done_logger')
        self.logger_done.setLevel(logging.DEBUG)

        # Create handlers
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.DEBUG)

        self.fh_error = logging.FileHandler(os.path.join(self.logs_path, 'error_logs.log'))
        self.fh_error.setLevel(logging.DEBUG)

        self.fh_info = logging.FileHandler(os.path.join(self.logs_path, 'info_logs.log'))
        self.fh_info.setLevel(logging.INFO)

        self.fh_done = logging.FileHandler(os.path.join(self.logs_path, 'done_logs.log'))
        self.fh_done.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Add formatter to handlers
        self.ch.setFormatter(formatter)
        self.fh_error.setFormatter(formatter)
        self.fh_info.setFormatter(formatter)
        self.fh_done.setFormatter(formatter)

        # Add handlers to loggers
        self.logger_err.addHandler(self.ch)
        self.logger_err.addHandler(self.fh_error)

        self.logger_info.addHandler(self.ch)
        self.logger_info.addHandler(self.fh_info)

        self.logger_done.addHandler(self.ch)
        self.logger_done.addHandler(self.fh_done)

    def ensure_log_folder_exists(self):
        """
        Ensures that the log folder exists.
        """
        if not os.path.exists(self.logs_path):
            os.makedirs(self.logs_path)


class AzureBlobUploader:
    """
    Handles uploading files to Azure Blob Storage.
    """

    def __init__(self, file_manager: FilePathManager, logger: LoggerManager):
        """
        Initializes the AzureBlobUploader with file paths and logger.

        Args:
            file_manager (FilePathManager): An instance of FilePathManager.
            logger (LoggerManager): An instance of LoggerManager.
        """
        self.file_manager = file_manager
        self.storage_account_name = self.file_manager.get_file_paths()['storage_account_name']
        self.storage_account_key = self.file_manager.get_file_paths()['storage_account_key']
        self.container_name_raw = self.file_manager.get_file_paths()['container_name_raw']
        self.container_name_refined = self.file_manager.get_file_paths()['container_name_refined']
        self.blob_name = self.file_manager.get_file_paths()['blob_name']
        self.file_path_output = self.file_manager.get_file_paths()['file_path_output']
        self.logger = logger
        self.connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={self.storage_account_name};"
            f"AccountKey={self.storage_account_key};"
            f"EndpointSuffix=core.windows.net"
        )

        self.logger.logger_info.info("Successfully initiated AzureBlobUploader")

    def upload_excel_to_azure_storage_account(self):
        """
        Uploads the Excel file to Azure Blob Storage under the "raw" container.
        """
        try:
            # Create a BlobServiceClient object using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

            # Get a reference to the container
            container_client = blob_service_client.get_container_client(self.container_name_raw)

            # Upload the file to Azure Blob Storage
            with open(self.file_path_output, "rb") as file:
                container_client.upload_blob(name=self.blob_name, data=file)

            self.logger.logger_done.info("File uploaded successfully to Azure Blob Storage (raw).")

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while uploading to raw storage: {e}")

    def transform_upload_to_refined(self):
        """
        Transforms and uploads the Excel file to Azure Blob Storage under the "refined" container.
        """
        exclude_sheets = ['Sheet1', 'Data', 'Re-Run', 'DONE']
        output_file_path = "temp_file.xlsx"  # Temporary file for transformation

        try:
            # Read the Excel file into a Pandas DataFrame
            excel_data = pd.read_excel(self.file_path_output, sheet_name=None)

            # Write the transformed data to a new Excel file
            with pd.ExcelWriter(output_file_path) as writer:
                for sheet_name, df in excel_data.items():
                    if sheet_name in exclude_sheets:
                        continue
                    # Transform the DataFrame (add your transformation logic here)
                    df['Data zestawienia'] = df['Data zestawienia'].astype('str')
                    df['IloscOpini'] = df['IloscOpini'].fillna(0)
                    df['IloscOpini'] = df['IloscOpini'].astype(str).str.replace('(', '').str.replace(')', '')
                    df['IloscOpini'] = df['IloscOpini'].apply(
                        lambda x: int(float(x.replace('K', '')) * 1000) if 'K' in x else x)
                    df['Przecena'] = df['Przecena'].apply(
                        lambda x: str(x).replace('€', '').replace('$', '').replace('£', '').strip())
                    df['Cena'] = df['Cena'].apply(
                        lambda x: str(x).replace('€', '').replace('$', '').replace('£', '').strip())
                    df['Opinia'] = df['Opinia'].fillna('N/A')
                    df = df[df['Tytul'] != 'Tytul']
                    df = df[df['Data zestawienia'] != 'Data zestawienia']
                    df = df[df['Data zestawienia'].str.len() > 4]
                    df['Cena'] = df['Cena'].map(
                        lambda x: x.split('from')[-1] if 'from' in x else x)
                    df['Przecena'] = df['Przecena'].map(
                        lambda x: x.split('per person')[0] if 'per person' in x.lower() else x)
                    df['Opinia'] = df['Opinia'].map(
                        lambda x: x.replace("NEW", '') if isinstance(x, str) else x)

                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Upload the transformed Excel file to Azure Blob Storage
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            container_client = blob_service_client.get_container_client(self.container_name_refined)

            with open(output_file_path, "rb") as data:
                container_client.upload_blob(name=self.blob_name, data=data)

            self.logger.logger_done.info("File uploaded successfully to Azure Blob Storage (refined).")

        except Exception as e:
            self.logger.logger_err.error(
                f"An error occurred while transforming and uploading to refined storage: {e}")

        finally:
            # Clean up the temporary file
            if os.path.exists(output_file_path):
                os.remove(output_file_path)
