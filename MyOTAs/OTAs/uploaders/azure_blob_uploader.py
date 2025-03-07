import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
import numpy as np

class AzureBlobUploader:
    def __init__(self, file_manager, logger):
        self.file_manager = file_manager
        self.storage_account_name = self.file_manager.get_file_paths()['storage_account_name']
        self.storage_account_key = self.file_manager.get_file_paths()['storage_account_key']
        self.container_name_raw = self.file_manager.get_file_paths()['container_name_raw']
        self.container_name_refined = self.file_manager.get_file_paths()['container_name_refined']
        self.blob_name = self.file_manager.get_file_paths()['blob_name']
        self.file_path_output = self.file_manager.get_file_paths()['file_path_output'] # OUTPUT FILE PATH FOR DAILY UDPATES
        self.logger = logger
        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key};EndpointSuffix=core.windows.net"

        self.logger.logger_info.info("Sucessfuly initiated AzureBlobUploader")

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
    def is_valid_date(self, date_str):
        try:
            pd.to_datetime(date_str, format='%Y-%m-%d', errors='raise')
            return True
        except ValueError:
            return False

    def transform_upload_to_refined(self):
        """
        Transforms and uploads the Excel file to Azure Blob Storage under the "refined" container.
        """
        exclude_sheets = ['Sheet1', 'Data', 'Re-Run', 'DONE']
        output_file_path = f"{self.file_manager.site}_temp_file.xlsx"  # Temporary file for transformation

        try:
            # Read the Excel file into a Pandas DataFrame
            excel_data = pd.read_excel(self.file_path_output, sheet_name=None)

            # Write the transformed data to a new Excel file
            with pd.ExcelWriter(output_file_path) as writer:
                for sheet_name, df in excel_data.items():
                    if sheet_name in exclude_sheets:
                        continue

                     # Check 'Data zestawienia' for valid date formats
                    df['Data zestawienia'] = df['Data zestawienia'].astype(str)
                    
                    # Filter rows where 'Data zestawienia' does not have a valid date
                    invalid_rows = df[~df['Data zestawienia'].apply(self.is_valid_date)]
                    
                    # Log sheet name and number of invalid rows if found
                    if not invalid_rows.empty:
                        self.logger.logger_err.error(f"Sheet {sheet_name} has {len(invalid_rows)} invalid date entries in 'Data zestawienia' column.")
                        continue

                    # Convert 'Data zestawienia' to YYYY-MM-DD format if valid
                    df['Data zestawienia'] = pd.to_datetime(df['Data zestawienia']).dt.strftime('%Y-%m-%d')

                    # Transform the DataFrame (add your transformation logic here)
                    df['Data zestawienia'] = df['Data zestawienia'].astype('str')
                    df['IloscOpini'] = df['IloscOpini'].astype(str)
                    df['IloscOpini'] = df['IloscOpini'].fillna(0)
                    df['IloscOpini'] = df['IloscOpini'].str.replace('(', '').str.replace(')','').str.replace(',', '').str.replace('s','').str.replace('review','').str.strip()
                    df['IloscOpini'] = df['IloscOpini'].apply(lambda x: int(float(x.replace('K', '')) * 1000) if isinstance(x, str) and 'K' in x else x)
                    df['Opinia'] = df['Opinia'].astype(str)
                    df['Opinia'] = df['Opinia'].fillna('N/A')
                    df['Opinia'] = df['Opinia'].map(lambda x: x.replace("NEW", '') if isinstance(x, str) else x)

                    df = df[df['Tytul'] != 'Tytul']
                    df = df[df['Data zestawienia'] != 'Data zestawienia']
                    df = df[df['Data zestawienia'].str.len() > 4]

                    df['Cena'] = df['Cena'].str.lower()
                    df['Cena'] = df['Cena'].map(lambda x: x.split('from')[-1] if isinstance(x, str) and 'from' in x else x)
                    df['Cena'] = df['Cena'].apply(lambda x: str(x).replace('€', '').replace('$', '').replace('£', '').strip() if isinstance(x, str) else x)
                    df['Cena'] = df['Cena'].map(lambda x: x.split('per person')[0] if isinstance(x, str) and 'per person' in x.lower() else x)
                    df['Cena'] = df['Cena'].map(lambda x: x.split('per group')[0] if isinstance(x, str) and 'per group' in x.lower() else x)

                    df['Przecena'] = df['Przecena'].apply(lambda x: str(x).replace('€', '').replace('$', '').replace('£', '').strip() if isinstance(x, str) else x)
                    df['Przecena'] = df['Przecena'].map(lambda x: x.split('per person')[0] if isinstance(x, str) and 'per person' in x.lower() else x)
                    df['Przecena'] = df['Przecena'].map(lambda x: x.split('per group')[0] if isinstance(x, str) and 'per group' in x.lower() else x)


                    # Apply str.replace only if the value is a string

                    if self.file_manager.site == 'Tripadvisor':
                        df['Opinia'] = df['Opinia'].str.extract(r'(\d\.\d)')
                        df['Opinia'].astype(float)
                        df['Tytul'] = df['Tytul'].str.replace(r'^\d+\.\s*', '', regex=True)
                        df['IloscOpini'] = df['IloscOpini'].map(lambda x: x.replace(",", '') if isinstance(x, str) else x)


                    df.to_excel(writer, sheet_name=sheet_name, index=False)
    # Upload the transformed Excel file to Azure Blob Storage
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            container_client = blob_service_client.get_container_client(self.container_name_refined)

            with open(output_file_path, "rb") as data:
                container_client.upload_blob(name=self.blob_name, data=data)
            
            self.logger.logger_done.info("File uploaded successfully to Azure Blob Storage (refined).")

        except Exception as e:
            print(sheet_name)
            self.logger.logger_err.error(f"An error occurred while transforming and uploading to refined storage: {e}")
            
        finally:
            # Clean up the temporary file
            if os.path.exists(output_file_path):
                os.remove(output_file_path)


    def upload_excel_to_azure_storage_account_future_price(self, future_price_file_path, future_price_blob_name):
        """
        Uploads the Excel file to Azure Blob Storage under the "raw" container.
        """
        try:
            # Create a BlobServiceClient object using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

            # Get a reference to the container
            container_client = blob_service_client.get_container_client(self.container_name_raw)

            # Upload the file to Azure Blob Storage
            with open(future_price_file_path, "rb") as file:
                container_client.upload_blob(name=future_price_blob_name, data=file)
            
            self.logger.logger_done.info("File uploaded successfully to Azure Blob Storage (raw).")

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while uploading to raw storage: {e}")

    def transform_upload_to_refined_future_price(self, future_price_file_path, future_price_blob_name):
        """
        Transforms and uploads the Excel file to Azure Blob Storage under the "refined" container.
        """
        self.logger.logger_info.info(f'Processing file: {future_price_file_path} to refined layer as {future_price_blob_name}')
        output_file_path = "temp_file.xlsx"  # Temporary file for transformation

        try:
            # Write the transformed data to a new Excel file
            df = pd.read_excel(future_price_file_path)
            # city replacment if there are incorrect in url
            fill_values = {
                'tour_option': 'Option unavailable',
                'time_range': 'Option unavailable'
            }
            # Make changes to the df DataFrame as needed
            df['extraction_date'] = df['extraction_date'].astype('str')
            df['date'] = df['date'].astype('str')
            df['price_per_person'] = df['price_per_person'].astype('str')
            df['price_per_person'] = df['price_per_person'].map(lambda x: x.split(' ')[-1] if x != 'Price unavailable' else None)
            df['price_per_person'] = df['price_per_person'].replace(r'[$€£]', '', regex=True).str.replace(',', '').str.strip()
            df['price_per_person'] = pd.to_numeric(df['price_per_person'], errors='coerce')
            df['total_price'] = df.apply(lambda row: float(row['price_per_person']) * int(row['adults']) if row['availability'] != False else None, axis=1)

            # df['price_per_person'] = df['price_per_person'].replace('Price unavailable', None)
            # df['total_price'] = df['total_price'].replace('Price unavailable', None)
            # Fill empty or NaN cells
            df = df.fillna(fill_values)
            
            # Save modified DataFrame to an Excel file temporarily
            df.to_excel(output_file_path, index=False)
            # Create a connection to Azure Blob Storage

    # Upload the transformed Excel file to Azure Blob Storage
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            container_client = blob_service_client.get_container_client(self.container_name_refined)

            with open(output_file_path, "rb") as data:
                container_client.upload_blob(name=future_price_blob_name, data=data)

            self.logger.logger_done.info(f"File uploaded successfully to Azure Blob Storage (refined).")
            self.logger.logger_done.info(f"File: {future_price_file_path} uploaded as: {future_price_blob_name}")
            

        except Exception as e:
            self.logger.logger_err.error(f"An error occurred while transforming and uploading to refined storage: {e}")
            
        finally:
            # Clean up the temporary file
            if os.path.exists(output_file_path):
                os.remove(output_file_path)