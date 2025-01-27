import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from file_management.file_path_manager import FilePathManager, ConnectorsSQL_OTA
from uploaders.sql_database_uploader import SQLTableUpload
from logger.logger_manager import LoggerManager


sql_connectors = ConnectorsSQL_OTA()
sites = ['Tripadvisor', "GYG", "Musement", "Viator"]
for site in sites:
    file_manager = FilePathManager(site, 'N/A')
    logger = LoggerManager(file_manager, 'operator_update')
    table_upload = SQLTableUpload(sql_connectors.USERNAME, sql_connectors.PASSWORD, logger)
    file_path_xlsx_operator = file_manager.get_file_paths()['file_path_xlsx_operator']
    table_upload.upsert_df_to_sql_db(file_path_xlsx_operator, 'OTAs')
    if site == "GYG":
        table_upload.upsert_df_to_sql_db(file_path_xlsx_operator, 'db_ota_future_price')
