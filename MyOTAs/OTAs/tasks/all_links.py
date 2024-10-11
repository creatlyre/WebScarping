import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from file_management.file_path_manager import FilePathManager
from scrapers.scraper_base import ScraperBase
from logger.logger_manager import LoggerManager

sites = ['GYG', 'Viator', 'Musement', 'Headout']
for site in sites:
    file_manager = FilePathManager(site, 'N/A')
    logger = LoggerManager(file_manager, "AllLinks")
    scraper = ScraperBase("N/A", "N/A", {}, file_manager, logger)
    file_path_xlsx_operator = file_manager.get_file_paths()['file_path_xlsx_operator']
    file_path_output = file_manager.get_file_paths()['file_path_output']
    logger.logger_info.info(file_path_xlsx_operator)
    logger.logger_info.info(file_path_output)
    logger.logger_info.info(file_path_output)
    scraper.all_links_excelfile(file_path_output, file_path_xlsx_operator)