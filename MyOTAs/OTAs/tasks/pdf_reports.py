import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from file_management.file_path_manager import ConnectorsSQL_OTA
from reports.historical_report_generator import HistoricalReportGenerator

file_manager = ConnectorsSQL_OTA()

historical_review = HistoricalReportGenerator(file_manager.USERNAME, file_manager.PASSWORD)


url = input("URL Input:")
historical_review.run_report(url, date_filter=None)

# date filter option: previous_month previous_week previous_quarter last_week