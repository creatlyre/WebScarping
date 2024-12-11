import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from file_management.file_path_manager import ConnectorsSQL_OTA
from reports.historical_report_generator import HistoricalReportGenerator
from notifications.email_sender_alerts import EmailSenderAlerts
from logger.logger_manager import LoggerManager
from file_management.file_path_manager import FilePathManager

file_manager = ConnectorsSQL_OTA()
file_manager_logger = FilePathManager("TEST", "NA")
logger = LoggerManager(file_manager_logger, f'PDF_reports')

historical_review = HistoricalReportGenerator(file_manager.USERNAME, file_manager.PASSWORD)


url = input("URL Input:")
historical_review.run_report(url, date_filter='last_year')

# Convert the overview to an HTML-compatible string
overview_html = "<br>".join(historical_review.overview)

# date filter option: None, previous_month previous_week previous_quarter last_week, last year,
if historical_review.output_filename:
    email_sender = EmailSenderAlerts("wojbal3@gmail.com", "Test_123", url, "2024-12-11", "N/A", "N/A", logger)
    email_sender.send_report_email_with_attachment(historical_review.output_filename, overview_html) 