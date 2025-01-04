import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
import pandas as pd
from file_management.file_path_manager import ConnectorsSQL_OTA
from reports.historical_report_generator import HistoricalReportGenerator
from notifications.email_sender_alerts import EmailSenderAlerts
from logger.logger_manager import LoggerManager
from file_management.file_path_manager import FilePathManager

def load_csv(file_path):
    try:
        # Assuming the CSV has a column named 'URL'
        df = pd.read_csv(file_path)
        if 'URL' not in df.columns:
            raise ValueError("The CSV file must have a 'URL' column.")
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None
file_path_config = r'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Baza Excel\Resource\new_year_reports_urls.csv'
df = load_csv(file_path_config)
df = df[df['Status'] != 'Done']

for index, row in df.iterrows():
    url = row['URL']
    viewer = row['Viewer']
    city = row['City']
    if city:
        print(f"{city} City")
    print(f"Processing URL: {url}")
    file_manager = ConnectorsSQL_OTA()
    file_manager_logger = FilePathManager("TEST", "NA")
    logger = LoggerManager(file_manager_logger, f'PDF_reports')

    historical_review = HistoricalReportGenerator(file_manager.USERNAME, file_manager.PASSWORD)


    # url = input("URL Input:")
     
    historical_review.run_report(url, viewer=viewer, date_filter='last_year')

    # Convert the overview to an HTML-compatible string
    overview_html = "<br>".join(historical_review.overview)
    df.loc[index, 'Status'] = 'Done'

    # df.to_csv(file_path_config, index=False)
    print(f"Row {index} saved to CSV.")

    # date filter option: None, previous_month previous_week previous_quarter last_week, 
    # last_year_to_date -> (today - 365days), last_year -> (entire last year)



# if historical_review.output_filename:
#     email_sender = EmailSenderAlerts("wojbal3@gmail.com", "Test_123", url, "2024-12-11", "N/A", "N/A", logger)
#     email_sender.send_report_email_with_attachment(historical_review.output_filename, overview_html) 