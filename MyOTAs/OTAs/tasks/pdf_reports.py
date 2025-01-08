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

def generate_html_summary(filename):
       # Check if the file exists
    if os.path.exists(filename):
        # Append to the file
        with open(filename, 'a', encoding='utf-8') as file:
            file.write(f"""
            <hr>
            <p>{overview_html}</p>
            """)
        print(f"Appended overview to existing file: {filename}")
    else:
        # Create a new file and write the overview with basic HTML structure
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{viewer} Overview</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 20px;
                        line-height: 1.6;
                        background-color: #f9f9f9;
                        color: #333;
                    }}
                    h1 {{
                        color: #0056b3;
                    }}
                    hr {{
                        border: 0;
                        height: 1px;
                        background: #ddd;
                        margin: 20px 0;
                    }}
                    p {{
                        background: #fff;
                        padding: 10px;
                        border-radius: 5px;
                        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                    }}
                    a {{
                        color: #007bff;
                        text-decoration: none;
                    }}
                    a:hover {{
                        text-decoration: underline;
                    }}
                </style>
            </head>
            <body>
                <h1>{viewer} Overview</h1>
                <p>{overview_html}</p>
            </body>
            </html>
            """)
        print(f"Created new HTML file and saved overview: {filename}")
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
df_to_process = df[df['Status'] != 'Done']

for index, row in df_to_process.iterrows():
    url = row['URL']
    viewer = row['Viewer']
    city = row['City']
    ota = url.split('.com')[0].split('www.')[-1]
    if ota == 'getyourguide':
        ota = 'GetYourGuide' 
    else:
        ota = ota.capitalize
    if city:
        print(f"---------------- {city} City")
    print(f"Processing URL: {url}")
    file_manager = ConnectorsSQL_OTA()
    file_manager_logger = FilePathManager("TEST", "NA")
    logger = LoggerManager(file_manager_logger, f'PDF_reports')

    historical_review = HistoricalReportGenerator(file_manager.USERNAME, file_manager.PASSWORD, city, ota)

    # url = input("URL Input:")
    
    # date filter option: None --> all data, previous_month, previous_week, previous_quarter, last_week, 
    # last_year_to_date -> (today - 365days), last_year -> (entire last year)
    
    historical_review.run_report(url, viewer=viewer, date_filter='last_year')

    # Convert the overview to an HTML-compatible string
    overview_html = "<br>".join(historical_review.overview)
    # Define the HTML filename based on the viewer
    filename = f"PDF_reports\{viewer}_summary.html"
    generate_html_summary(filename=filename)
    # Update only the current row's status
    df.at[index, 'Status'] = 'Done'

    # Save updated rows back to the CSV file
    df.to_csv(file_path_config, index=False)
    print(f"Row {index} updated and saved to CSV.")




# # if historical_review.output_filename:
    email_sender = EmailSenderAlerts("wojbal3@gmail.com", "Test_123", url, "2024-12-11", "N/A", "N/A", logger)
    email_sender.send_report_email_with_attachment(historical_review.output_filename, overview_html) 