import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from file_management.file_path_manager_future_price import FilePathManagerFuturePrice
from file_management.config_manager_future_price import ConfigReader
from logger.logger_manager_future_price import LoggerManagerFuturePrice
from notifications.email_sender_alerts import EmailSenderFuturePriceVerification
import pandas as pd
from datetime import datetime

SITES = ['GYG', 'Viator', 'Other']

def save_products_due_to_csv(products_due):
    """ Saves the products due to a CSV file """
    if not products_due:
        return
    
    # Create a DataFrame
    df = pd.DataFrame(products_due)

    # Define the file name dynamically
    timestamp = datetime.now().strftime('%Y-%m-%d')
    path = fr'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Baza Excel\Resource\future_price_not_done'
    file_name = fr"{path}\verify_future_price_not_done_{timestamp}.csv"
    
    # Save the file in the same directory or a specific folder
    output_dir = os.path.join(current_dir, "output_files")
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    file_path = os.path.join(output_dir, file_name)

    df.to_csv(file_path, index=False)
    print(f"File saved: {file_path}")

def main():
    
    file_manager = FilePathManagerFuturePrice('Verify Future Price Collection', 'N/A', 'N/A', 'N/A')  
    logger = LoggerManagerFuturePrice(file_manager=file_manager, application="verify_future_price_run")
    # Initialize ConfigReader
    email_sender = EmailSenderFuturePriceVerification(["wojbal3@gmail.com", "office@myotas.com"], logger)
    
    config_reader = ConfigReader(file_manager.config_file_path)

    products_due = []
    for SITE in SITES:
        urls = config_reader.get_urls_by_ota(SITE)
        for item in urls:
            url = item['url']
            viewer = item['viewer']
            city = item['city']
            for cfg in item['configurations']:
                adults = cfg['adults']
                language = cfg['language']
                schedules = cfg['schedules']

                for schedule in schedules:
                    is_due = config_reader.is_schedule_due(schedule=schedule)
                    if is_due:
                        next_run_due = schedule['next_run']
                        last_run_due = schedule['last_run']
                        frequency_type_due = schedule['frequency_type']
                        days_in_future_due = schedule['days_in_future']
                        # Pack multiple values into a single dictionary
                        product_due = {
                            'url': url,
                            'viewer': viewer,
                            'adults': adults,
                            'language': language,
                            'next_run_due': next_run_due,
                            'last_run_due': last_run_due,
                            'frequency_type_due': frequency_type_due,
                            'days_in_future_due': days_in_future_due
                        }
                        
                        # Append the dictionary as a single argument
                        products_due.append(product_due)

    if products_due:
        save_products_due_to_csv(products_due)
        email_sender.send_email(products_due)
if __name__ == "__main__":
    main()