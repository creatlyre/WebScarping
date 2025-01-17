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

SITES = ['GYG', 'Viator', 'Other']
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
        email_sender.send_email(products_due)
if __name__ == "__main__":
    main()