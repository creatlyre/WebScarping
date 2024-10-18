# %%

import pandas as pd
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_gyg import ScraperGYG
from file_management.file_path_manager import FilePathManager, DetermineDebugRun
from logger.logger_manager import LoggerManager


# %%

# %%
site = "GYG"
file_manager = FilePathManager(site, 'N/A')
logger = LoggerManager(file_manager, application="getoperator")
#
# css_selectors = {
#     'provider': 'div[data-test-id*="activity-provider"]',
# }
class_selectors = {
    'provider': 'supplier-name__link',
}

file_path_xlsx_operator = file_manager.get_file_paths()['file_path_xlsx_operator']

# %%

df = pd.read_excel(file_path_xlsx_operator)
logger.logger_info.info(f"There are {len(df[df['Operator'] == 'ToDo'])}")
counter = 1
df_todo = df[df['Operator'] == 'ToDo']
for index, row in df_todo.iterrows():
    
    url = row['Link']
    # Log the current row being processed
    logger.logger_info.info(f"Processing row {index} with URL: {url}")

    if row['Operator'] != "ToDo":
        logger.logger_info.info(f"Skipping row {index} as the URL is not 'ToDo'.")
        continue
    try:
        scraper = ScraperGYG(url, "N/A", class_selectors, file_manager, logger, provider=True)
        
        # Log the initiation of the scraping process
        logger.logger_info.info(f"Initialized scraper for URL: {url}")
        scraper.get_url()
        
        provider_name = scraper.get_provider_name()
        provider_name = provider_name.text
        # Log that the provider name was successfully fetched
        logger.logger_done.info(f"Provider name fetched for row {index}: {provider_name}")
            
        df.at[index, 'Operator'] = provider_name

    except Exception as e:
        # Log any errors encountered during the scraping process
        logger.logger_err.error(f"Error processing row {index} with URL {url}: {str(e)}")
        df.at[index, 'Operator'] = "NotFound"
    finally:
        # Ensure that the driver is closed
        # scraper.quit_driver()
        counter += 1
        if counter % 50 == 0:
            logger.logger_done.info(f"Already process {counter} saving progress.")
            scraper.save_dataframe(df, file_path_xlsx_operator)
        logger.logger_done.info(f"Closed scraper for URL: {url}")

scraper.save_dataframe(df, file_path_xlsx_operator)


# %%
counter

# %%



