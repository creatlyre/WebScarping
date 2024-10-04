# %%

import pandas as pd
import common_functions

# %%
site = "GYG"
file_manager = common_functions.FilePathManager(site, 'N/A')
logger = common_functions.LoggerManager(file_manager, application="getoperator")
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
        scraper = common_functions.ScraperGYG(url, "N/A", class_selectors, file_manager, logger, provider=True)
        
        # Log the initiation of the scraping process
        logger.logger_info.info(f"Initialized scraper for URL: {url}")
        scraper.get_url()
        
        provider_name = scraper.get_provider_name()
        provider_name = provider_name.text
        # Log that the provider name was successfully fetched
        logger.logger_done.info(f"Provider name fetched for row {index}: {provider_name}")
            
        df.at[index, 'Operator'] = provider_name
        if counter % 50 == 0:
            logger.logger_done.info(f"ALready process {counter} saving progress.")
            scraper.save_dataframe(df, file_path_xlsx_operator)
    except Exception as e:
        # Log any errors encountered during the scraping process
        logger.logger_err.error(f"Error processing row {index} with URL {url}: {str(e)}")
    finally:
        # Ensure that the driver is closed
        # scraper.quit_driver()
        logger.logger_done.info(f"Closed scraper for URL: {url}")

scraper.save_dataframe(df, file_path_xlsx_operator)


# %%



