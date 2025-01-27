# %%
API_KEY = "56ed5b7f827aa5c258b3f6d3f57d36999aa949e8"
import pandas as pd
import os
import sys
import threading
from queue import Queue

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_tripadvisor import TripadvisorScraper
from file_management.file_path_manager import FilePathManager
from logger.logger_manager import LoggerManager

def main():
    # Initialize necessary components
    site = "Tripadvisor"
    file_manager = FilePathManager(site, 'N/A')
    logger = LoggerManager(file_manager, application="getoperator")
    css_selectors = {
        'supplier_section': 'div[class*="qyzqH f k w"]',
        'supplier': 'div[class*="biGQs _P"]',
    }
    file_path_xlsx_operator = file_manager.get_file_paths()['file_path_xlsx_operator']

    df = pd.read_excel(file_path_xlsx_operator)
    logger.logger_info.info(f"There are {len(df[df['Operator'] == 'ToDo'])}")
    df_todo = df[df['Operator'] == 'ToDo']
    logger.logger_info.info(f"Processing {len(df_todo)} rows.")

    scraper = TripadvisorScraper(api_key=API_KEY, file_manager=file_manager, date_today="N/A", css_selectors=css_selectors)

    # Define a thread-safe queue
    queue = Queue()

    # Thread function
    def process_row(index, row, worker_id):
        url = row['Link']
        try:
            logger.logger_info.info(f"Worker{worker_id}: Processing row {index} with URL: {url}")
            provider_name = scraper.all_links_get_provider_name(url)
            provider_name = provider_name.split('By')[-1].strip()
            logger.logger_done.info(f"Worker{worker_id}: Provider name fetched for row {index}: {provider_name}")
            df.at[index, 'Operator'] = provider_name
        except Exception as e:
            logger.logger_err.error(f"Worker{worker_id}: Error processing row {index} with URL {url}: {str(e)}")
            df.at[index, 'Operator'] = "NotFound"

    # Worker function for threads
    def worker(worker_id):
        while not queue.empty():
            index, row = queue.get()
            process_row(index, row, worker_id)
            queue.task_done()

            # Periodically save progress
            if queue.qsize() % 51 == 0:
                logger.logger_info.info(f"Worker{worker_id}: Saving progress. Remaining rows in queue: {queue.qsize()}")
                scraper.all_links_save_dataframe(df, file_path_xlsx_operator)

    # Add rows to the queue
    for index, row in df_todo.iterrows():
        queue.put((index, row))

    # Start threads
    num_threads = 16  # Adjust based on your system's capacity and the nature of the task
    threads = []
    for worker_id in range(1, num_threads + 1):
        thread = threading.Thread(target=worker, args=(worker_id,))
        thread.start()
        threads.append(thread)


    # Wait for all threads to complete
    queue.join()

    # Save the final DataFrame
    scraper.all_links_save_dataframe(df, file_path_xlsx_operator)
    logger.logger_done.info(f"Finished processing all rows.")

    # Ensure threads finish execution
    for thread in threads:
        thread.join()

# Entry point
if __name__ == "__main__":
    main()
