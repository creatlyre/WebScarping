import os
import pandas as pd
import logging
import concurrent.futures
from bs4 import BeautifulSoup
from utils import (
    API_KEY_ZENROWS,
    ZenRowsScraper,
    setup_logger,
    output_viator_all_links
)

# --- Configuration ---
LOG_FILE = 'viator_getoperator.log'
OPERATOR_FILE_PATH = os.path.join(r"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe", "Operators_Viator.xlsx")
PROCESSED_URLS_PATH = os.path.join(output_viator_all_links, f"SupplierExtract - {pd.Timestamp.now().strftime('%Y-%m-%d')}.csv")

# Setup logging
logger = setup_logger('viator_getoperator', LOG_FILE)

# --- Main Scraper Class ---
class OperatorScraper:
    def __init__(self, api_key, operator_file_path, processed_urls_path):
        self.scraper = ZenRowsScraper(api_key)
        self.operator_file_path = operator_file_path
        self.processed_urls_path = processed_urls_path
        self.processed_urls = self._load_processed_urls()

    def _load_processed_urls(self):
        """Loads the set of already processed URLs from the output file."""
        if os.path.exists(self.processed_urls_path):
            try:
                return set(pd.read_csv(self.processed_urls_path)['UrlRequest'].unique())
            except Exception as e:
                logger.error(f"Error loading processed URLs: {e}")
                return set()
        return set()

    def _save_dataframe(self, df, file_path, is_excel=False):
        """Saves a DataFrame to either CSV or Excel."""
        try:
            if is_excel:
                df.to_excel(file_path, index=False)
            else:
                df.to_csv(file_path, index=False, mode='a', header=not os.path.exists(file_path))
            logger.info(f"Successfully saved data to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {e}")

    def _extract_supplier_name(self, html_content, url):
        """Extracts the supplier name from the HTML content."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            supplier_name_tag = soup.find(lambda tag: 'supplierName' in str(tag))
            if supplier_name_tag:
                supplier_name_text = supplier_name_tag.get_text(strip=True)
                # Further cleaning can be done here if needed
                return supplier_name_text
            else:
                logger.warning(f"Supplier name not found for URL: {url}")
                return None
        except Exception as e:
            logger.error(f"Error extracting supplier name from {url}: {e}")
            return None

    def _process_url(self, url):
        """Processes a single URL to get the supplier name."""
        params = {'js_render': 'true', 'json_response': 'true', 'premium_proxy': 'true'}
        response = self.scraper.get(url, params=params)

        if response and response.status_code == 200:
            supplier_name = self._extract_supplier_name(response.content, url)
            if supplier_name:
                return url, supplier_name
        return url, None

    def run(self):
        """Main method to run the scraper."""
        try:
            operator_df = pd.read_excel(self.operator_file_path)
        except Exception as e:
            logger.error(f"Failed to read operator file: {e}")
            return

        urls_to_process = operator_df[(operator_df['Operator'] == 'ToDo') & (~operator_df['Link'].isin(self.processed_urls))]['Link'].tolist()

        if not urls_to_process:
            logger.info("No new URLs to process.")
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(self._process_url, url): url for url in urls_to_process}
            
            for future in concurrent.futures.as_completed(future_to_url):
                url, supplier_name = future.result()
                if supplier_name:
                    operator_df.loc[operator_df['Link'] == url, 'Operator'] = supplier_name
                    logger.info(f"Successfully processed {url} and found operator: {supplier_name}")
                else:
                    logger.warning(f"Failed to process {url}")

                # Save progress incrementally
                self.processed_urls.add(url)
                processed_df = pd.DataFrame({'UrlRequest': [url]})
                self._save_dataframe(processed_df, self.processed_urls_path)

        self._save_dataframe(operator_df, self.operator_file_path, is_excel=True)
        logger.info("Scraping complete. Operator file updated.")

# --- Main Execution ---
if __name__ == "__main__":
    scraper = OperatorScraper(API_KEY_ZENROWS, OPERATOR_FILE_PATH, PROCESSED_URLS_PATH)
    scraper.run()