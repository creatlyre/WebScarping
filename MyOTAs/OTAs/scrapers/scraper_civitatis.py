from selenium.webdriver.common.by import By
import time
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


from scrapers.scraper_base import ScraperBase


class ScraperCivitatis(ScraperBase):
    def __init__(self, url, city, css_selectors, file_manager, logger, provider=False):
        super().__init__(url, city, css_selectors, file_manager, logger)

        # Update the css_selectors with Civitatis-specific selectors
        self.css_selectors = css_selectors
        # Assign Civitatis-specific selectors to instance variables
        self.css_view_more_button = self.css_selectors.get('view_more_button')
        self.css_cookies_banner_decline = self.css_selectors.get('cookies_banner')
        self.css_sort_by = self.css_selectors.get('sort_by')
        self.css_option_rating = self.css_selectors.get('option_rating')
        self.css_option_popularity = self.css_selectors.get('option_popularity')
        self.js_shadow_root = self.css_selectors.get('js_script_for_shadow_root')
        if provider:
            self.css_provider = self.css_selectors.get('provider')
        self.provider = provider
        self.wait = WebDriverWait(self.driver, 10)

    def select_currency(self):
        currency_button = self.driver.find_element(By.CSS_SELECTOR, self.css_currency)
        if "eur" not in currency_button.text.lower():
            currency_button.click()
            currency_list = self.driver.find_elements(
                By.CSS_SELECTOR, self.css_currency_list
            )
            for currency in currency_list:
                if 'eur' in currency.text.lower():
                    currency.click()
                    time.sleep(2)
                    break

    def get_product_count(self):
        products_count_selenium = self.driver.find_element(
            By.CSS_SELECTOR, self.css_products_count
        )
        if 'Loading' in products_count_selenium.get_attribute('innerHTML'):
            time.sleep(1.5)
        products_count_selenium = self.driver.find_element(
            By.CSS_SELECTOR, self.css_products_count
        )
        products_count = int(
            products_count_selenium.get_attribute('innerHTML').split(' ')[0]
        )
        return products_count

    def get_provider_name(self):
        provider_name = self.driver.find_element(
            By.CSS_SELECTOR, self.css_provider
        )
        return provider_name

    def load_all_products_by_button(self, products_count, scroll_step=-100):
        current_scroll_position = self.driver.execute_script(
            "return document.body.scrollHeight"
        )

        while current_scroll_position > 0:
            self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
            current_scroll_position += scroll_step
            time.sleep(0.01)

        current_count_of_products = 0

        while current_count_of_products < products_count * 0.8:
            current_count_of_products = len(
                self.driver.find_elements(By.CSS_SELECTOR, self.css_product_card)
            )
            self.logger.logger_info.info(
                f"Current count of products: {current_count_of_products} "
                f"Products count: {products_count} 80% --> {products_count*0.8}"
                f"Will finish the while loop in this iteration"
            )
            try:
                view_more_button = self.wait.until(
                    EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, self.css_view_more_button)
                    )
                )
            except:
                if current_count_of_products > 400 or current_count_of_products > products_count * 0.8:
                    self.logger.logger_info.info(f"Cound't find view more button che")
                    break

            self.driver.execute_script(
                "arguments[0].scrollIntoView(true);", view_more_button
            )
            self.driver.execute_script("arguments[0].click();", view_more_button)
            time.sleep(1.5)
            
    def navigate_to_next_page(self, page_number):
        try:
            url = f"{self.url}{page_number}"
            self.driver.get(url)
        except:
            return False
        
    def scrape_products(self, products_count, global_category=False, products_per_page=20):
        products_collected = 0
        page_number = 1
        ## Loop thorugh all pages
        self.logger.logger_info.info(f"Scraping up to products count: {products_count} and 80% is: {products_count * 0.8}")
        while products_collected < products_count * 0.8:
            products = self.driver.find_elements(By.CSS_SELECTOR, self.css_product_card)
            data = []
            position = 1
            
            for product in products:
                product_data = self.extract_product_data(
                    product, position, global_category
                )
                data.append(product_data)
                position += 1
                products_collected += 1

            page_number += 1
            self.navigate_to_next_page(page_number)

        return pd.DataFrame(
            data,
            columns=[
                'Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini',
                'Przecena', 'Data zestawienia', 'Pozycja', 'Kategoria',
                'SiteUse', 'Miasto'
            ],
        )

    def extract_product_data(
        self, product, position, global_category=False
    ):
        product_title = product.find_element(By.TAG_NAME, 'a').text
        product_url = product.find_element(By.TAG_NAME, 'a').get_attribute('href')

        try:
            product_price = product.find_element(
                By.CSS_SELECTOR, self.css_tour_price
            ).text
        except:
            product_price = "N/A"

        try:
            product_discount_price = product.find_element(
                By.CSS_SELECTOR, self.css_tour_price_discount
            ).text
            if product_discount_price == "from":
                product_discount_price = "N/A"
        except:
            product_discount_price = "N/A"

        if product_discount_price != 'N/A':
            product_discount_price, product_price = product_price, product_discount_price

        try:
            product_ratings = product.find_element(
                By.CSS_SELECTOR, self.css_ratings
            ).text.split("/")[0]
        except:
            product_ratings = "N/A"

        try:
            product_review_count = product.find_element(
                By.CSS_SELECTOR, self.css_review_count
            ).text
        except:
            product_review_count = "N/A"

        try:
            product_category = product.find_element(
                By.CSS_SELECTOR, self.css_category_label
            ).text
        except:
            product_category = "N/A"

        if global_category:
            product_category = "Global"

        return [
            product_title, product_url, product_price, product_ratings,
            product_review_count, product_discount_price, self.date_today, position,
            product_category, self.site, self.city
        ]
 