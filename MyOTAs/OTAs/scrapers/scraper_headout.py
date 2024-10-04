from selenium.webdriver.common.by import By
import time
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from scrapers.scraper_base import ScraperBase

class ScraperHeadout(ScraperBase):
    def __init__(self, url, city, css_selectors, file_manager, logger):
        super().__init__(url, city, css_selectors, file_manager, logger)

    def select_currency(self):
        currency_button = self.driver.find_element(By.CSS_SELECTOR, self.css_currency)
        if "EUR" not in currency_button.get_attribute('innerHTML'):
            currency_button.click()
            currency_list = self.driver.find_elements(
                By.CSS_SELECTOR, self.css_currency_list
            )
            for currency in currency_list:
                if 'EUR' in currency.get_attribute('innerHTML'):
                    currency.click()
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

    def load_all_products(self, products_count, scroll_attempts=5, scroll_step=200):
        self.driver.get(f"{self.url}?limit={products_count}")
        time.sleep(3)

        total_height = self.driver.execute_script(
            "return document.body.scrollHeight"
        ) * 0.9
        target_scroll_increment = total_height / scroll_attempts
        current_scroll_position = 0

        for _ in range(scroll_attempts):
            target_scroll_position = current_scroll_position + target_scroll_increment

            while current_scroll_position < target_scroll_position:
                self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                current_scroll_position += scroll_step
                time.sleep(0.01)  # Fast scrolling

            time.sleep(1)  # Allow content to load
            new_height = self.driver.execute_script(
                "return document.body.scrollHeight"
            )
            if current_scroll_position + self.driver.execute_script(
                "return window.innerHeight"
            ) >= new_height:
                break

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

        product_ratings = product.find_element(
            By.CSS_SELECTOR, self.css_ratings
        ).text

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
