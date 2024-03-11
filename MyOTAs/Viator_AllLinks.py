# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import numpy as np
import datetime
from selenium.webdriver.common.action_chains import ActionChains
import xlsxwriter
from openpyxl import Workbook, load_workbook
import os
import shutil
import logging
import traceback
import re
from azure.storage.blob import BlobServiceClient
# from undetected_chromedriver import Chrome, ChromeOptions
# from user_agent import generate_user_agent
# import ctypes  # An included library with Python install.   
import random
import requests
import json
import concurrent.futures

# eyJhbGciOiJSUzI1NiIsImtpZCI6IjY3YmFiYWFiYTEwNWFkZDZiM2ZiYjlmZjNmZjVmZTNkY2E0Y2VkYTEiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiV29qdGVrIEJhbG9uIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FBY0hUdGZCODM1WVhSalRJeEl4WmxyTnBaRXpWQk9hZmUyMUFmU1dZZXNnUGc9czk2LWMiLCJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vZXhhMi1mYjE3MCIsImF1ZCI6ImV4YTItZmIxNzAiLCJhdXRoX3RpbWUiOjE2ODY2NTg5MDYsInVzZXJfaWQiOiJEcWRXRDhRdloyUTkzcTR4WFhWWlFWUk8wSEMyIiwic3ViIjoiRHFkV0Q4UXZaMlE5M3E0eFhYVlpRVlJPMEhDMiIsImlhdCI6MTY4NjY1OTA2MSwiZXhwIjoxNjg2NjYyNjYxLCJlbWFpbCI6IndvamJhbDNAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZ29vZ2xlLmNvbSI6WyIxMTUwNTc1NjgzNzI4NjQ1MzA0NTciXSwiZW1haWwiOlsid29qYmFsM0BnbWFpbC5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJnb29nbGUuY29tIn19.IAOh_U2LXNXGk1jqG3q6m9utI79QVMDtCuUcDBSH5TEKPmMCEdW962qOZN6J8wfMzexHX1cWoqGcXYBmjLcjQKBhhQoAUAdYjxEivrLHe8Hi37bIwXrEX9mvAKD1wE71Sq1sbB3B9xU51lTsH88l7P0pq9LDgbaKkJCljvvzJ186BTbX9Qw0CF4gma1XjJ1W3Nmd0BK2pE9y0b3arF_V8bSME6BeR4Ls1yKLM9da-MCN5y-IkwGVB6j78Qrt-4_emtAhxjkcYlzauOtEM8dZ0NzblgSxY-hdG_sG-Clg0gM6fxXRQSQJYjqHNgwY7sjAP885JUWbtjWjoXKvdJn_iA

# %%
date_today = datetime.date.today().strftime("%Y-%m-%d")
# date_today = '2023-10-19'
date_yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
output_viator = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Viator/All Links'
file_path_done =fr'{output_viator}/{date_today}-DONE-Viator.csv'  
archive_folder = fr'{output_viator}/Archive'

file_path_done_archive =fr'{archive_folder}/{date_yesterday}-DONE-Viator.csv'  
file_path_output = fr"{output_viator}/AllLinksViator - {date_today}.xlsx"
file_path_output_processed = fr"{output_viator}/All Links Viator - {date_today}.xlsx"
file_path_output_processed_csv = fr"{output_viator}/All Links Viator - {date_today}.csv"
file_path_csv_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_Groups.csv"
file_path_all_links_send_to_scraper = fr"{output_viator}\SupplierExtract - {date_today}.csv"
file_path_all_links_send_to_scraper_finished = fr"{output_viator}\SupplierExtractFinished - {date_today}.csv"
link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/Viator_links.csv'
all_links_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/AllViator_links.csv'
# Set the path of the local file
local_file_path = file_path_output
# local_file_path = f"{output_viator}/AllLinksViator - {date_today}.xlsx"

# Set the name of your Azure Storage account and the corresponding access key
storage_account_name = "storagemyotas"
storage_account_key = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="

# Set the name of the container and the desired blob name
container_name_raw = "raw/all_links/viator"
container_name_refined = "refined/all_links/viator"

blob_name = fr'Viator - {date_today}.xlsx'
# file_path_logs_processed = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed/{blob_name.split(".")[0]}'

mapping_currency = {'COP\xa0': 'COP (Colombian Peso)', 'HK$': 'HKD (Hong Kong Dollar)', 
                    '¥': 'JPY (Japanese Yen)', 'DKK': 'DKK (Danish Krone)', 'R$': 'BRL (Brazilian Real)',
                    '₹': 'INR (Indian Rupee)', 'MX$': 'MXN (Mexican Peso)', 'ZAR\xa0': 'ZAR (South African Rand)',
                    'PEN\xa0': 'PEN (Peruvian Sol)', 'NZ$': 'NZD (New Zealand Dollar)', '€': 'EUR (Euro)',
                    'CA$': 'CAD (Canadian Dollar)', 'Â£': 'GBP (British Pound Sterling)',
                    'PEN': 'PEN (Peruvian Sol)', 'SEK\xa0': 'SEK (Swedish Krona)', 'NOK': 'NOK (Norwegian Krone)',
                    '$': 'USD (United States Dollar)', 'COP': 'COP (Colombian Peso)', 
                    'NT$': 'TWD (New Taiwan Dollar)', '£': 'GBP (British Pound Sterling)',
                    'â‚¬': 'EUR (Euro)', 'Â¥': 'JPY (Japanese Yen)',
                    'â‚¹': 'INR (Indian Rupee)', 'SEK': 'SEK (Swedish Krona)', 'ZAR': 'ZAR (South African Rand)',
                    'CHF': 'CHF (Swiss Franc)', 'â‚´': 'UAH (Ukrainian Hryvnia)', 'zÅ‚': 'PLN (Polish Zloty)',
                    'Ð»Ð²': 'BGN Bulgarian Lev', 'US$': 'USD (United States Dollar)', 'lei': 'RON (Romanian Leu)',
                    'zł': 'PLN (Polish Zloty)','$U': 'UYU (Uruguayan Peso)', 'COL$': 'COP (Colombian Peso)', 
                    '₴': 'UAH (Ukrainian Hryvnia)',
                    'CHF': 'CHF (Swiss Franc)', 'zł': 'PLN (Polish Zloty)', 'R$': 'BRL (Brazilian Real)',
                    'CL$': 'CLP (Chilean Peso)', 'Rp': 'IDR (Indonesian Rupiah)', 'AR$': 'ARS (Argentine Peso)',
                    '฿': 'THB (Thai Baht)', 'Kč': 'CZK (Czech Koruna)', 'lei': 'RON (Romanian Leu)',
                    '₺': 'TRY (Turkish Lira)', 'A$': 'AUD (Australian Dollar)', 'Ft': 'HUF (Hungarian Forint)',
                    '€': 'EUR (Euro)', '£': 'GBP (British Pound Sterling)', '₹': 'INR (Indian Rupee)',
                    'US$': 'USD (United States Dollar)', 'лв': 'BGN (Bulgarian Lev)',
                    'COL$': 'COP (Colombian Peso)', 'lei': 'RON (Romanian Leu)', 'C$': 'NIO (Nicaraguan Cordoba)',
                    '₺': 'TRY (Turkish Lira)', 'AR$': 'ARS (Argentine Peso)', 'A$': 'AUD (Australian Dollar)',
                    'лв': 'BGN (Bulgarian Lev)', 'Ft': 'HUF (Hungarian Forint)', 'DKK': 'DKK (Danish Krone)',
                    '₪': 'ILS (Israeli Shekel)', '€.': 'EUR (Euro)', '₴': 'UAH (Ukrainian Hryvnia)',
                    'R$': 'BRL (Brazilian Real)', '₹': 'INR (Indian Rupee)', 'zł': 'PLN (Polish Zloty)',
                    'US$': 'USD (United States Dollar)', '€': 'EUR (Euro)', '$U': 'UYU (Uruguayan Peso)',
                    'Kč': 'CZK (Czech Koruna)', 'SEK': 'SEK (Swedish Krona)', '£': 'GBP (British Pound Sterling)',
                    'E£': 'EGP (Egyptian Pound)', 'CL$': 'CLP (Chilean Peso)'}


currency_list = []
API_KEY = '8c36bc42cd11c738c1baad3e2000b40c'

# %%
EUR_City = [
    'Madrid',
    'Florence',
    'Capri',
    'Naples',
    'Taormina',
    'Mount-Etna',
    'Bali',
    'Porto',
    'Krakow',
    'Barcelona',
    'Athens',
    'Palermo',
    'Paris',
    'Dubrovnik',
    'Berlin',
    'Istanbul',
    'Adelaide',
    'Venice',
    'Amsterdam',
    'Cairns-and-the-Tropical-North',
    'Sorrento',
    'Dublin',
    'Rome',
    'Perth',
    'Gold-Coast',
    'Amalfi-Coast',
    'Salta',
    'Bariloche',
    'Milan',
    'Hobart',
    'Mount-Vesuvius',
    'Reykjavik',
    'Pompeii',
    'Vienna',
    'Herculaneum',
    'Lisbon',
    'Brisbane',
    'Marrakech',
    'Mt-Vesuvius',
    'Buenos-Aires',
    'Cartagena',
    'Mendoza',
    'Prague',
    'Rio-de-Janeiro'
]

USD_City = [
    'Oahu',
    'New-York-City',
    'Miami',
    'Cancun',
    'Vancouver',
    'Cappadocia',
    'Las-Vegas',
    'Niagara-Falls-and-Around',
    'Toronto',
    'Dubai',
    'Montreal',
    'San-Francisco',
    'Maui',
    'Punta-Cana',
    'Quebec-City',
    'Queenstown',
    'Singapore',
    'Tokyo'
]

GBP_City = [
    'Belfast',
    'Killarney',
    'Galway',
    'Lanzarote',
    'Edinburgh',
    'Manchester',
    'England',
    'London'
]

# %%
def get_rates(of_date, currency_code='EUR'):
# USING API TO GET RATES FROM SITE https://fixer.io/documentation
    res = requests.get(fr'http://data.fixer.io/api/{of_date}?access_key=acfed48df1159d37fa4305e5e95c234f&base={currency_code}')
    rates = res.json()['rates']
    return rates


# %%
def send_url_to_process_scraperapi(url_input, city_input, category_input, page = 1, max_pages = 25):
    global date_today
    global output_viator
    global file_path_done
    global file_path_output
    global avg_file
    global re_run_path
    global folder_path_with_txt_to_count_avg
    global archive_folder
    data = []
    city_path_done = fr'{output_viator}/{date_today}-{city_input}-{category_input}.csv'          
    if os.path.exists(city_path_done):
        city_done_msg = pd.read_csv(city_path_done)
        page = int(city_done_msg.drop_duplicates(subset='City', keep='last')['Page']) + 1
        
    
    url_time = time.time()
    while page <= max_pages:
        if page == 1:
            url = f'{url_input}'
        else:
            url = f'{url_input}/{page}'
        print(url)

        country_codes = ["eu", "us"]

        random_country_code = random.choice(country_codes)
        
# CHECK THE TXT FILE FOR DATE-CITY IF THERE IS ANYTHING DONE 
#         print(random_country_code)
    
        url_request = requests.post(url = 'https://async.scraperapi.com/jobs', 
                                    json={'apiKey': f'{API_KEY}', 
                                          'country_code': random_country_code,
                                          'url': url })
#         time.sleep(random.uniform(1, 10))
        if url_request.status_code == 200:
            try:
                print(url_request.json()['statusUrl'])
                status_url = url_request.json()['statusUrl']
                data_send_df = pd.DataFrame({
                    'UrlRequest': [url],
                    'UrlResponse': [status_url],
                    'City': [city_input],
                    'Page': [page],
                    'Category': category_input
                }, columns=['UrlRequest', 'UrlResponse', 'City', 'Page', 'Category'])
                data_send_df.to_csv(city_path_done, header=not os.path.exists(city_path_done), index=False, mode='a')
            except json.JSONDecodeError:
                print("JSON could not be decoded")
        else:
            print("HTTP request returned code: ", url_request.status_code, "reduced page number from: ", page, " to ", page-1)
            page -=1


# IN THE TEXT FILE ADD URL AND STATUS AND WHICH PAGE IS IT RELATED TO 
        
        page += 1

# %%
def get_max_pages(url):
    try:
        results = requests.get(url)
        soup = BeautifulSoup(results.content, 'html.parser')
        product_list_count = None

        # Try finding the productListCount label using two different CSS selectors
        selectors = ["[id*=productListCount]", "h3[class*=productListCount]", "h2[class*=productListCountLabel]"]
        for selector in selectors:
            count_element = soup.select_one(selector)
            if count_element:
                product_list_count = int(count_element.text.split()[0].replace(',', ''))
                break

        if product_list_count is None:
            print("Product count not found in the HTML content.")
            return None

        max_pages = int(round(product_list_count / 24, 0))
        return max_pages
    except Exception as e:
        print(f"Error while fetching HTML content: {e}")
        return 25  # Return a default value of 25 pages if there's an error
    
def get_status(url):
    try:
        response = requests.get(url)
        return response.json()['status']
    except Exception as e:
        print(f"Error while fetching URL: {url}, Error: {e}")
        return 'error'

def check_status_and_process_city_data(df_links):
    cities_to_process = []
       
        
    for index, row in df_links.iterrows():
        city = row['City']
        category = row['MatchCategory']
        city_path_done = fr'{output_viator}/{date_today}-{city}-{category}.csv'
        if os.path.exists(city_path_done):
            print(city, '-', category, city_path_done)
            city_done_msg = pd.read_csv(city_path_done)
            city_done_msg.drop_duplicates(inplace=True)
        else:
#             MAYBE REMOVE VALUE FROM DF_LINKS WHEN THERE IS NO FILE
            df_links = df_links[(df_links['City'] != city) & (df_links['MatchCategory'] != category)]
            continue
        start_time_get_resposne = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(get_status, url): url for url in city_done_msg['UrlResponse']}
            for future in concurrent.futures.as_completed(futures):
                url = futures[future]
#                 print(url)
                status = future.result()
                city_done_msg.loc[city_done_msg['UrlResponse'] == url, 'Status'] = status
        end_time_get_resposne = time.time()
#         print(f'First option with concurrent: {round(end_time_get_resposne-start_time_get_resposne,2)}s')
        print(f"For {city} finished {len(city_done_msg[city_done_msg['Status'] == 'finished'])} from {len(city_done_msg)}")
        # Check if all statuses are finished
        if len(city_done_msg[city_done_msg['Status'] == 'finished']) == len(city_done_msg):
            df_links = df_links[(df_links['City'] != city) & (df_links['MatchCategory'] != category)]
            try:
                position = df_links[df_links['City'] == city]['Page'] * 24
            except:
                position = 0

            max_page_for_city = get_max_pages(city_done_msg.iloc[0]['UrlResponse'])
            city_done_msg['MaxPage'] = max_page_for_city
            process_html_from_response_scraperapi(city_done_msg, city_path_done,  position)
            
    return df_links, cities_to_process


# %%
def process_html_from_response_scraperapi(data_city_df, city_path_done, position = 0):
# data_city_df = pd.read_csv(city_path_done)
    data = []
    for index, row in data_city_df.iterrows():
#         print(index)
        results = requests.get(row['UrlResponse'])            
        soup = BeautifulSoup(results.content, 'html.parser')       
        

        tour_items = soup.select("[id*=productName]")

        if len(tour_items) > 0:
            for tour_item in tour_items:
#                 page_pos = tour_item['data-action-page-properties']
#                 page_list = page_pos.split('|')[0].split(':')[1]
#                 position = int(page_pos.split('|')[1].split(':')[1]) + (page - 1) * 24
                position = position + 1
                title = tour_item.find('h2').text.strip()
                splitter = tour_item.text.split('From')[-1][0]
                price = splitter + tour_item.text.split('From')[-1].split(splitter)[1]
                if len(price) > 9:
                    price = price.split('Price')[0]
                part_url = tour_item['data-url'].split('"')[1].split('\\')[0]
                product_url = f"https://www.viator.com{part_url}"
                siteuse = 'Viator'
                city = row['City']
                category = row['Category']
#                 category = 'Global'
                try:
                    discount = tour_item.find('div', {'class': 'text-special product-list-card-savings-label'}).text.strip()
                except:
                    discount = 'N/A'

                amount_reviews = 'N/A'
                #NUMBER OF REVIEWS
                spans = tour_item.select('span')
                for span in spans:
        #             print('________________________')
        #             print(span.attrs)
                    try:
                        span['reviewlink']
                        amount_reviews = span.text
                        break
                    except:
                        pass

                try:
                    stars = tour_item.find('svg').text.strip()
                except:
                    stars = 'N/A'

                text = tour_item.text.strip()


                data.append([title,product_url, price, stars, amount_reviews, discount, text, date_today, position, category, siteuse, city ])
        else:
            tour_items = soup.select("[class*=productListCardWithDebug]")
            print('Running using debug HTML')
            for tour_item in tour_items:
                position = position + 1
                title = tour_item.select_one("[class*=title]").text.strip()
                price = tour_item.select_one("[class*=currentPrice]").text.strip()
                if 'from' in price:
                    price = price.split('from')[1]
                splitter = price[0]
                product_url = f"https://www.viator.com{tour_item.find('a')['href']}"
                siteuse = 'Viator'
                city = row['City']
    #             category = row['Category']
                category = 'Global'

                star ="M7.5 0a.77.77 0 00-.701.456L5.087 4.083a.785.785 0 01-.588.448l-3.827.582a.828.828 0 00-.433 1.395L3.008 9.33c.185.192.26"
                half ="M14.761 6.507a.828.828 0 00-.433-1.395L10.5 4.53a.785.785 0 01-.589-.447L8.201.456a.767.767 0 00-1.402 0L5.087 4.083a.785"
                nostar ="M7.5 1.167l1.565 3.317c.242.52.728.885 1.295.974l3.583.544-2.62 2.673a1.782 1.782 0 00-.48 1.532l.609 3.718L8.315 12.2a1.6"
                try:
                    discount = tour_item.select_one("[class*=savingsLabel]").text.strip()
                except:
                    discount = 'N/A'
                try:
                    amount_reviews = tour_item.select_one("[class*=reviewCount]").text.strip()
                except:
                    amount_reviews = 'N/A'
                try:
                    star_int = 0
                    stars_grouped = tour_item.select_one("[class*=stars]").find_all('svg')
                    half_star = 'M14'
                    for st in stars_grouped:
                        path_text = str(st.find('path')['d'])
                        if half_star in path_text:
                            star_int = star_int + 0.5
                        else:
                            if '0a.77.77' in str(st):
                                star_int = star_int + 1
                    stars = f'star-{str(star_int)}'
                except:
                    stars = 'N/A'
                text = tour_item.text.strip()

                data.append([title,product_url, price, stars, amount_reviews, discount, text, date_today, position, category, siteuse, city ])
        print(f'URL: {city} currency: {splitter}')
    url_done = time.time()
    # message = f'Time for {city}-{category}: {round((url_done - url_time)/60, 3)}min | Pages: {max_pages} | AVG {round((url_done - url_time)/max_pages, 2)}s per page Currency: 1-{first_style_curr}, 2-{second_style_curr}, 3-{thirtd_style_curr}'
    # print(message)
    # logger_info.info(message)
    df = pd.DataFrame(data, columns=['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto'])
    file_path = fr'{output_viator}/{date_today}-{city}-Viator.csv' 
    df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
    data_city_df.to_csv(file_path_done, header=not os.path.exists(file_path_done), index=False, mode='a')
    os.remove(city_path_done)
#     row.to_csv(file_path_done, header=True, index=True) 


# %%
def combine_csv_to_xlsx():
    # Get all CSV files with the specified date prefix
    csv_files = [file for file in os.listdir(f'{output_viator}') if file.endswith('.csv') and file.startswith(date_today)]

    if not csv_files:
        print(f"No CSV files found with the date prefix '{date_today}'")
        return

    # Create a Pandas Excel writer using XlsxWriter as the engine
    output_file = file_path_output
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    for csv_file in csv_files:
        csv_path = os.path.join(f'{output_viator}', csv_file)
        sheet_name = os.path.splitext(csv_file)[0]
        sheet_name = sheet_name.split(date_today + '-')[1].split('-Viator')[0]
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_path)

        # Write the DataFrame to the Excel file
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Save the Excel file
    # writer.save()
    writer.close()

    print(f"Combined CSV files with date prefix '{date_today}' into '{output_file}'")

    # Remove the CSV files
#     for csv_file in csv_files:
#         os.remove(csv_file)
    # Move the CSV files to the Archive folder
    for csv_file in csv_files:
        csv_path = os.path.join(f'{output_viator}', csv_file)
        destination_path = os.path.join(archive_folder, csv_file)
        shutil.move(csv_path, destination_path)

    print(f"Moved {len(csv_files)} CSV file(s) to the '{archive_folder}' folder.")

# %%
def run_1st_page_in_dataframe(df_links):
    if os.path.exists(file_path_done):
        done_msg = pd.read_csv(file_path_done).drop_duplicates(subset=['City', 'Category'], keep='last').reset_index()
        df_links = df_links[~(df_links['City'].isin(done_msg['City']) & df_links['MatchCategory'].isin(done_msg['Category']))]
#         df_links = df_links[~df_links['City'].isin(done_msg['City'].values)]
        
    else:
        print("Nothing done yet")
# ################# THE BELWO CODE PROCESSED ONLY ONE PAGE TO GET MAXIMUM AMOUNT OF PAGES ON THE WEBSITE   
    for index, row in df_links.iterrows():
        url = row["URL"]
        city = row['City']
        category = row['MatchCategory']
        print(city, category, url )
        send_url_to_process_scraperapi(url, city, category, max_pages=1)
        
    while not df_links.empty:
        print(len(df_links))
        df_links, processed_cities = check_status_and_process_city_data(df_links)
# ##########################################################

# %%
def run_all_pages_in_dataframe(df_links):
    if os.path.exists(file_path_done):
        done_msg = pd.read_csv(file_path_done).drop_duplicates(subset=['City', 'Category'], keep='last').reset_index()
        df_links_with_page_maxpage = df_links[df_links['City'].isin(done_msg['City'].values)]
        df_links_with_page_maxpage = pd.merge(df_links_with_page_maxpage, done_msg[['City', 'Page', 'MaxPage']], on='City', how='left')
#         df_links = df_links[~df_links['City'].isin(done_msg['City'].values)]
        
# #################### GET DATA FOR ALL PAGES 
    for index, row in df_links_with_page_maxpage.iterrows():
        url = row["URL"]
        city = row['City']
        category = row['MatchCategory']
        page = row['Page'] + 1
        max_page = round(row['MaxPage']*0.7, 0)
        send_url_to_process_scraperapi(url, city, category, page, max_page)
        
    while not df_links_with_page_maxpage.empty:
        print(len(df_links))
        df_links_with_page_maxpage, processed_cities = check_status_and_process_city_data(df_links_with_page_maxpage)
        print(f'Processed cities: {processed_cities}')

# %%
def transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name):
    global mapping_currency
    global date_today
    global currency_list
    exclude_sheets = ['Sheet1', 'Data', 'Re-Run', 'DONE']
    # Define the Azure Blob Storage connection details
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    # Read the Excel file into a Pandas DataFrame
    rates_eur = get_rates(date_today, 'EUR')
#     rates_gbp = get_rates(date_today, 'EUR')
#     GBP AND USD ARE NOT SUPORTED WITHING THIS CURRENT SUBSRICPTION UPGRADE PLAN
#     rates_gbp = get_rates(date_today, 'GBP')
#     rates_usd = get_rates(date_today, 'USD')
    excel_data = pd.read_excel(local_file_path, sheet_name=None)  # for .xlsx files
    output_file_path = file_path_output_processed
    with pd.ExcelWriter(output_file_path) as writer:
        for sheet_name, df in excel_data.items():
            position = 1
            if sheet_name in exclude_sheets:
                continue
            if sheet_name == 'Mt-Vesuvius':
                sheet_name = 'Mount-Vesuvius'
                df['Miasto'] = 'Mount-Vesuvius'
            # Make changes to the df DataFrame as needed
            df['Data zestawienia'] = df['Data zestawienia'].astype('str')
            df['IloscOpini'].fillna(0, inplace= True)
            df['Opinia'].fillna('N/A', inplace=True)
            df = df[df['Tytul'] != 'Tytul']
            df = df[df['Data zestawienia'] != 'Data zestawienia']
            df = df[df['Data zestawienia'].str.len() > 4]
            df['Tytul URL'] = df['Tytul URL'].str.replace(r'\\"', '', regex=True)
            df['Tytul URL'] = df['Tytul URL'].str.replace(r'\"', '', regex=True)
            df['Tytul URL'] = df['Tytul URL'].str.replace(r'\\', '', regex=True)
            df['IloscOpini'] = df['IloscOpini'].astype(str).str.replace(',','',regex=True)
            df = df.drop_duplicates(subset=['Tytul URL'], keep='first')
            for index, row in df.iterrows():

                df.at[index, 'Pozycja'] = position
                position += 1
                currency = ''
                if 'per group' in row['Cena']:
                    df.at[index, 'Cena'] = row['Cena'].split('per group')[0]
                    row['Cena']= row['Cena'].split('per group')[0]
                for i in range(0,10):
                    if not row['Cena'][i].isnumeric():
                        currency = currency + (row['Cena'][i])
                    else:
                        if row['Cena'][i] == '¹':
                            currency = currency + (row['Cena'][i])
                            continue
                        price = float(row['Cena'][i:].split()[0].replace(',',''))
                        total_price = row['Cena']
                        break
    #             print(currency)
                if sheet_name in EUR_City:
                    try:
                        conversion_rate = float(rates_eur[mapping_currency[currency[:3]][0:3]])
                    except:
                        print(currency, sheet_name)
                elif sheet_name in GBP_City:
                    try:
                        conversion_rate = float(rates_eur[mapping_currency[currency[:3]][0:3]])
                    except:
                        print(currency, sheet_name)
                elif sheet_name in USD_City:
                    try:
                        conversion_rate = float(rates_eur[mapping_currency[currency[:3]][0:3]])
                    except:
                        print(currency, sheet_name)
    #             print(f'{mapping_currency[currency[:3]][0:3]} conversion rate: {conversion_rate}')
    #             print(f'{total_price}- price: {price} - covnersion: {price/(conversion_rate*1.020)}')
                df.at[index, 'Cena'] = round(price/(conversion_rate*1.0185), 2)
                currency_list.append(currency)

            currency_list = list(set(currency_list))
    #         display(df)

    #         df['Cena'] = df['Cena'].map(lambda x: x.split(x[0])[1].strip() if not x[0].isnumeric() else x)
            df.drop(columns=['Przecena', 'Tekst'], inplace=True)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Create a connection to Azure Blob Storage
#     blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#     container_client = blob_service_client.get_container_client(container_name_refined)

#     # Upload the modified Excel file to Azure Blob Storage
#     with open(output_file_path, "rb") as data:
#         container_client.upload_blob(name=blob_name, data=data)
        
#     print("File uploaded successfully to Azure Blob Storage (refined).")
#     os.remove(output_file_path)
#     create_log_done('Refined')
    return 'Added to Blob'


# %%
def weekly_run_viator_all_links(df_links=pd.DataFrame()):
    
    df_links = pd.read_csv(all_links_file)
    df_links = df_links[df_links['Category'] == 'Global']
    if os.path.exists(file_path_output):
        print(f'Today ({date_today}) Viator done')
        return 'Done'  
    run_1st_page_in_dataframe(df_links)
    run_all_pages_in_dataframe(df_links)
    return 'Done'
    


# %%
def convert_excel_to_single_sheet(excel_file, output_excel):
    # Read all sheets of the Excel file into a dictionary of DataFrames
    all_sheets = pd.read_excel(excel_file, sheet_name=None)
    exclude_sheets = ['Sheet1', 'Data', 'Re-Run', 'DONE']
    dataframes_to_combine = []

    for sheet_name, data in all_sheets.items():
        if sheet_name not in exclude_sheets:
            dataframes_to_combine.append(data)

    # Combine data using concat
    combined_data = pd.concat(dataframes_to_combine, ignore_index=True)
    combined_data['Tytul URL'] = combined_data['Tytul URL'].str.lower()
    combined_data.drop_duplicates(subset=['Tytul URL'], inplace=True)
    # If you wish to convert URLs to text to bypass Excel's limitation
    # Comment out the following line if you don't want this
#     combined_data = combined_data.applymap(lambda x: 'URL:' + x if isinstance(x, str) and x.startswith('http') else x)
    combined_data['Tytul URL'] = "'" + combined_data['Tytul URL'].astype(str)
    combined_data.to_excel(output_excel, sheet_name='AllLinks', index=False)


# %%
def check_if_all_csv_processed():
    global date_today
    global output_viator
    global file_path_done
    global file_path_output
    global avg_file
    global re_run_path
    global folder_path_with_txt_to_count_avg
    global archive_folder
    # Get all CSV files with the specified date prefix    
    csv_files = [file for file in os.listdir(f'{output_viator}') if file.endswith('.csv') and file.startswith(date_today)]
    csv_files_not_finished = []
    for csv in csv_files:
        if 'viator' not in csv.lower():
            csv_files_not_finished.append(csv)


    if len(csv_files_not_finished) == 0:
        return 'brake'
    else:
        return f"Files to process: {len(csv_files_not_finished)}"

# %% [markdown]
# Execute below cell to get the all link from Viator page

# %%
def main():
    msg_output = ''
    while True:
        try:
            msg_output = weekly_run_viator_all_links()
            check_brake_option = check_if_all_csv_processed()
            if check_brake_option == 'brake':
                break
            else:
                print(f'CSV file available in {output_viator}')
        except Exception as e:
            print(e)
        
    try:
        combine_csv_to_xlsx()
    except Exception as e:
        print(e)
            
    transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name)    
    convert_excel_to_single_sheet(file_path_output_processed, file_path_output_processed)

    return msg_output


