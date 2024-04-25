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
import os
import shutil
import logging
import logging.handlers
import traceback
import re
from threading import Lock, current_thread
from azure.storage.blob import BlobServiceClient
# from undetected_chromedriver import Chrome, ChromeOptions
# from user_agent import generate_user_agent
# import ctypes  # An included library with Python install.   
import random
import requests
import json
import concurrent.futures


# %%
# File paths


# date_today = datetime.date.today().strftime("%Y-%m-%d")
# output_viator = r'output/Viator'
# archive_folder = fr'{output_viator}/Archive'
# file_path_done =fr'output/Viator/{date_today}-DONE-Viator.csv'  
# file_path_output = fr"output/Viator - {date_today}.xlsx"
# link_file = fr'resource/Viator_links.csv'
# avg_file = fr'resource/avg-Viator.csv'
# re_run_path = fr'output/Viator/{date_today}-ReRun-Viator.csv'
# folder_path_with_txt_to_count_avg = 'Avg/Viator'
# logs_path = fr'Logs/Viator'

date_today = datetime.date.today().strftime("%Y-%m-%d")
# date_today = '2024-05-30'
output_viator = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Viator/Daily'
archive_folder = fr'{output_viator}/Archive'
file_path_done =fr'{output_viator}/{date_today}-DONE-Viator.csv'  
file_path_output = fr"{output_viator}/Viator - {date_today}.xlsx"
link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/Viator_links.csv'
max_page_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/Viator_max_page.csv'
avg_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/avg-viator.csv'
re_run_path = fr'{output_viator}/{date_today}-ReRun-Viator.csv'
logs_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/Viator'
# FOR ONE TIME USED NOT SYNCHORNIEZD WITH RUNING APPLCIATION
folder_path_with_txt_to_count_avg = 'Avg/Viator'

# Set the path of the local file
local_file_path = f"{output_viator}/Viator - {date_today}.xlsx"

# Set the name of your Azure Storage account and the corresponding access key
storage_account_name = "storagemyotas"
storage_account_key = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="

# Set the name of the container and the desired blob name
container_name_raw = "raw/daily/viator"
container_name_refined = "refined/daily/viator"

blob_name = fr'Viator - {date_today}.xlsx'
file_path_logs_processed = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed/{blob_name.split(".")[0]}'

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
                    'CHF': 'CHF (Swiss Franc)', 'ARS\xa0': 'ARS (Argentine Peso)', 'ARS': 'ARS (Argentine Peso)',
                    'A$': 'AUD (Australian Dollar)', 'SGD': 'SGD (Singapur Dolar)'}

currency_list = []
API_KEY_SCRAPERAPI = '8c36bc42cd11c738c1baad3e2000b40c' # https://dashboard.scraperapi.com/
API_KEY_ZENROWS = '56ed5b7f827aa5c258b3f6d3f57d36999aa949e8' # https://app.zenrows.com/buildera
file_write_lock = Lock()

# %%
archive_logs_path = os.path.join(logs_path, 'archive_logs')
if not os.path.exists(archive_logs_path):
    os.makedirs(archive_logs_path)
# Function to get the log file name based on the current month
def get_log_file_name(base_file_name):
    current_month = time.strftime("%Y%m")
    return f"{current_month}_{base_file_name}.log"
# Create a function to rotate the logs
def rotate_logs(handler, logger):
    current_month = time.strftime("%Y%m")
    if not handler.baseFilename.endswith(current_month + ".log"):
        logger.removeHandler(handler)
        handler.close()
        handler.baseFilename = os.path.join(logs_path, get_log_file_name(handler.baseFilename))
        logger.addHandler(handler)


# %%
# create logger objects
logger_err = logging.getLogger('Error_logger')
logger_info = logging.getLogger('Info_logger')
logger_done = logging.getLogger('Done_logger')

# set loggers' level
logger_err.setLevel(logging.DEBUG)
logger_info.setLevel(logging.DEBUG)
logger_done.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to console handler
ch.setFormatter(formatter)

# Add console handler to loggers
logger_err.addHandler(ch)
logger_info.addHandler(ch)
logger_done.addHandler(ch)

# Create TimedRotatingFileHandlers for each logger
fh_error = logging.handlers.TimedRotatingFileHandler(
    filename=os.path.join(logs_path, get_log_file_name('error_logs')), 
    when='M', 
    interval=1, 
    backupCount=24
)
fh_info = logging.handlers.TimedRotatingFileHandler(
    filename=os.path.join(logs_path, get_log_file_name('info_logs')), 
    when='M', 
    interval=1, 
    backupCount=24
)
fh_done = logging.handlers.TimedRotatingFileHandler(
    filename=os.path.join(logs_path, get_log_file_name('done_logs')), 
    when='M', 
    interval=1, 
    backupCount=24
)

# Set level for file handlers
fh_error.setLevel(logging.DEBUG)
fh_info.setLevel(logging.INFO)
fh_done.setLevel(logging.INFO)

# Add formatter to file handlers
fh_error.setFormatter(formatter)
fh_info.setFormatter(formatter)
fh_done.setFormatter(formatter)

# Add file handlers to loggers
logger_err.addHandler(fh_error)
logger_info.addHandler(fh_info)
logger_done.addHandler(fh_done)

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
    ''
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
def handle_error_and_rerun(error):
#     recipient_error = 'wojbal3@gmail.com'
    tb = traceback.format_exc()
    logger_err.error('An error occurred: {} on {}'.format(str(error), tb))
#     subject = f'Error occurred - {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}'
#     message = f'<html><body><p>Error occurred: {str(error)} on {tb}</p></body></html>'
#     send_email(subject, message, recipient_error)

# %%
def get_rates(of_date, currency_code='EUR'):
# USING API TO GET RATES FROM SITE https://fixer.io/documentation
    res = requests.get(fr'http://data.fixer.io/api/{of_date}?access_key=acfed48df1159d37fa4305e5e95c234f&base={currency_code}')
    rates = res.json()['rates']
    return rates


# %%
def combine_csv_to_xlsx():
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

    if not csv_files:
        print(f"No CSV files found with the date prefix '{date_today}'")
        return

    # Create a Pandas Excel writer using XlsxWriter as the engine
    output_file = f"{output_viator}/Viator - {date_today}.xlsx"
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    for csv_file in csv_files:
        csv_path = os.path.join(f'{output_viator}', csv_file)
        if 'Viator' not in csv_file:
            continue
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
        if 'DONE' in csv_file:
            df_done = pd.read_csv(csv_path)
            df_done = df_done.drop_duplicates(subset=['City', 'Category'])
            df_done = df_done.drop(columns=['UrlRequest', 'UrlResponse', 'Status', 'Page'])
            df_done['Date'] = date_today
            df_done.to_csv(max_page_file, mode='a', index=False, header=False)
        destination_path = os.path.join(archive_folder, csv_file)
        shutil.move(csv_path, destination_path)
        

    print(f"Moved {len(csv_files)} CSV file(s) to the '{archive_folder}' folder.")

# %%
def create_log_done(log_type):
    global file_path_logs_processed
    if log_type == 'Raw':
        with open(f'{file_path_logs_processed}-raw.txt', 'w') as file:
            file.write('Done')
    elif log_type == 'Refined':
        with open(f'{file_path_logs_processed}-refined.txt', 'w') as file:
            file.write('Done')

# %%
def upload_excel_to_azure_storage_account(local_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name):
    try:
        # Create a connection string to the Azure Storage account
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"

        # Create a BlobServiceClient object using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name_raw)

        # Upload the file to Azure Blob Storage
        with open(local_file_path, "rb") as file:
            container_client.upload_blob(name=blob_name, data=file, )
        create_log_done('Raw')
        print("File uploaded successfully to Azure Blob Storage (raw).")
        
    except Exception as e:
        print(f"An error occurred: {e}")

# %%


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
    rates_gbp = get_rates(date_today, 'EUR')
    rates_usd = rates_eur
    currency_not_found_list = []
    currny_not_found = False
#     GBP AND USD ARE NOT SUPORTED WITHING THIS CURRENT SUBSRICPTION UPGRADE PLAN
#     rates_gbp = get_rates(date_today, 'GBP')

#     rates_usd = get_rates(date_today, 'USD')
    excel_data = pd.read_excel(local_file_path, sheet_name=None)
    output_file_path = "temp_file.xlsx"
    with pd.ExcelWriter(output_file_path) as writer:
        for sheet_name, df in excel_data.items():
            if sheet_name in exclude_sheets:
                continue
            if sheet_name == 'Mt-Vesuvius':
                sheet_name = 'Mount-Vesuvius'
                df['Miasto'] = 'Mount-Vesuvius'
            # Make changes to the df DataFrame as needed
            df['Data zestawienia'] = df['Data zestawienia'].astype('str')
            df['IloscOpini'] = df['IloscOpini'].fillna(0) 
            df['Opinia'] = df['Opinia'].fillna('N/A')
            df = df[df['Tytul'] != 'Tytul']
            df = df[df['Data zestawienia'] != 'Data zestawienia']
            df = df[df['Data zestawienia'].str.len() > 4]
            df['Tytul URL'] = df['Tytul URL'].str.replace('\\"', '', regex=True)
            df['Tytul URL'] = df['Tytul URL'].str.replace('\"', '', regex=True)
            df['Tytul URL'] = df['Tytul URL'].str.replace(r'\\', '', regex=True)
            df['IloscOpini'] = df['IloscOpini'].astype(str).str.replace(',','')
            df['Pozycja'] = df.groupby('Kategoria').cumcount() + 1
            
            for index, row in df.iterrows():
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
                        print("Currency mapping not found for: ",currency," in ", sheet_name)
                        currny_not_found = True
                        currency_not_found_list.append(currency)
                elif sheet_name in GBP_City:
                    try:
                        conversion_rate = float(rates_gbp[mapping_currency[currency[:3]][0:3]])
                    except:
                        print("Currency mapping not found for: ",currency," in ", sheet_name)
                        currny_not_found = True
                        currency_not_found_list.append(currency)
                elif sheet_name in USD_City:
                    try:
                        conversion_rate = float(rates_usd[mapping_currency[currency[:3]][0:3]])
                    except:
                        print("Currency mapping not found for: ",currency," in ", sheet_name)
                        currny_not_found = True
                        currency_not_found_list.append(currency)
    #             print(f'{mapping_currency[currency[:3]][0:3]} conversion rate: {conversion_rate}')
    #             print(f'{total_price}- price: {price} - covnersion: {price/(conversion_rate*1.020)}')
                df.at[index, 'Cena'] = round(price/(conversion_rate*1.0185), 2)
                currency_list.append(currency)

            currency_list = list(set(currency_list))
            if currny_not_found:
                logger_done.info(currency_not_found_list)
                print('Curreny not found: ', currency_not_found_list)
    #         display(df)

    #         df['Cena'] = df['Cena'].map(lambda x: x.split(x[0])[1].strip() if not x[0].isnumeric() else x)
            df.drop(columns=['Przecena', 'Tekst'], inplace=True)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Create a connection to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name_refined)

    # Upload the modified Excel file to Azure Blob Storage
    with open(output_file_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data)
        
    print("File uploaded successfully to Azure Blob Storage (refined).")
    os.remove(output_file_path)
    create_log_done('Refined')
    return 'Added to Blob'


# %%
def currency_switcher(currency_code_text, driver):
    wait_currency = WebDriverWait(driver,2) 
#     try:
#         currency_code_element = wait_currency.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-automation*="EUR"]')))
#         # Click on the EUR element
#         currency_code_element.click()
#         time.sleep(5)
#     except:
#         try:
#             driver.find_element(By.CSS_SELECTOR, 'div[class*="menuContent"]').find_element(By.CSS_SELECTOR, 'button[data-automation="currency"]').click()
#         except:
    driver.find_element(By.CSS_SELECTOR, 'div[class*="menu-container"]').find_element(By.CSS_SELECTOR, '[data-action-page-properties*="currency"]').click()
    time.sleep(5)

#         currency_code = driver.find_elements(By.CSS_SELECTOR, '[data-automation="header-code"]')
#     if len(currency_code) == 0:
    currency_code = driver.find_elements(By.CSS_SELECTOR, '[data-action-tag="select_currency_modal"]')

    for item in currency_code:
            if currency_code_text in item.text:
                item.click()
                time.sleep(15)
                break


# %%
def check_amount_data():
    global date_today
    global output_gyg
    global file_path_done
    global file_path_output
    global avg_file
    global re_run_path
    global folder_path_with_txt_to_count_avg
    #     date_today = datetime.date.today().strftime("%Y-%m-%d")
#     xls = pd.ExcelFile(fr"{output_viator}/Viator - 2023-05-31.xlsx")
    xls = pd.ExcelFile(fr"{output_viator}/Viator - {date_today}.xlsx")
    #     link_file = fr'resource/GYG_links.csv'
    #     avg_file = fr'resource/avg-gyg.csv'
    #     re_run_path = fr'output/GYG/{date_today} - ReRun GYG.csv'
    df_links = pd.read_csv(link_file)
    df_avg = pd.read_csv(avg_file)
    re_run_data = []

    city_to_get_data = df_links['City'].drop_duplicates().tolist()
    for excel_sheet_name in city_to_get_data:
    #     Check if the all excel files which are in df_links are available in created excel file
        if excel_sheet_name in xls.sheet_names:
    #         for viator to change 

    #         Data collected it's loaded excel file for selected city
            data_collected = xls.parse(sheet_name=excel_sheet_name)
            if excel_sheet_name == 'Mt-Vesuvius':
                excel_sheet_name = 'Mount-Vesuvius'
            amount_of_data_collected = len(data_collected)
            print(excel_sheet_name, amount_of_data_collected)
            avg_value_city = int(df_avg[df_avg['City'] == excel_sheet_name]['Avg'])
            if abs(amount_of_data_collected - avg_value_city)/avg_value_city > 0.15 :
                if amount_of_data_collected < avg_value_city:
                    print(abs(amount_of_data_collected - avg_value_city), excel_sheet_name, amount_of_data_collected, avg_value_city)
    #                     logger_done.info(abs(amount_of_data_collected - avg_value_city), excel_sheet_name, amount_of_data_collected, avg_value_city)
                category_to_get = df_links[(df_links['City'] == excel_sheet_name)]['MatchCategory'].tolist()
                category_collected = data_collected['Kategoria'].drop_duplicates().tolist()
    #             display(data_collected.groupby('Kategoria')['Kategoria'].count())
                for category_name in category_to_get:
                    if category_name in category_collected:
                        pass
                    else:
    #                     If the category is missing in the excel sheet add it to re-run data
                        print(f'Missing {category_name} for {excel_sheet_name}')
                        re_run_data.append([excel_sheet_name, category_name])
    #                 FOR TESTING
    #                 re_run_data.append([excel_sheet_name, category_name])
    #                 re_run_data.append([excel_sheet_name, 'all'])
    #     If the excel sheet is missing add it to re-run data
        else:
            print(f'Missing {excel_sheet_name} in data')
            re_run_data.append([excel_sheet_name, 'all'])
    if len(re_run_data) > 0:
        pd.DataFrame(re_run_data).to_csv(re_run_path, index=False, header=['City', 'Category'])


# %%
def count_avg_data_required():
    global date_today
    global output_viator
    global file_path_done
    global file_path_output
    global avg_file
    global re_run_path
    global folder_path_with_txt_to_count_avg
    # COUNT AVG PER CITY 
    # Initialize variables
    city_counts = []
    total_rows = 0
    result = []
    # Iterate over each text file in the directory
    for file_name in os.listdir(folder_path_with_txt_to_count_avg):
        if file_name.endswith('.txt'):
            file_path = os.path.join(folder_path_with_txt_to_count_avg, file_name)

            # Open the text file
            with open(file_path, 'r') as file:
                content = file.read()

                # Extract the city name using regular expressions
                city_list = re.findall(r'\d+ - ([^\n]+).', content)
                count_list = re.findall(r'\d+ rows', content)

                for item1, item2 in zip(city_list, count_list):
                    joined = str(item1) + ' ' + str(item2.split(' ')[0])
                    result.append(joined)

                for row in result:
                    city = row.split(' ')[0]

                    # Extract the row count using regular expressions
                    count_match = row.split(' ')[1]
                    count = int(count_match)
                    # Add the city and row count to the list
                    city_counts.append((city, count))

                    # Update the total row count
                    total_rows += count
    city_population = {}

    # Store population values for each city
    for city, row_count in city_counts:
        if city in city_population:
            city_population[city].append(row_count)
        else:
            city_population[city] = [row_count]

    # Calculate average population for each city
    city_avg = {}
    for city, population_list in city_population.items():
        city_avg[city] = round(sum(population_list) / len(population_list),0)

    # Print average population for each city
    #     report_str+= f"{city} - {round(avg, 0)}"
    avg_path_viator = 'resource/avg-viator.csv'
    # with open(avg_path_viator, "w") as f:
    #                 f.write(report_str)
    df = pd.DataFrame(city_avg.items(), columns=['City', 'Avg'])
    df.to_csv(avg_path_viator, header=True, index=False)

# %%
# ##### FOR RE-RUN PREPARATION
# def re_run_daily():
#     global re_run_path
#     global link_file
#     global archive_folder
#     #     re_run_path = fr'output/GYG/2023-05-31-ReRun-GYG.csv'
#     if os.path.exists(re_run_path):
#         df_re_run = pd.read_csv(re_run_path)
#         df_links = pd.read_csv(link_file)
#         mergded_df_re_run = pd.merge(df_links,df_re_run, how='right', on=('City'))

#         for index, row in mergded_df_re_run.iterrows():
#             if row['Category_y'] == 'all':
#                 continue
#             if row['Category_y'] != row['MatchCategory']:
#                 mergded_df_re_run.drop(index=index, inplace=True)

#         daily_run_viator(mergded_df_re_run, True)
#     else:
#         print('No missing categories or cities')

    
# #     NOT DONE DATA IS NOT BEING INSERTED TO EXCEL FILE

# %%


# %%
# def process_city_data(df_links):
#     cities_to_process = []

#     for city in df_links['City'].unique():
#         city_path_done = fr'{output_viator}/{date_today}-{city}.csv'
#         if os.path.exists(city_path_done):
#             print(city)
#             city_done_msg = pd.read_csv(city_path_done)
#         else:
#             continue
        
#         # Request the status of all URLs at once
#         city_done_msg['Status'] = city_done_msg['UrlResponse'].apply(lambda url: requests.get(url=url).json()['status'])

#         # Check if all statuses are finished
#         if all(city_done_msg['Status'] == 'finished'):
#             print('All finished')
#             df_links = df_links[df_links['City'] != city]
#             try:
#                 position = df_links[df_links['City'] == city]['Page'] * 24
#             except:
#                 position = 0
            
#             max_page_for_city = get_max_pages(city_done_msg.iloc[0]['UrlResponse'])
#             city_done_msg['MaxPage'] = max_page_for_city
#             city_done_msg.to_csv(file_path_done, header=not os.path.exists(file_path_done), index=False, mode='a')
#             cities_to_process.append(city)
#             process_html_from_response_scraperapi(city_done_msg, position)
            
# #             os.remove(city_path_done)

#     return df_links, cities_to_process

# %%
# def send_url_to_process_scraperapi(url_input, city_input, category_input, page = 1, max_pages = 25):
#     global date_today
#     global output_viator
#     global file_path_done
#     global file_path_output
#     global avg_file
#     global re_run_path
#     global folder_path_with_txt_to_count_avg
#     global archive_folder
#     data = []
#     city_path_done = fr'{output_viator}/{date_today}-{city_input}-{category_input}.csv'  
#     if city_input == 'Capri':
#         max_pages = 9
#     elif city_input == 'Taormina':
#         max_pages = 6
#     elif city_input == 'Lisbon' and category_input == 'Global':
#         max_pages = 100
#     elif city_input == 'Porto' and category_input == 'Global' :
#         max_pages = 40
        
#     if os.path.exists(city_path_done):
#         city_done_msg = pd.read_csv(city_path_done)
#         page = int(city_done_msg.drop_duplicates(subset='City', keep='last')['Page']) + 1
# #         print(f'City: {city_input} category: {category_input} have page done {page} in file {city_path_done}')
        
    
#     url_time = time.time()
#     while page <= max_pages:
#         if page == 1:
#             url = f'{url_input}'
#         else:
#             url = f'{url_input}/{page}'
#         print(url)

#         country_codes = [
#             'us',
#             'eu'
#             ]

#         random_country_code = random.choice(country_codes)
        
# # CHECK THE TXT FILE FOR DATE-CITY IF THERE IS ANYTHING DONE 
#         print(random_country_code)
#         params = {
#             'url': url,
#             'apikey': API_KEY_ZENROWS,
#             'js_render': 'true',
#             'json_response': 'true',
#             'js_instructions': """[{"click":".selector"},{"wait":500},{"fill":[".input","value"]},{"wait_for":".slow_selector"}]""",
#             'premium_proxy': 'true',
#         }
#         response = requests.get('https://api.zenrows.com/v1/', params=params)

#         url_request = requests.post(url = 'https://async.scraperapi.com/jobs', 
#                                     json={'apiKey': f'{API_KEY_ZENROWS}', 
#                                           'country_code': random_country_code,
#                                           'url': url })
# #         time.sleep(5)
#         if url_request.status_code == 200:
#             try:
#                 print(url_request.json()['statusUrl'])
#                 status_url = url_request.json()['statusUrl']
#                 data_send_df = pd.DataFrame({
#                     'UrlRequest': [url],
#                     'UrlResponse': [status_url],
#                     'City': [city_input],
#                     'Page': [page],
#                     'Category': category_input
#                 }, columns=['UrlRequest', 'UrlResponse', 'City', 'Page', 'Category'])
#                 data_send_df.to_csv(city_path_done, header=not os.path.exists(city_path_done), index=False, mode='a')
#             except json.JSONDecodeError:
#                 print("JSON could not be decoded")
#         else:
#             print("HTTP request returned code: ", url_request.status_code, "reduced page number from: ", page, " to ", page-1)
#             page -=1


# # IN THE TEXT FILE ADD URL AND STATUS AND WHICH PAGE IS IT RELATED TO 
        
#         page += 1

# %%
def process_html_from_response_zenrows(response, city, category, position = 0, DEBUG=False):    
    data = []
    soup = BeautifulSoup(response.content, 'html.parser')       
    tours = soup.select("[data-automation*=ttd-product-list-card]")
    if DEBUG:
        print(response)
    # Filter these elements to find those that exactly match your target attribute value
    tour_items = [el for el in tours if el.get('data-automation') == r'\"ttd-product-list-card\"']
    print(f"Found {len(tour_items)} elements with exact 'data-automation=ttd-product-list-card' match.")
    if len(tour_items) > 0:
        for tour_item in tour_items:
        #                 page_pos = tour_item['data-action-page-properties']
        #                 page_list = page_pos.split('|')[0].split(':')[1]
        #                 position = int(page_pos.split('|')[1].split(':')[1]) + (page - 1) * 24
            position = position + 1
            if DEBUG:
                print(tour_item.content)
            title = tour_item.select_one("[data-automation*=ttd-product-list-card-title]").get_text()
            price_container = tour_item.select_one("[data-automation*=ttd-product-list-card-price]")
            price = price_container.select_one("[class*=currentPrice]").text.strip().split('from')[-1]
            try:
                part_url = tour_item.select_one("[data-automation*=ttd-product-list-card-link]").get('href').split('"')[1].split('\\')[0]
            except:
                try:
                    part_url = tour_item['href'].split('"')[1].split('\\')[0]
                except:
                    logger_err.error(f"No able to find the HREF for {title}, moving further")
                    part_url = ""
                    
            product_url = f"https://www.viator.com{part_url}"
            siteuse = 'Viator'
            try:
                discount = price_container.select_one("[class*=discountInfoContainer]").select_one("[class*=originalPrice]").text.strip()
            except:
                discount = 'N/A'

            amount_reviews = 'N/A'
            #NUMBER OF REVIEWS
            try:
                amount_reviews = tour_item.select_one("[class*=reviewCount]").text.strip()
            except:
                pass


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
    else:
        tour_items = soup.select("[class*=productListCardWithDebug]")
#             print('Running using debug HTML')
        for tour_item in tour_items:
            position = position + 1
            title = tour_item.select_one("[class*=title]").text.strip()
            price = tour_item.select_one("[class*=currentPrice]").text.strip()
            if 'from' in price:
                price = price.split('from')[1]
            splitter = price[0]
            product_url = f"https://www.viator.com{tour_item.find('a')['href']}"
            siteuse = 'Viator'
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
    # print(f'URL: {city} currency: {splitter}')
    url_done = time.time()
    # message = f'Time for {city}-{category}: {round((url_done - url_time)/60, 3)}min | Pages: {max_pages} | AVG {round((url_done - url_time)/max_pages, 2)}s per page Currency: 1-{first_style_curr}, 2-{second_style_curr}, 3-{thirtd_style_curr}'
    # print(message)
    # logger_info.info(message)
    df = pd.DataFrame(data, columns=['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto'])
    df['Pozycja'] = df.groupby('Kategoria').cumcount() + 1
    file_path = fr'{output_viator}/{date_today}-{city}-Viator.csv' 
    df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')

# %%

def process_city(row, thread_name = None):
    
    global date_today, output_viator, API_KEY_ZENROWS
    
    if thread_name:
        current_thread().name = thread_name
    page = 1
    url_input = row["URL"]
    city_input = row['City']
    category_input = row['MatchCategory']
    max_pages = calculate_max_pages(city_input, category_input)

    city_path_done = fr'{output_viator}/{date_today}-{city_input}-{category_input}.csv'
    city_path_done_archive = fr'{output_viator}/archive/{date_today}-{city_input}-{category_input}.csv'
    
    if os.path.exists(city_path_done):
        city_done_msg = pd.read_csv(city_path_done)
        page = int(city_done_msg.drop_duplicates(subset='City', keep='last')['Page'].iloc[0]) + 1
        logger_info.info(f'Resuming {city_input}-{category_input} from page {page} of {max_pages}')
    elif os.path.exists(city_path_done_archive):
        logger_done.info('City already in Archive folder moving further')
        return
    
    while page <= max_pages:
        url = f'{url_input}' if page == 1 else f'{url_input}/{page}'
        logger_info.info(f'Processing: {city_input}, {category_input}, Page: {page} of max {max_pages}, URL: {url}')
        response = make_request(url)
        logger_info.info(current_thread().name)
        if response and response.status_code == 200:
            try:
                save_data(response, city_input, category_input, url, page, city_path_done)
            except json.JSONDecodeError as e:
                logger_err.error(f'JSON could not be decoded for URL: {url}, error: {str(e)}')
                raise
        else:
            # Log the error with the status code and response content
            logger_err.error(f'HTTP request failed for city: {city_input} category: {category_input} page: {page} with status code {response.status_code}  Decrement the page count. Content: {response.content}')
            page -= 1
            # Specific handling for 403 and 429 status codes
            if response.status_code == 403:
                logger_err.error(f'{current_thread().name}: IP Address Blocked, sleeping for 5 minutes before retrying.')
                time.sleep(300)  # Wait for 5 minutes before retrying
            elif response.status_code == 429:
                logger_err.error(f'{current_thread().name}: Concurrency limit exceeded , sleeping for 5 minutes before retrying.')
                time.sleep(300)  # Wait for 5 minutes before retrying
            else:
                logger_err.error(f'Status code did not set for {response.status_code}')
        page += 1
    
    shutil.move(city_path_done, city_path_done_archive)
    logger_info.info((f'Archived file to {city_path_done_archive}'))


def calculate_max_pages(city_input, category_input):
    if city_input == 'Capri':
        return 9
    if city_input == 'Taormina':
        return 6
    if city_input == 'Lisbon' and category_input == 'Global':
        return 65
    if city_input == 'Porto' and category_input == 'Global':
        return 30
    if city_input == 'Venice' and category_input == 'Global':
        return 55
    return 25 if category_input == 'Global' else 2

def make_request(url):
    params = {
        'url': url,
        'apikey': API_KEY_ZENROWS,
        'js_render': 'true',
        'json_response': 'true',
        'js_instructions': """[{"click":".selector"},{"wait":500},{"fill":[".input","value"]},{"wait_for":".slow_selector"}]""",
        'premium_proxy': 'true',
    }
    return requests.get('https://api.zenrows.com/v1/', params=params)

def save_data(response, city_input, category_input, url, page, city_path_done):
    try:
        data_send_df = pd.DataFrame({
            'UrlRequest': [url],
            'City': city_input,
            'Page': [page],
            'Category': category_input
        }, columns=['UrlRequest', 'City', 'Page', 'Category'])
        data_send_df.to_csv(city_path_done, header=not os.path.exists(city_path_done), index=False, mode='a')
        logger_done.info(f'Data for {city_input}-{category_input}, Page {page} saved on disk')
        process_html_from_response_zenrows(response, city_input, category_input)
    except json.JSONDecodeError:
        print("JSON could not be decoded")

def send_url_to_process_zenrows(df_links):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        for index, row in df_links.iterrows():
            thread_name = f"CityProcessor-{row['City']}-{row['MatchCategory']}-index-{index}"
            futures[executor.submit(process_city, row, thread_name=thread_name)] = row

        concurrent.futures.wait(futures)

# The rest of your global variables and helper functions should be defined outside of these functions.


# %%
# def send_url_to_process_zenrows(df_links):
#     global date_today
#     global output_viator
#     global file_path_done
#     global file_path_output
#     global avg_file
#     global re_run_path
#     global folder_path_with_txt_to_count_avg
#     global archive_folder

#     for index, row in df_links.iterrows():
#         print('Row processing: ', index)
#         page = 1
#         url_input = row["URL"]
#         city_input = row['City']
#         category_input = row['MatchCategory']
#         max_pages = calculate_max_pages(city_input, category_input)

#         city_path_done = fr'{output_viator}/{date_today}-{city_input}-{category_input}.csv'  
#         city_path_done_archive = fr'{output_viator}/archive/{date_today}-{city_input}-{category_input}.csv'  
#         if os.path.exists(city_path_done):
#             city_done_msg = pd.read_csv(city_path_done)
#             page = int(city_done_msg.drop_duplicates(subset='City', keep='last')['Page'].iloc[0]) + 1
#         elif os.path.exists(city_path_done_archive):
#             logger_done.info('City already in Archive folder moving further')
#             df_links = df_links.drop(index)
#             page = max_pages + 1
#             continue
                        

# #         print(f'City: {city_input} category: {category_input} have page done {page} in file {city_path_done}')
        

#         while page <= max_pages:
#             if page == 1:
#                 url = f'{url_input}'
#             else:
#                 url = f'{url_input}/{page}'
#             print(url)
#             page += 1
            
#     # CHECK THE TXT FILE FOR DATE-CITY IF THERE IS ANYTHING DONE 
#             print(city_input, category_input, url, 'Processing in ZEN')
#             params = {
#                 'url': url,
#                 'apikey': API_KEY_ZENROWS,
#                 'js_render': 'true',
#                 'json_response': 'true',
#                 'js_instructions': """[{"click":".selector"},{"wait":500},{"fill":[".input","value"]},{"wait_for":".slow_selector"}]""",
#                 'premium_proxy': 'true',
#             }
#             response = requests.get('https://api.zenrows.com/v1/', params=params)
#             # time.sleep(5)
#             if response.status_code == 200:
#                     try:
#                         data_send_df = pd.DataFrame({
#                             'UrlRequest': [url],
#                             'City': city_input,
#                             'Page': [page],
#                             'Category': category_input
#                         }, columns=['UrlRequest', 'City', 'Page', 'Category'])
#                         data_send_df.to_csv(city_path_done, header=not os.path.exists(city_path_done), index=False, mode='a')
#                         print('Data saved on disk, processing to extract data')
#                         process_html_from_response_zenrows(response, city_input, category_input)
#                     except json.JSONDecodeError:
#                         print("JSON could not be decoded")
#             else:
#                     print("HTTP request returned code: ", response.status_code, "reduced page number from: ", page, " to ", page-1)
#                     page -=1
#         shutil.move(city_path_done, city_path_done_archive)
#         logger_info.info((f'Archived file to {city_path_done_archive}'))


#     # IN THE TEXT FILE ADD URL AND STATUS AND WHICH PAGE IS IT RELATED TO 
            
            

# %%
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def get_max_pages(session, url, retries=5, backoff_factor=0.6):
#     retry_wait = backoff_factor
    
#     for attempt in range(1, retries + 1):
#         try:
#             results = session.get(url, timeout=10 + (retry_wait) + 5)  # 10 seconds timeout
#             results.raise_for_status()  # Raises an error for 4XX or 5XX status codes
#         except requests.exceptions.HTTPError as http_err:
#             logging.error(f'HTTP error occurred for {url}: {http_err}')
#         except requests.exceptions.ConnectionError as conn_err:
#             logging.error(f'Connection error occurred for {url}: {conn_err}')
#         except requests.exceptions.Timeout as timeout_err:
#             logging.error(f'Timeout error occurred for {url}: {timeout_err}')
#         except requests.exceptions.RequestException as err:
#             logging.error(f'Unexpected error occurred for {url}: {err}')
        
#         if attempt < retries:
#             logging.info(f'Waiting {retry_wait} seconds before retrying...')
#             time.sleep(retry_wait)
#             retry_wait *= 2  # Exponential backoff
            
#     soup = BeautifulSoup(results.content, 'html.parser')
#     product_list_count = None

#     # Try finding the productListCount label using two different CSS selectors
#     selectors = ["[id*=productListCount]", "h3[class*=productListCount]", "h2[class*=productListCountLabel]"]

#     for selector in selectors:
#         count_element = soup.select_one(selector)
#         if count_element:
#             product_list_count = int(count_element.text.split()[0].replace(',', ''))
#             break

#     if product_list_count is None:
#         print("Product count not found in the HTML content.")
#         return None
#     try:
#         max_pages = int(round(product_list_count / 24, 0))
#         return max_pages
#     except Exception as e:
#         print(f"Error while fetching HTML content: {e}")
#         return 25  # Return a default value of 25 pages if there's an error
    
# # Define the get_status function with a retry mechanism
# def get_status(session, url, retries=7, backoff_factor=0.9):
#     retry_wait = backoff_factor
#     for attempt in range(1, retries + 1):
#         try:
#             logging.info(f'Attempt {attempt}: Sending request to {url}')
#             response = session.get(url, timeout=10 + retry_wait + 5)  # 10 seconds timeout
#             response.raise_for_status()  # Raises an error for 4XX or 5XX status codes
#             status = response.json().get('status')
#             logging.info(f'Response received: {url} - Status: {status}')
#             return url, status
#         except requests.exceptions.HTTPError as http_err:
#             logging.error(f'HTTP error occurred for {url}: {http_err}')
#         except requests.exceptions.ConnectionError as conn_err:
#             logging.error(f'Connection error occurred for {url}: {conn_err}')
#         except requests.exceptions.Timeout as timeout_err:
#             logging.error(f'Timeout error occurred for {url}: {timeout_err}')
#         except requests.exceptions.RequestException as err:
#             logging.error(f'Unexpected error occurred for {url}: {err}')
        
#         if attempt < retries:
#             logging.info(f'Waiting {retry_wait} seconds before retrying...')
#             time.sleep(retry_wait)
#             retry_wait *= 2  # Exponential backoff

#     # If all retries fail, return 'error' status
#     logging.error(f'All attempts failed for {url}. Marking status as error.')
#     return url, 'error'

# def check_status_and_process_city_data(df_links):
#     cities_to_process = []
#     session = requests.Session()
#     statuses = {}
    
    
#     for index, row in df_links.iterrows():
#         city = row['City']
#         category = row['MatchCategory']
#         city_path_done = fr'{output_viator}/{date_today}-{city}-{category}.csv'
        
        
#         if os.path.exists(city_path_done):
#             print(city, '-', category)
#             city_done_msg = pd.read_csv(city_path_done)
#             city_done_msg.drop_duplicates(inplace=True)
#         else:
#             continue
            
        
#         start_time_get_resposne = time.time()
#         # Using ThreadPoolExecutor to fetch statuses
#         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#             future_to_url = {executor.submit(get_status, session, url): url for url in city_done_msg['UrlResponse']}
#             for future in concurrent.futures.as_completed(future_to_url):
#                 url = future_to_url[future]
#                 try:
#                     _, status = future.result()
#                     statuses[url] = status  # Store the status with the URL as the key
#                 except Exception as exc:
#                     logging.error(f'{url} generated an exception: {exc}')
                    
#          # Update the DataFrame outside of the loop
#         for url, status in statuses.items():
#             city_done_msg.loc[city_done_msg['UrlResponse'] == url, 'Status'] = status
#         end_time_get_resposne = time.time()
#         print(f'First option with concurrent: {round(end_time_get_resposne-start_time_get_resposne,2)}s')
#         print(f"For {city} finished {len(city_done_msg[city_done_msg['Status'] == 'finished'])} from {len(city_done_msg)}")
#         # Check if all statuses are finished
#         if len(city_done_msg[city_done_msg['Status'] == 'finished']) == len(city_done_msg):
#             try:
#                 position = df_links[df_links['City'] == city]['Page'] * 24
#             except:
#                 position = 0

#             max_page_for_city = get_max_pages(session, city_done_msg.iloc[0]['UrlResponse'])
#             city_done_msg['MaxPage'] = max_page_for_city
#             process_html_from_response_scraperapi(city_done_msg, city_path_done,  position)
#             df_links = df_links[(df_links['City'] != city) & (df_links['MatchCategory'] != category)]
#             print(f'In check_status_and_process_city_data finished process removing: {city} - {category} ')
            
#     return df_links, cities_to_process



# %%
# def process_html_from_response_scraperapi(data_city_df, city_path_done, position = 0):
# # data_city_df = pd.read_csv(city_path_done)
#     data = []
# ### OPTION IF THE BELOW WILL NOT WORK PROPELRY CHECK BELOW   
#     session = requests.Session()  # Using a Session object for connection pooling
    
#     # Set up retry strategy with backoff factor
#     retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
#     session.mount('http://', HTTPAdapter(max_retries=retries))
#     session.mount('https://', HTTPAdapter(max_retries=retries))

    
#     for index, row in data_city_df.iterrows():
#         url = row['UrlResponse'].replace(',', '')
        
#         try:
#             logging.info(f'{index} - Starting request for URL: {url}')
#             start_time = time.time()
#             results = session.get(url, timeout=10)  # Set a reasonable timeout
#             logging.info(F"Time: {time.time() - start_time}")
#             results.raise_for_status()  # This will raise an exception for HTTP error codes
#         except requests.exceptions.RequestException as e:
#             # Handle all requests-related exceptions
#             logging.error(f'Request exception for URL {url}: {e}')        
        
#         soup = BeautifulSoup(results.content, 'html.parser')       
#         tour_items = soup.select("[id*=productName]")

#         if len(tour_items) > 0:
#             for tour_item in tour_items:
# #                 page_pos = tour_item['data-action-page-properties']
# #                 page_list = page_pos.split('|')[0].split(':')[1]
# #                 position = int(page_pos.split('|')[1].split(':')[1]) + (page - 1) * 24
#                 position = position + 1
#                 title = tour_item.find('h2').text.strip()
#                 splitter = tour_item.text.split('From')[-1][0]
#                 price = splitter + tour_item.text.split('From')[-1].split(splitter)[1]
#                 if len(price) > 9:
#                     price = price.split('Price')[0]
#                 part_url = tour_item['data-url'].split('"')[1].split('\\')[0]
#                 product_url = f"https://www.viator.com{part_url}"
#                 siteuse = 'Viator'
#                 city = row['City']
#                 category = row['Category']
#                 try:
#                     discount = tour_item.find('div', {'class': 'text-special product-list-card-savings-label'}).text.strip()
#                 except:
#                     discount = 'N/A'

#                 amount_reviews = 'N/A'
#                 #NUMBER OF REVIEWS
#                 spans = tour_item.select('span')
#                 for span in spans:
#         #             print('________________________')
#         #             print(span.attrs)
#                     try:
#                         span['reviewlink']
#                         amount_reviews = span.text
#                         break
#                     except:
#                         pass

#                 try:
#                     stars = tour_item.find('svg').text.strip()
#                 except:
#                     stars = 'N/A'

#                 text = tour_item.text.strip()


#                 data.append([title,product_url, price, stars, amount_reviews, discount, text, date_today, position, category, siteuse, city ])
#         else:
#             tour_items = soup.select("[class*=productListCardWithDebug]")
# #             print('Running using debug HTML')
#             for tour_item in tour_items:
#                 position = position + 1
#                 title = tour_item.select_one("[class*=title]").text.strip()
#                 price = tour_item.select_one("[class*=currentPrice]").text.strip()
#                 if 'from' in price:
#                     price = price.split('from')[1]
#                 splitter = price[0]
#                 product_url = f"https://www.viator.com{tour_item.find('a')['href']}"
#                 siteuse = 'Viator'
#                 city = row['City']
#                 category = row['Category']

#                 star ="M7.5 0a.77.77 0 00-.701.456L5.087 4.083a.785.785 0 01-.588.448l-3.827.582a.828.828 0 00-.433 1.395L3.008 9.33c.185.192.26"
#                 half ="M14.761 6.507a.828.828 0 00-.433-1.395L10.5 4.53a.785.785 0 01-.589-.447L8.201.456a.767.767 0 00-1.402 0L5.087 4.083a.785"
#                 nostar ="M7.5 1.167l1.565 3.317c.242.52.728.885 1.295.974l3.583.544-2.62 2.673a1.782 1.782 0 00-.48 1.532l.609 3.718L8.315 12.2a1.6"
#                 try:
#                     discount = tour_item.select_one("[class*=savingsLabel]").text.strip()
#                 except:
#                     discount = 'N/A'
#                 try:
#                     amount_reviews = tour_item.select_one("[class*=reviewCount]").text.strip()
#                 except:
#                     amount_reviews = 'N/A'
#                 try:
#                     star_int = 0
#                     stars_grouped = tour_item.select_one("[class*=stars]").find_all('svg')
#                     half_star = 'M14'
#                     for st in stars_grouped:
#                         path_text = str(st.find('path')['d'])
#                         if half_star in path_text:
#                             star_int = star_int + 0.5
#                         else:
#                             if '0a.77.77' in str(st):
#                                 star_int = star_int + 1
#                     stars = f'star-{str(star_int)}'
#                 except:
#                     stars = 'N/A'
#                 text = tour_item.text.strip()

#                 data.append([title,product_url, price, stars, amount_reviews, discount, text, date_today, position, category, siteuse, city ])
#         print(f'URL: {city} currency: {splitter}')
#     url_done = time.time()
#     # message = f'Time for {city}-{category}: {round((url_done - url_time)/60, 3)}min | Pages: {max_pages} | AVG {round((url_done - url_time)/max_pages, 2)}s per page Currency: 1-{first_style_curr}, 2-{second_style_curr}, 3-{thirtd_style_curr}'
#     # print(message)
#     # logger_info.info(message)
#     df = pd.DataFrame(data, columns=['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto'])
#     df['Pozycja'] = df.groupby('Kategoria').cumcount() + 1
#     file_path = fr'{output_viator}/{date_today}-{city}-Viator.csv' 
#     df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
#     data_city_df.to_csv(file_path_done, header=not os.path.exists(file_path_done), index=False, mode='a')
#     os.remove(city_path_done)
# #     row.to_csv(file_path_done, header=True, index=True) 


# %%


# %%
def daily_run_viator(df_links=pd.DataFrame(), re_run=False):
    global date_today
    global output_viator
    global file_path_done
    global file_path_output
    global avg_file
    global re_run_path
    global folder_path_with_txt_to_count_avg
    global archive_folder
    if len(df_links) == 0:
        df_links = pd.read_csv(link_file)
    EUR_City = [
        "Amsterdam", "Athens", "Barcelona", "Berlin", "Dublin", "Dubrovnik", "Florence", "Istanbul",
        "Krakow", "Lisbon", "Madrid", "Milan", "Naples", "Paris", "Porto", "Rome", "Palermo", "Venice",
        "Taormina", "Capri", "Sorrento", "Mount-Etna", "Mount-Vesuvius", "Herculaneum", "Amalfi-Coast",
        "Pompeii"
    ]

    USD_City = [
        "Las-Vegas", "New-York-City", "Cancun", "Dubai"
    ]

    GBP_City = [
        "Edinburgh", "London"
    ]

#     date_today = datetime.date.today().strftime("%Y-%m-%d")
#     file_path_done =fr'output/Viator/{date_today}-DONE-Viator.csv'  
#     file_path_output = f"output/Viator - {date_today}.xlsx"
    if os.path.exists(file_path_output) and re_run == False:
        print(f'Today ({date_today}) Viator done')
        return 'Done'



    if os.path.exists(file_path_done) and re_run == False:
        
        done_msg = pd.read_csv(file_path_done).drop_duplicates(subset=['City', 'Category'], keep='last').reset_index()
#         display(df_links)
#         df_links = df_links[~(df_links['City'].isin(done_msg['City']) & df_links['MatchCategory'].isin(done_msg['Category']))]
        merged = df_links.merge(done_msg, left_on=['City', 'MatchCategory'], right_on=['City', 'Category'], how='left', indicator=True)
        # Filter rows where '_merge' is 'left_only', which means the combination is not present in done_msg
        filtered = merged[merged['_merge'] == 'left_only']
        # Drop the _merge column and reset index
        filtered = filtered.drop(columns='_merge').reset_index(drop=True)
        df_links = filtered
#         df_links = df_links[~df_links['City'].isin(done_msg['City'].values)]
        df_links_with_page_maxpage = df_links[df_links['City'].isin(done_msg['City'].values)]
        df_links_with_page_maxpage = pd.merge(df_links_with_page_maxpage, done_msg[['City', 'Page', 'MaxPage']], on='City', how='left')
    elif re_run == True:
        print(f'Lenght of links: {len(df_links)}')
    else:
        logger_info.info("Nothing done yet")

    # Define the URL of the website we want to scrape
    start_time = time.time()
    if len(df_links) == 0:
        print('Df_links empty')
        return 'Done'
    df_links = df_links[df_links['Priority'] > 0]
    send_url_to_process_zenrows(df_links)
    # print('Finished sending data to scraperapi')
        
#     display(df_links)
#     while not df_links.empty:
# #         display(df_links)
#         df_links, processed_cities = check_status_and_process_city_data(df_links)
#         print(f'After processing one row in df_links the df_links is {len(df_links)}')
# #         display(df_links)
        
    return 'Done'

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

# %%
def calculate_max_pages_specualtions(city_input, category_input):
    if city_input == 'Capri':
        return 9
    if city_input == 'Taormina':
        return 6
    if city_input == 'Lisbon' and category_input == 'Global':
        return 65
    if city_input == 'Porto' and category_input == 'Global':
        return 30
    if city_input == 'Venice' and category_input == 'Global':
        return 55
    return 25 if category_input == 'Global' else 2

def count_credits_use():
    df_links = pd.read_csv(link_file)
    total_pages_per_day = sum(calculate_max_pages_specualtions(row['City'], row['MatchCategory']) for index, row in df_links.iterrows())
    credit_per_page = 25
    avg_days_in_month = 30
    logger_done.info(f'There are {total_pages_per_day} pages to collect daily which is {total_pages_per_day*credit_per_page} credits daily')
    logger_done.info(f'Requried credits per month for current setup {total_pages_per_day*credit_per_page*avg_days_in_month}')

# %%


# %%
while True:
    try:
        viator_day = daily_run_viator()
        check_brake_option = check_if_all_csv_processed()
        logger_info.info(check_brake_option)
        if check_brake_option == 'brake':
            break

        else:
            print('re-run not done yet')
    except Exception as e:
        handle_error_and_rerun(e)

try:
    combine_csv_to_xlsx()
except Exception as e:
    handle_error_and_rerun(e)   
    tb = traceback.format_exc()
    logger_err.error('An error occurred: {} on {}'.format(str(e), tb))
# # Call the function to upload the file to Azure Blob Storage
try:
    upload_excel_to_azure_storage_account(local_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name)
except Exception as e:
    handle_error_and_rerun(e)

try:
    transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name)    
except Exception as e:
    handle_error_and_rerun(e)


# %%
# # # Call the function to upload the file to Azure Blob Storage
# try:
#     upload_excel_to_azure_storage_account(local_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name)
# except Exception as e:
#     handle_error_and_rerun(e)

# try:
#     transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name)    
# except Exception as e:
#     handle_error_and_rerun(e)


# %%
# df_links = pd.read_csv(link_file)
# for index, row in df_links.iterrows():
#     city = row['City']
#     category = row['MatchCategory']
#     city_path_done = fr'{output_viator}/{date_today}-{city}-{category}.csv'
#     if os.path.exists(city_path_done):
#         print(city, '-', category)
#         city_done_msg = pd.read_csv(city_path_done)
#         display(city_done_msg)
#         for i, r in city_done_msg.iterrows():
#             url = r['UrlResponse'].replace(',', '')
#             print(url)
#             as_start = time.time()
#             results = requests.get(url)    
#             print('Time: ', time.time() - as_start)
#             print(results)
#             print('_______________________')

# %%
# """
# DEBUG error in output from ZEN

# """
# df_links = pd.read_csv(link_file)
# df_links = df_links.head(1)
# for index, row in df_links.iterrows():
#     print('Row processing: ', index)
#     page = 1
#     url_input = row["URL"]
#     city_input = row['City']
#     category_input = row['MatchCategory']

#     if category_input == 'Global':
#         max_pages = 20
#     else:
#         max_pages = 2

#     if city_input == 'Capri':
#         max_pages = 9
#     elif city_input == 'Taormina':
#         max_pages = 6
#     elif city_input == 'Lisbon' and category_input == 'Global':
#         max_pages = 65
#     elif city_input == 'Porto' and category_input == 'Global' :
#         max_pages = 30


#     # max_pages = 2

#     city_path_done = fr'{output_viator}/{date_today}-{city_input}-{category_input}.csv'  
#     city_path_done_archive = fr'{output_viator}/archive/{date_today}-{city_input}-{category_input}.csv'  
#     if os.path.exists(city_path_done):
#         city_done_msg = pd.read_csv(city_path_done)
#         page = int(city_done_msg.drop_duplicates(subset='City', keep='last')['Page'].iloc[0]) + 1
#     elif os.path.exists(city_path_done_archive):
#         logger_done.info('City already in Archive folder moving further')
#         df_links = df_links.drop(index)
#         page = max_pages + 1
#         continue
                    

# #         print(f'City: {city_input} category: {category_input} have page done {page} in file {city_path_done}')
    

#     while page <= max_pages:
#         if page == 1:
#             url = f'{url_input}'
#         else:
#             url = f'{url_input}/{page}'
#         print(url)
#         page += 1
        
# # CHECK THE TXT FILE FOR DATE-CITY IF THERE IS ANYTHING DONE 
#         print(city_input, category_input, url, 'Processing in ZEN')
#         params = {
#             'url': url,
#             'apikey': API_KEY_ZENROWS,
#             'js_render': 'true',
#             'json_response': 'true',
#             'js_instructions': """[{"click":".selector"},{"wait":500},{"fill":[".input","value"]},{"wait_for":".slow_selector"}]""",
#             'premium_proxy': 'true',
#         }
#         response = requests.get('https://api.zenrows.com/v1/', params=params)
#         # time.sleep(5)
#         if response.status_code == 200:
#                 try:
#                     data_send_df = pd.DataFrame({
#                         'UrlRequest': [url],
#                         'City': city_input,
#                         'Page': [page],
#                         'Category': category_input
#                     }, columns=['UrlRequest', 'City', 'Page', 'Category'])
#                     display(data_send_df)
#                     t = process_html_from_response_zenrows(response, city_input, category_input)
#                     print('Data saved on disk')
#                     data_send_df.to_csv(city_path_done, header=not os.path.exists(city_path_done), index=False, mode='a')
#                 except json.JSONDecodeError:
#                     print("JSON could not be decoded")
#         else:
#                 print("HTTP request returned code: ", response.status_code, "reduced page number from: ", page, " to ", page-1)
#                 page +=1
#     # shutil.move(city_path_done, city_path_done_archive)
#     # logger_info.info((f'Archived file to {city_path_done_archive}'))



# %%


# %%

# data = []
# soup = BeautifulSoup(t.content, 'html.parser')       
# tours = soup.select("[data-automation*=ttd-product-list-card]")
# print(response)
# print("@@@@@@@@@@@@@@\n", response.content)
# # Filter these elements to find those that exactly match your target attribute value
# tour_items = [el for el in tours if el.get('data-automation') == r'\"ttd-product-list-card\"']
# print(f"Found {len(tour_items)} elements with exact 'data-automation=ttd-product-list-card' match.")
# if len(tour_items) > 0:
#     for tour_item in tour_items:
#     #                 page_pos = tour_item['data-action-page-properties']
#     #                 page_list = page_pos.split('|')[0].split(':')[1]
#     #                 position = int(page_pos.split('|')[1].split(':')[1]) + (page - 1) * 24
#         # position = position + 1
#         title = tour_item.select_one("[data-automation*=ttd-product-list-card-title]").get_text()
#         price_container = tour_item.select_one("[data-automation*=ttd-product-list-card-price]")
#         price = price_container.select_one("[class*=currentPrice]").text.strip().split('from')[-1]
#         part_url = tour_item.select_one("[data-automation*=ttd-product-list-card-link]").get('href').split('"')[1].split('\\')[0]
#         product_url = f"https://www.viator.com{part_url}"
#         siteuse = 'Viator'

# for i in tours:
#     if i.get('data-automation') == r'\"ttd-product-list-card\"':
#         print(i.select_one("[data-automation*=ttd-product-list-card-title]").get_text())

# %%
# Title: "Slow Loading Times Challenges in Efficiently Retrieving HTML Content"
# Description:
# This issue revolves around the prolonged loading times experienced when using ScraperAPI to access websites. The process begins with sending a request to ScraperAPI, which in turn provides a URL response containing the HTML content of the desired website. However, the main challenge arises in the subsequent step, where the loading of this HTML content takes an excessively long time. This delay significantly hinders the efficiency of the data retrieval process, affecting the overall performance of applications reliant on timely data scraping. The goal is to identify and resolve the factors contributing to these slow loading times, ensuring a more streamlined and rapid data extraction experience.

# %%
# DELETE


# %%



