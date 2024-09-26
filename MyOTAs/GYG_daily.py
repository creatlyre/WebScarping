# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.webdriver import WebDriver
from bs4 import BeautifulSoup
import time
import pandas as pd
import datetime
from selenium.webdriver.common.action_chains import ActionChains
import os
import shutil
import logging
import traceback
import re
import csv
from azure.storage.blob import BlobServiceClient
import Azure_stopVM
import importlib
# 

# %%
# File paths


# date_today = datetime.date.today().strftime("%Y-%m-%d")
# output_gyg = r'output/GYG'
# archive_folder = fr'{output_gyg}/Archive'
# file_path_done =fr'output/GYG/{date_today}-DONE-GYG.csv'  
# file_path_output = fr"output/GYG - {date_today}.xlsx"
# link_file = fr'resource/GYG_links.csv'
# avg_file = fr'resource/avg-gyg.csv'
# re_run_path = fr'output/GYG/{date_today}-ReRun-GYG.csv'
# folder_path_with_txt_to_count_avg = 'Avg/GYG'

date_today = datetime.date.today().strftime("%Y-%m-%d")
# date_today = '2024-07-11'
output_gyg = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Get Your Guide'
archive_folder = fr'{output_gyg}/Archive'
file_path_done =fr'{output_gyg}/{date_today}-DONE-GYG.csv'  
file_path_output = fr"{output_gyg}/GYG - {date_today}.xlsx"
link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/GYG_links.csv'
max_page_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/GYG_max_page.csv'
avg_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/avg-gyg.csv'
re_run_path = fr'{output_gyg}/{date_today}-ReRun-GYG.csv'
logs_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/GYG'
# FOR ONE TIME USED NOT SYNCHORNIEZD WITH RUNING APPLCIATION
folder_path_with_txt_to_count_avg = 'Avg/GYG'

# Set the path of the local file
local_file_path = f"{output_gyg}/GYG - {date_today}.xlsx"

# Set the name of your Azure Storage account and the corresponding access key
storage_account_name = "storagemyotas"
storage_account_key = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="

# Set the name of the container and the desired blob name
container_name_raw = "raw/daily/gyg"
container_name_refined = "refined/daily/gyg"

blob_name = fr'GYG - {date_today}.xlsx'
file_path_logs_processed = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed/{blob_name.split(".")[0]}'


# %%
# create logger object
logger_err = logging.getLogger('Error_logger')
logger_err.setLevel(logging.DEBUG)
logger_info = logging.getLogger('Info_logger')
logger_info.setLevel(logging.DEBUG)
logger_done = logging.getLogger('Done_logger')
logger_done.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create file handler for error logs and set level to debug
fh_error = logging.FileHandler(fr'{logs_path}/error_logs.log')
fh_error.setLevel(logging.DEBUG)

# create file handler for info logs and set level to info
fh_info = logging.FileHandler(fr'{logs_path}/info_logs.log')
fh_info.setLevel(logging.INFO)

# create file handler for info logs and set level to info
fh_done = logging.FileHandler(fr'{logs_path}/done_logs.log')
fh_done.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to handlers
ch.setFormatter(formatter)
fh_error.setFormatter(formatter)
fh_info.setFormatter(formatter)
fh_done.setFormatter(formatter)

# add handlers to logger
logger_err.addHandler(ch)
logger_err.addHandler(fh_error)
logger_info.addHandler(ch)
logger_info.addHandler(fh_info)
logger_done.addHandler(ch)
logger_done.addHandler(fh_done)

# %%
def handle_error_and_rerun(error):
#     recipient_error = 'wojbal3@gmail.com'
    tb = traceback.format_exc()
    logger_err.error('An error occurred: {} on {}'.format(str(error), tb))
#     subject = f'Error occurred - {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}'
#     message = f'<html><body><p>Error occurred: {str(error)} on {tb}</p></body></html>'
#     send_email(subject, message, recipient_error)

# %%
def combine_csv_to_xlsx():
    """
    This function combines all CSV files in the specified output directory that have
    a filename starting with today's date into a single Excel file.
    Each CSV file is written as a separate sheet in the Excel file.
    After combining, the original CSV files are moved to the archive folder.
    """
    # Get all CSV files with the specified date prefix in the output directory
    csv_files = [file for file in os.listdir(output_gyg) if file.endswith('.csv') and file.startswith(date_today)]


    # Check if no CSV files were found and exit the function if true
    if not csv_files:
        print(f"No CSV files found with the date prefix '{date_today}'")
        return

    # Specify the output Excel file path and name
    output_file = f"{output_gyg}/GYG - {date_today}.xlsx"
    # Create a Pandas Excel writer using XlsxWriter as the engine
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
    
    for csv_file in csv_files:
        # Construct the full file path for the CSV file
        csv_path = os.path.join(output_gyg, csv_file)
        
        # Generate a sheet name based on the CSV file name
        sheet_name = os.path.splitext(csv_file)[0]
        sheet_name = sheet_name.split(date_today + '-')[1].split('-GYG')[0]
        
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_path)
        
        # Write the DataFrame to the Excel file as a new sheet
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Save and close the Excel writer to finalize the Excel file
    writer.close()

    # Log the successful combination of CSV files
    print(f"Combined CSV files with date prefix '{date_today}' into '{output_file}'")

    # Move the original CSV files to the Archive folder
    for csv_file in csv_files:
        # Construct the full file path for the CSV file
        csv_path = os.path.join(output_gyg, csv_file)
        # Specify the destination path in the archive folder
        destination_path = os.path.join(archive_folder, csv_file)
        # Move the CSV file to the Archive folder
        shutil.move(csv_path, destination_path)

    # Log the successful archival of CSV files
    print(f"Moved {len(csv_files)} CSV file(s) to the '{archive_folder}' folder.")

# %%


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
            container_client.upload_blob(name=blob_name, data=file)
        create_log_done('Raw')
        print("File uploaded successfully to Azure Blob Storage (raw).")

    except Exception as e:
        print(f"An error occurred: {e}")

# %%
def transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name):
    exclude_sheets = ['Sheet1', 'Data', 'Re-Run', 'DONE']
    # Define the Azure Blob Storage connection details
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    # Read the Excel file into a Pandas DataFrame
    excel_data = pd.read_excel(local_file_path, sheet_name=None)
    output_file_path = "temp_file.xlsx"
    with pd.ExcelWriter(output_file_path) as writer:
        for sheet_name, df in excel_data.items():
            if sheet_name in exclude_sheets:
                continue
            # Make changes to the df DataFrame as needed
            df['Data zestawienia'] = df['Data zestawienia'].astype('str')
            df['IloscOpini'] = df['IloscOpini'].fillna(0)
            df['Opinia'] = df['Opinia'].fillna('N/A')
            df = df[df['Tytul'] != 'Tytul']
            df = df[df['Data zestawienia'] != 'Data zestawienia']
            df = df[df['Data zestawienia'].str.len() > 4]
            df = df.drop(columns=['VPN_City', 'Tekst'])
            df['Booked'] = df['Booked'].astype('str')
            df['Przecena'] = df['Przecena'].astype('str')
            df['Cena'] = df['Cena'].map(lambda x: x.lower().split('from')[-1] if 'from' in x.lower() else x)
            df['Cena'] = df['Cena'].map(lambda x: x.split(x[0])[1].strip() if not x[0].isnumeric() else x)
            df['Booked'] = df['Booked'].str.replace('New activity', 'nan')
            df['Booked'] = df['Booked'].map(lambda x: x.split('Booked')[1].split()[0] if len(x) > 5 else x)
            df['Przecena'] = df['Przecena'].map(lambda x: x.lower().split('per person')[0] if 'per person' in x.lower() else x)
            df['Przecena'] = df['Przecena'].str.replace(r'[$€£]', '', regex=True).str.strip()
            df['Przecena'] = df['Przecena'].map(lambda x: x.split()[0] if len(x) > 4 else x)
            df['Przecena'] = df['Przecena'].fillna("NULL")
            #     df['VPN_City'].fillna("NULL", inplace= True)
            df['Booked'] = df['Booked'].fillna("NULL")
            
            
            
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    # Create a connection to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name_refined)

    ## Upload the modified Excel file to Azure Blob Storage
    with open(output_file_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data)
        
    print("File uploaded successfully to Azure Blob Storage (refined).")
    os.remove(output_file_path)


# %%
def initilize_driver() -> WebDriver:
    try:
        logger_info.info("Initializing the Chrome driver and logging into the website")

        # Setting up Chrome options
        options = webdriver.ChromeOptions()
        # options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--blink-settings=imagesEnabled=false')

        # Initialize the Chrome driver
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        
        return driver

    except Exception as e:
        logger_err.error(f"An error occurred during login: {e}")
        raise
    
def quit_driver(driver: WebDriver) -> None:
    driver.quit()    

# %%
def daily_run_gyg(df_links=pd.DataFrame(), re_run=False):
    global date_today
    global output_gyg
    global file_path_done
    global file_path_output
    global avg_file
    global re_run_path
    global folder_path_with_txt_to_count_avg
#     link_file = fr'resource/GYG_links.csv'
# Check if the run is re-run to colelct city or not

# DEBUG MODE:
#     df_links=pd.DataFrame()
#     re_run=False
    if len(df_links) == 0:
        df_links = pd.read_csv(link_file)
    # t_url = df_links.iloc[15]['URL']
    # city = df_links.iloc[15]['City']
    # category = df_links.iloc[15]['Category']
    # t_url   
    # df_links = df_links.tail(95)
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
#     file_path_done =fr'output/GYG/{date_today}-DONE-GYG.csv'  
#     file_path_output = fr"output/GYG - {date_today}.xlsx"
    if os.path.exists(file_path_output) and re_run == False:
        print(f'Today ({date_today}) GYG done')
        return 'Done'

    if os.path.exists(file_path_done) and re_run == False:
        done_msg = pd.read_csv(file_path_done)
        done_msg = done_msg.transpose()
        done_msg = done_msg.set_axis(done_msg.iloc[0], axis=1)
        done_msg = done_msg.iloc[1:]
        done_index = int(done_msg.index.values[0])
        df_links = df_links.iloc[(done_index+1):]
    elif re_run == True:
        print(f'Lenght of links: {len(df_links)}')
    else:
        print("Nothing done yet")
        
    
#     df_links = df_links[df_links['WhatIsIt'] != 'Category']
    df_links = df_links[df_links['Run'] == 1]
#     display(df_links)
    driver = initilize_driver()
    # Define the URL of the website we want to scrape
    start_time = time.time()
    total_pages = 1
    iter = 0
    for index, row in df_links.iterrows():
        
    #     CHECK IF FILE PATH EXISIT IF SO CHECK THE DATA INSIDE
#         print(index, row)
        page = 1
        max_pages = 9999
        data = []
        position = 0
        url_time = time.time()
        while page <= max_pages:
            if iter % 25 == 0:
                driver.quit()
                driver = initilize_driver()

            iter +=1
            url = f'{row["URL"]}&p={page}'
            print(url)
            if max_pages == 9999:
                max_pages = 'Set'
        
            driver.get(url)
            time.sleep(1)

            try:
                title_webpage = driver.title
                current_url = driver.current_url
                print(f'Title: {title_webpage} \n\n CURRENT URL: {current_url}')
            except WebDriverException:
                # If an exception occurs, it might indicate that the page is unresponsive
                print("The page might be unresponsive (possibly 'Aw, Snap!'). Attempting to refresh...")
                try:
                    driver.refresh()
                    time.sleep(1)  # Wait for the page to load after refresh
                except WebDriverException:
                    driver.quit()
                    print("Failed to refresh the page. Consider checking your setup or the website status.")
                    driver.get(url=url)
                    print('Closed and opens once again the webpage')
                    time.sleep(4)

            
        #     VERIFY IF THE CURRENCY IS CORRECT
            login_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
#             Below is previous version when the it was Log in instead Prfile
#             login_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Log in']")))
            login_button.click()
#             currency = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Select Currency']")))
            currency = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
            currency = currency.text.strip()
            if row['City'] in EUR_City:
                if 'EUR' in currency:
                    pass
                else:
                    login_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
                    login_button.click()
#                     currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Select Currency']")))
                    currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
                    # hover over the currency switcher button to show the menu
                    actions = ActionChains(driver)
                    actions.move_to_element(currency_switcher_button).perform()
                    currency_switcher_button .click()
                    # wait for the EUR currency option to be clickable
                    eur_currency_option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//li[@class='currency-modal-picker__item-parent item__currency-modal item__currency-modal--EUR']")))
                    # click on the EUR currency option to change the currency
                    eur_currency_option.click()
                    time.sleep(2)
            elif row['City'] in USD_City:
                if 'USD' in currency:
                    pass
                else:
                    login_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
                    login_button.click()
#                     currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Select Currency']")))
                    currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
                    # hover over the currency switcher button to show the menu
                    actions = ActionChains(driver)
                    actions.move_to_element(currency_switcher_button).perform()
                    currency_switcher_button .click()
                    # wait for the EUR currency option to be clickable
                    eur_currency_option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//li[@class='currency-modal-picker__item-parent item__currency-modal item__currency-modal--USD']")))
                    # click on the EUR currency option to change the currency
                    eur_currency_option.click()
                    time.sleep(2)
            elif row['City'] in GBP_City:
                if 'GBP' in currency:
                    pass
                else:
                    login_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
                    login_button.click()
#                     currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Select Currency']")))
                    currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
    # hover over the currency switcher button to show the menu
                    actions = ActionChains(driver)
                    actions.move_to_element(currency_switcher_button).perform()
                    currency_switcher_button .click()
                    # wait for the EUR currency option to be clickable
                    eur_currency_option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//li[@class='currency-modal-picker__item-parent item__currency-modal item__currency-modal--GBP']")))
                    # click on the EUR currency option to change the currency
                    eur_currency_option.click()
                    time.sleep(2)
            else:
#                 pass
                print('Missing from the list:', row['City'])

            # Parse the HTML content of the page using Beautiful Soup
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            if max_pages == 'Set':
                try:
                    max_pages = int((soup.find('span', {'class': 'trip-item-pagination__controls-info'}).text.strip()).split(' ')[-1])        
                except:
                    try:
                        max_pages = round(float(soup.find('div', {'class': 'search-header__left__data-wrapper__count'}).text.strip().split()[0])/40,0)+1
                        print('Divided by amount of activiytes: ', max_pages)
                    except:
                        max_pages = 5
                        print('Dindt found max page - new UI')
                total_pages = total_pages+max_pages
# #############################################                
#                 max_pages = 1
# #############################################

            # Extract the data from the HTML using Beautiful Soup
            tour_items = soup.find_all('li', {'class': 'list-element'})
            if len(tour_items) == 0:
                tour_items = soup.select("[data-test-id=vertical-activity-card]")
            # print(tour_items)
            date_today = datetime.datetime.now().strftime('%Y-%m-%d')
            for tour_item in tour_items:
                title = tour_item.find('h3', {'class': 'vertical-activity-card__title'}).text.strip()
                price = tour_item.find('div', {'class': 'baseline-pricing__value'}).text.strip()
#                 product_category = tour_item.find('span', {'class': 'vertical-activity-card__activity-type c-classifier-badge'}).text.strip()
                product_url = f"https://www.getyourguide.com/{tour_item.find('a')['href']}"
                product_url = product_url.split('?ranking_uuid')[0]
                try:
                    position = int(tour_item['key']) + 1 + (page - 1) * 16
                except:
                    position = position + 1
                siteuse = 'GYG'
                city = row['City']
                category = row['RawCategory']
                try:
                    discount = tour_item.find('div', {'class': 'baseline-pricing__value baseline-pricing__value--low'}).text.strip()
                except:
                    discount = 'N/A'
                try:
                    amount_reviews = tour_item.find('div', {'class': 'rating-overall__reviews'}).text.strip()
                except:
                    try:
                        amount_reviews = tour_item.find('div', {'class': 'c-activity-rating__label'}).text.strip()
                    except:
                        amount_reviews = 'N/A'
                try:
                    stars = tour_item.find('span', {'rating-overall__rating-number rating-overall__rating-number--right'}).text.strip()
                except:
                    try:
                        stars = tour_item.find('span', {'c-activity-rating__rating'}).text.strip()
                    except:
                        stars = 'N/A'
                try:
                    booked = tour_item.find('span', {'class': 'c-marketplace-badge c-marketplace-badge--secondary'}).text.strip()
                except:
                    booked = 'N/A'
                try:
                    new_activity = tour_item.find('span', {'class': 'activity-info__badge c-marketplace-badge c-marketplace-badge--secondary'}).text.strip()
                except:
                    new_activity = 'N/A'

                text = tour_item.text.strip()

                data.append([title,product_url, price, stars, amount_reviews, discount, text, date_today, position, category, booked, siteuse, city ])


            page += 1
        url_done = time.time()
        message = f'Time for {city}-{category}: {round((url_done - url_time)/60, 3)}min | Pages: {max_pages} | AVG {round((url_done - url_time)/max_pages, 2)}s per page'
        print(message)
        logger_info.info(message)
        df = pd.DataFrame(data, columns=['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja', 'Kategoria', 'Booked', 'SiteUse', 'Miasto'])
        df['Cena'] = df['Cena'].map(lambda x: x.split(' ')[-1])
        df['Przecena'] = df['Przecena'].map(lambda x: x.split('From')[1] if x != 'N/A' else 'N/A')
        df['IloscOpini'] = df['IloscOpini'].map(lambda x: x.split('(')[-1].split(')')[0].split(' ')[0].replace(',', '') if x != 'N/A' else x)
        df['VPN_City'] = ''
        with open(max_page_file, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)

            # Append the data
            csvwriter.writerow([city, category, max_pages, date_today])
            
        file_path = fr'{output_gyg}/{date_today}-{city}-GYG.csv' 
        df.to_csv(file_path, header=not os.path.exists(file_path), index=False, mode='a')
        row.to_csv(file_path_done, header=True, index=True)    
    driver.quit()
    end_time = time.time()
    message_done = f'Done {len(df_links)} URLs in {round((end_time - start_time)/60,2)} mins | Pages: {total_pages} | AVG: {round((end_time - start_time)/total_pages, 2)}s'
    # print(message_done)

    logger_done.info(message_done)
    if re_run == False:
        combine_csv_to_xlsx()


# %%
# Excel file which will be checked against avg values
def check_amount_data():
    global date_today
    global output_gyg
    global file_path_done
    global file_path_output
    global avg_file
    global re_run_path
    global folder_path_with_txt_to_count_avg
#     date_today = datetime.date.today().strftime("%Y-%m-%d")
#     xls = pd.ExcelFile(fr"output/GYG - 2023-05-27.xlsx")
    xls = pd.ExcelFile(fr"{output_gyg}/GYG - {date_today}.xlsx")
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
    #         Data collected it's loaded excel file for selected city
            data_collected = xls.parse(sheet_name=excel_sheet_name)
            amount_of_data_collected = len(data_collected)
    #         print(excel_sheet_name, amount_of_data_collected)
            avg_value_city = int(df_avg[df_avg['City'] == excel_sheet_name]['Avg'])
            if abs(amount_of_data_collected - avg_value_city)/avg_value_city > 0.15 :
                if amount_of_data_collected < avg_value_city:
#                     print(abs(amount_of_data_collected - avg_value_city), excel_sheet_name, amount_of_data_collected, avg_value_city)
                    logger_done.info(abs(amount_of_data_collected - avg_value_city), excel_sheet_name, amount_of_data_collected, avg_value_city)
                category_to_get = df_links[(df_links['City'] == excel_sheet_name) & (df_links['WhatIsIt'] == 'Category')]['RawCategory'].tolist()
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
    global output_gyg
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
    avg_path_viator = 'resource/avg-gyg.csv'
    # with open(avg_path_viator, "w") as f:
    #                 f.write(report_str)
    df = pd.DataFrame(city_avg.items(), columns=['City', 'Avg'])
    df.to_csv(avg_path_viator, header=True, index=False)

# %%
##### FOR RE-RUN PREPARATION
def re_run_daily():
    global re_run_path
    global link_file
    global archive_folder
#     re_run_path = fr'output/GYG/2023-05-31-ReRun-GYG.csv'
    if os.path.exists(re_run_path):
        df_re_run = pd.read_csv(re_run_path)
        df_links = pd.read_csv(link_file)
        df_links = df_links[df_links['WhatIsIt'] == 'Category']
        mergded_df_re_run = pd.merge(df_links,df_re_run, how='right', on=('City'))

        for index, row in mergded_df_re_run.iterrows():
            if row['Category_y'] == 'all':
                continue
            if row['Category_y'] != row['RawCategory']:
                mergded_df_re_run.drop(index=index, inplace=True)

        daily_run_gyg(mergded_df_re_run, True)
    else:
        print('No missing categories or cities')

    
#     NOT DONE DATA IS NOT BEING INSERTED TO EXCEL FILE

# %%
while True:
    try:
        gyg_day = daily_run_gyg()
        if gyg_day == 'Done':
            break
        else:
            print('re-run not done yet')
    except Exception as e:
        handle_error_and_rerun(e)

# After sucessfull run check amount of data in Excel file if the data is missing collect missing city and/or categories
# check_amount_data()
# re_run_daily()

# Call the function to upload the file to Azure Blob Storage
try:
    upload_excel_to_azure_storage_account(local_file_path, storage_account_name, storage_account_key, container_name_raw, blob_name)
except Exception as e:
    handle_error_and_rerun(e)

try:
    transform_upload_to_refined(local_file_path, storage_account_name, storage_account_key, container_name_refined, blob_name)    
except Exception as e:
    handle_error_and_rerun(e)

# %%
if 'backup' in os.getcwd():
    importlib.reload(Azure_stopVM)
    script_name = 'Viator_daily.py'

    check_if_viator_running = Azure_stopVM.check_if_script_is_running(script_name)
    if check_if_viator_running:
        logger_done.info(f"{script_name} is currently running.")
    else:
        logger_done.info(f"{script_name} is not running. Stoping VM")
        Azure_stopVM.stop_vm()


# %%

# ##################DEBUG CURRENCY SWITCHER




# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
# driver.maximize_window()
# # Define the URL of the website we want to scrape
# start_time = time.time()
# total_pages = 0
# #     CHECK IF FILE PATH EXISIT IF SO CHECK THE DATA INSIDE
# #         print(index, row)
# page = 1
# max_pages = 9999
# data = []
# position = 0
# url_time = time.time()

# url = f'https://www.getyourguide.com/s?q=Amsterdam&p=1'

# driver.get(url)
# time.sleep(1)
# #     VERIFY IF THE CURRENCY IS CORRECT
# login_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Profile']")))
# login_button.click()
# # currency = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@title='Select Currency']")))
# currency = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
# currency
# html = driver.page_source
# soup = BeautifulSoup(html, 'html.parser')

# %%
# currency_switcher_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='option option-currency']")))
# # hover over the currency switcher button to show the menu
# actions = ActionChains(driver)
# actions.move_to_element(currency_switcher_button).perform()
# currency_switcher_button .click()
# # wait for the EUR currency option to be clickable
# eur_currency_option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//li[@class='currency-modal-picker__item-parent item__currency-modal item__currency-modal--EUR']")))
# # click on the EUR currency option to change the currency
# eur_currency_option.click()

# html = driver.page_source
# soup = BeautifulSoup(html, 'html.parser')

# tour_items = soup.select("[data-test-id=vertical-activity-card]")
# len(tour_items)
# title = tour_items[0].find('p', {'class': 'vertical-activity-card__title'}).text.strip()
# price = tour_items[0].find('div', {'class': 'baseline-pricing__value'}).text.strip()


