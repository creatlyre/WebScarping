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
from openpyxl import Workbook, load_workbook
import re
import io
from selenium.webdriver.chrome.webdriver import WebDriver


# from undetected_chromedriver import Chrome, ChromeOptions
# from user_agent import generate_user_agent
# import ctypes  # An included library with Python install.   

# eyJhbGciOiJSUzI1NiIsImtpZCI6IjY3YmFiYWFiYTEwNWFkZDZiM2ZiYjlmZjNmZjVmZTNkY2E0Y2VkYTEiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiV29qdGVrIEJhbG9uIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FBY0hUdGZCODM1WVhSalRJeEl4WmxyTnBaRXpWQk9hZmUyMUFmU1dZZXNnUGc9czk2LWMiLCJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vZXhhMi1mYjE3MCIsImF1ZCI6ImV4YTItZmIxNzAiLCJhdXRoX3RpbWUiOjE2ODY2NTg5MDYsInVzZXJfaWQiOiJEcWRXRDhRdloyUTkzcTR4WFhWWlFWUk8wSEMyIiwic3ViIjoiRHFkV0Q4UXZaMlE5M3E0eFhYVlpRVlJPMEhDMiIsImlhdCI6MTY4NjY1OTA2MSwiZXhwIjoxNjg2NjYyNjYxLCJlbWFpbCI6IndvamJhbDNAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZ29vZ2xlLmNvbSI6WyIxMTUwNTc1NjgzNzI4NjQ1MzA0NTciXSwiZW1haWwiOlsid29qYmFsM0BnbWFpbC5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJnb29nbGUuY29tIn19.IAOh_U2LXNXGk1jqG3q6m9utI79QVMDtCuUcDBSH5TEKPmMCEdW962qOZN6J8wfMzexHX1cWoqGcXYBmjLcjQKBhhQoAUAdYjxEivrLHe8Hi37bIwXrEX9mvAKD1wE71Sq1sbB3B9xU51lTsH88l7P0pq9LDgbaKkJCljvvzJ186BTbX9Qw0CF4gma1XjJ1W3Nmd0BK2pE9y0b3arF_V8bSME6BeR4Ls1yKLM9da-MCN5y-IkwGVB6j78Qrt-4_emtAhxjkcYlzauOtEM8dZ0NzblgSxY-hdG_sG-Clg0gM6fxXRQSQJYjqHNgwY7sjAP885JUWbtjWjoXKvdJn_iA

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
# date_today = '2024-03-32'
output_gyg = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Get Your Guide'
archive_folder = fr'{output_gyg}/Archive'
file_path_done =fr'{output_gyg}/{date_today}-DONE-GYG.csv'  
file_path_output = fr"{output_gyg}/GYG - {date_today}.xlsx"
link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/GYG_links.csv'
file_path_csv_operator_gyg = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_GYG.csv"
file_path_xlsx_operator_gyg = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_GYG.xlsx"
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


# %%
def initilize_driver() -> WebDriver:
    try:

        # Setting up Chrome options
        options = webdriver.ChromeOptions()
        # options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--blink-settings=imagesEnabled=false')

        # Initialize the Chrome driver
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        
        return driver

    except Exception as e:
        raise
    
def quit_driver(driver: WebDriver) -> None:
    driver.quit()   

# %%
def save_dataframe(df, file_path):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        workbook.strings_to_urls = False
        df.to_excel(writer, index=False, sheet_name='AllLinks')
    with open(file_path, 'wb') as f:
        f.write(output.getvalue())

def get_operators_name_from_chrome():
    path_main_file = file_path_xlsx_operator_gyg
#     print(f'--------------{path_main_file}')
    driver = initilize_driver()
#     path = path_gyg.replace('\\','/') # replaces backslashes with forward slashes
#     path = path_gyg[1:len(path_gyg)-1] # to remove quote marks
#     print(path)

    df = pd.read_excel(path_main_file)
    df['Link'] = df['Link'].str.lower()
    df.drop_duplicates(subset=['Link'], inplace=True)              
    df.reset_index()
    timeS=time.time()
    countDone = 0
    countFailed = 0
    for index, row in df.iterrows():
        notFound = False
        if (row["Operator"] == "ToDo" and row['Link'] != 'tytul url'):
#             or ('Â' in str(row["Operator"])  and row['Link'] != 'tytul url'):
        #   print(row['GYG Link'], row['Tytul'])
#                 print(str(row["Operator"]))
            url = row['Link']
            driver.get(url)
            try:
                elem = driver.find_element(By.CLASS_NAME, "supplier-name__link")
#                     print(f"Re: {re.sub('[^A-Za-z0-9 ]+', '', elem.text)}")
            except:
                notFound = True


            if notFound == True:
                df.at[index,'Operator'] = 'Incorrect URL'
                countDone = countDone + 1    
                countFailed = countFailed + 1
            else:
                df.at[index,'Operator'] = re.sub('[^A-Za-z0-9 ]+', '', elem.text)
                countDone = countDone + 1

            
            print(f'Index: {index} | Total time: {time.time() - timeS} | Avg per record: {(time.time() - timeS) / countDone} | Total done | {countDone} | {((countDone - countFailed)/countDone)*100}%')

            if countDone % 10 == 0:
                print('INSERTING DF TO EXCEL')
                save_dataframe(df, path_main_file)
    
    df.to_excel(path_main_file, index=False)    

    quit_driver(driver)

# %%
get_operators_name_from_chrome()

# %%


# %%



