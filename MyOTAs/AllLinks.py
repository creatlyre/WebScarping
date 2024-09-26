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
import io
# eyJhbGciOiJSUzI1NiIsImtpZCI6IjY3YmFiYWFiYTEwNWFkZDZiM2ZiYjlmZjNmZjVmZTNkY2E0Y2VkYTEiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiV29qdGVrIEJhbG9uIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FBY0hUdGZCODM1WVhSalRJeEl4WmxyTnBaRXpWQk9hZmUyMUFmU1dZZXNnUGc9czk2LWMiLCJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vZXhhMi1mYjE3MCIsImF1ZCI6ImV4YTItZmIxNzAiLCJhdXRoX3RpbWUiOjE2ODY2NTg5MDYsInVzZXJfaWQiOiJEcWRXRDhRdloyUTkzcTR4WFhWWlFWUk8wSEMyIiwic3ViIjoiRHFkV0Q4UXZaMlE5M3E0eFhYVlpRVlJPMEhDMiIsImlhdCI6MTY4NjY1OTA2MSwiZXhwIjoxNjg2NjYyNjYxLCJlbWFpbCI6IndvamJhbDNAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZ29vZ2xlLmNvbSI6WyIxMTUwNTc1NjgzNzI4NjQ1MzA0NTciXSwiZW1haWwiOlsid29qYmFsM0BnbWFpbC5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJnb29nbGUuY29tIn19.IAOh_U2LXNXGk1jqG3q6m9utI79QVMDtCuUcDBSH5TEKPmMCEdW962qOZN6J8wfMzexHX1cWoqGcXYBmjLcjQKBhhQoAUAdYjxEivrLHe8Hi37bIwXrEX9mvAKD1wE71Sq1sbB3B9xU51lTsH88l7P0pq9LDgbaKkJCljvvzJ186BTbX9Qw0CF4gma1XjJ1W3Nmd0BK2pE9y0b3arF_V8bSME6BeR4Ls1yKLM9da-MCN5y-IkwGVB6j78Qrt-4_emtAhxjkcYlzauOtEM8dZ0NzblgSxY-hdG_sG-Clg0gM6fxXRQSQJYjqHNgwY7sjAP885JUWbtjWjoXKvdJn_iA

# %%
date_today = datetime.date.today().strftime("%Y-%m-%d")
# date_today = '2023-10-19'
date_yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
output_viator = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Viator/Daily'
file_path_done =fr'{output_viator}/{date_today}-DONE-Viator.csv'  
archive_folder = fr'{output_viator}/Archive'

file_path_done_archive =fr'{archive_folder}/{date_yesterday}-DONE-Viator.csv'  
file_path_output = fr"{output_viator}/AllLinksViator - {date_today}.xlsx"
file_path_output_processed = fr"{output_viator}/All Links Viator - {date_today}.xlsx"
file_path_output_processed_csv = fr"{output_viator}/All Links Viator - {date_today}.csv"
file_path_csv_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_Groups.csv"
file_path_xlsx_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_Groups.xlsx"
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
import pandas as pd
import os
from datetime import datetime
from glob import glob

def process_excel_and_csv(excel_file_path, file_path, path_to_save):
    # Extract the date from the Excel file name
    date_from_filename = os.path.basename(excel_file_path).split(' - ')[1].split(".")[0]

    # Read all sheets from the Excel file into a single DataFrame
    df_day = pd.concat(pd.read_excel(excel_file_path, sheet_name=None), ignore_index=True)

    # Rename columns in df_day to match df_oper
    df_day.rename(columns={
            'Tytul': 'Tytul',
            'Tytul URL': 'Link',
            'Miasto': 'City',
            'IloscOpini': 'Reviews',
            'Data zestawienia': 'Date input'
        }, inplace=True)
    df_day['Date update'] = df_day['Date input']

    df_day['Link'] = df_day['Link'].str.lower()

    # Drop the columns from df_day that are not in df_oper
    df_day = df_day[['Tytul', 'Link', 'City', 'Reviews', 'Date input', 'Date update']]

    # Remove duplicates based on the 'Link' column
    df_day = df_day.drop_duplicates(subset=['Link'])

    

    # Read the CSV file into a DataFrame
    # df_oper = pd.read_csv(file_path.replace('.csv', '.xlsx'))
    df_oper = pd.read_excel(file_path)
    df_oper['Link'] = df_oper['Link'].str.lower()
    # Update the 'Reviews' in df_oper from df_day
    df_oper_updated = pd.merge(df_oper, df_day[['Link', 'Reviews']], on='Link', how='left')
    df_oper_updated['Reviews'] = df_oper_updated['Reviews_y'].combine_first(df_oper_updated['Reviews_x'])
    df_oper_updated.drop(columns=['Reviews_x', 'Reviews_y'], inplace=True)

    # Update 'Date update' for matched links
    df_oper_updated.loc[df_oper_updated['Reviews'].notnull(), 'Date update'] = datetime.strptime(date_from_filename, '%Y-%m-%d')

    # Merge df_oper on top of df_day
    merged_df = pd.concat([df_oper_updated, df_day], ignore_index=True)

    # Drop duplicates while keeping all rows from df_oper
    merged_df = merged_df.drop_duplicates(subset='Link', keep='first')
    merged_df = merged_df[~merged_df['Link'].isnull()]
    merged_df['Link'] = merged_df['Link'].astype(str)
    # Fill empty 'Operator' column entries with 'ToDo'
    merged_df['Operator'] = merged_df['Operator'].fillna('ToDo')
    # Clean GYG file
    if 'GYG' in file_path:
        merged_df['Reviews'] = merged_df['Reviews'].apply(lambda x: str(x).lower().replace('(', '').replace(')', '').replace('reviews', '') if len(str(x)) > 0 else '0')
        merged_df['uid'] = merged_df['Link'].apply(lambda x: str(x).split('-')[-1].replace('/', ''))

    else:
        merged_df['uid'] = merged_df['Link'].apply(lambda x: str(x).split('/')[-1])

    
    # Save the resulting DataFrame to a new file
    output_file_path = os.path.join(file_path)
    # merged_df.to_excel(output_file_path, index=False)
  # Use XlsxWriter as the engine to write the Excel file
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        workbook.strings_to_urls = False
        merged_df.to_excel(writer, index=False, sheet_name='AllLinks')

    with open(output_file_path, 'wb') as f:
        f.write(output.getvalue())
        
    print(f"Processed data saved to {output_file_path}")

# %%
# Example usage:
file_path = file_path_xlsx_operator 
process_excel_and_csv(output_viator + f"/Viator - {date_today}.xlsx", file_path, fr'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe')

# %%
output_gyg = r'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Get Your Guide/'
gyg_file_daily = output_gyg + f'GYG - {date_today}.xlsx'
file_path = file_path_xlsx_operator.replace('Operators_Groups.xlsx', 'Operators_GYG.xlsx')
process_excel_and_csv(gyg_file_daily, file_path, fr'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe')

# %%



