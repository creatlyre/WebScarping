#!/usr/bin/env python
# coding: utf-8

# # Upload daily excel to SQL Database
# Upload is based on the path to excel file

# In[4]:


import PySimpleGUI as sg
import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np
import os
import logging
import traceback
import datetime


# In[46]:


def upload_daily_to_sql(file_path):
    # Set up database connection details
    server = 'sqlserver-myotas.database.windows.net'
    database = 'OTAs'
    username = 'azureadmin'
    password = 'brudnyHarry!66'   
    driver = '{ODBC Driver 18 for SQL Server}'
#     print(pyodbc.drivers())
    drive_path = r'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs'
    viator_daily_path = fr'{drive_path}\Baza Excel\Viator\Daily\\'
    
    viator_report_path = rf'{drive_path}\Baza Excel\Viator\Daily\ImportReports\\'
    gyg_report_path = rf'{drive_path}\Baza Excel\Get Your Guide\ImportReports\\'
    gyg_daily_path = fr'{drive_path}\Baza Excel\Get Your Guide\\'
    
    try:
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password, timeout=180)
    except:
        return "Coundn't connect to database - retry"

    # Get path to Excel file from user input
#     excel_path = input("Enter path to Excel file: ")
    if '||' in file_path:
        files_upload = file_path.split('||')
    else:
        print('Notsplited')
        files_upload = file_path

    if (os.path.exists(fr"{viator_daily_path}{files_upload[0]}")) and (os.path.exists(fr"{gyg_daily_path}{files_upload[1]}")):
        pass
    else:
        return f'{files_upload[0]}: {os.path.exists(f"{viator_daily_path}{files_upload[0]}")} || {files_upload[1]}: {os.path.exists(f"{gyg_daily_path}{files_upload[1]}")}'
    
    for file_upload in files_upload:
        excel_path = file_upload
        # excel_path = r'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Baza Excel\Viator\Daily\TestVPN_Viator - 2023-05-10.xlsx'
        # List of sheet names to exclude
        exclude_sheets = ['Sheet1', 'Data', 'Re-Run', 'DONE']
        date_of_import = excel_path.split()[-1].split('.')[0]

        # Save report to file ImportOfVPN_Viator - 2023-05-10.txt
        if 'Viator' in excel_path:
            report_path = f"{viator_report_path}ImportOfVPN_Viator - {date_of_import}.txt"
            folder_path = viator_report_path
            excel_path = f"{viator_daily_path}{file_upload}"
            header = ['Tytul', 'Tytul Url', 'Cena', 'Opinia','IloscOpini', 'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto']
#             header = ['Tytul', 'Tytul Url', 'Cena', 'IloscOpini', 'Opinia', 'RozmiarCena', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto']
        elif 'GYG' in excel_path:
            report_path = f"{gyg_report_path}ImportOfGYG - {date_of_import}.txt"
            folder_path = gyg_report_path
            excel_path = f"{gyg_daily_path}{file_upload}"
#             header = ['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja', 'Kategoria', 'VPN_City', 'Booked', 'SiteUse', 'Miasto']
            header = ['Tytul', 'Tytul URL', 'Cena', 'Opinia', 'IloscOpini', 'Przecena', 'Tekst', 'Data zestawienia', 'Pozycja', 'Kategoria', 'Booked', 'SiteUse', 'Miasto', 'VPN_City']
        print(excel_path)
        pd.read_excel(excel_path, sheet_name='Athens', header=None)
        
        files = os.listdir(folder_path)

        if any(date_of_import in file for file in files):
            print(f'Import report already exisit for file {file_path}')
            print(f'Import report already exisit for file {file_upload}')
    #         continue
        else:
            # Load Excel file into pandas dataframe
            xls = pd.ExcelFile(excel_path)

            # Initialize report string
            report_str = ""
            cursor = cnxn.cursor()
            cursor.fast_executemany = True
            i = 1
            # Iterate over each sheet in the Excel file
            for sheet_name in xls.sheet_names:
                # Skip excluded sheets
                if sheet_name in exclude_sheets:
                    print('next', sheet_name)
                    continue
                print(f'{i} - {sheet_name}')
                start = time.time()
                df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)

                print(f'DF read {round(time.time() - start, 4)}s')
                df.columns = header
                df['Data zestawienia'] = df['Data zestawienia'].astype('str')
                df['IloscOpini'].fillna(0, inplace= True)
                df['Opinia'].fillna('N/A', inplace=True)
                df = df[df['Tytul'] != 'Tytul']
                df = df[df['Data zestawienia'] != 'Data zestawienia']
                df = df[df['Data zestawienia'].str.len() > 4]
#                 display(df)
                if sheet_name == 'Mt-Vesuvius':
                    sheet_name = 'Mount-Vesuvius'
                    df['Miasto'] = 'Mount-Vesuvius'
                # Insert Dataframe into SQL Server:
                if 'Viator' in excel_path:
                    insert_query = f"INSERT INTO [{sheet_name}] ([Tytul], [Tytul Url], [Cena], [Opinia], [IloscOpini],\
                    [Data zestawienia], [Pozycja], [Kategoria], [SiteUse], [Miasto])\
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    df['Cena'] = df['Cena'].map(lambda x: x.split(x[0])[1].strip() if not x[0].isnumeric() else x)
                    df.drop(columns=('Przecena'), inplace=True)
                    df.drop(columns=('Tekst'), inplace=True)
                    
                    
                elif 'GYG' in excel_path:
#                     insert_query = f"INSERT INTO [{sheet_name}] ([Tytul], [Tytul Url], [Cena], [Opinia], [IloscOpini], [Przecena],\
#                     [Tekst], [Data zestawienia], [Pozycja], [Kategoria], [VPN_City], [Booked], [SiteUse], [Miasto])\
#                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    insert_query = f"INSERT INTO [{sheet_name}] ([Tytul], [Tytul Url], [Cena], [Opinia], [IloscOpini], [Przecena],\
                    [Data zestawienia], [Pozycja], [Kategoria], [Booked], [SiteUse], [Miasto], [VPN_City])\
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
##                    USED WHEN IMPORT GYG FROM PYTHON
                    df.drop(columns=('Tekst'), inplace=True)
                    df['Booked'] = df['Booked'].astype('str')
                    df['Przecena'] = df['Przecena'].astype('str')
                    df['Cena'] = df['Cena'].map(lambda x: x.split(x[0])[1].strip() if not x[0].isnumeric() else x)
                    df['Booked'] = df['Booked'].map(lambda x: x.split('Booked')[1].split()[0] if len(x) > 5 else x)
                    df['Przecena'] = df['Przecena'].map(lambda x: x.split()[1].replace(",", "") if len(x) > 4 else x)
##                  _________________
                    df['Przecena'].fillna("NULL", inplace= True)
                    df['VPN_City'].fillna("NULL", inplace= True)
                    df['Booked'].fillna("NULL", inplace= True)

                data_list = [tuple(row) for row in df.values]
            #     print(data_list)


            # # FOR TESTING PURPOSE IN CASE OF ANY ERROR
            #     for i, row in enumerate(data_list):
            #         print(i, row)
            #         try:
            #             cursor.execute(insert_query, row)
            #         except pyodbc.DataError:
            #             print(f"Row {i}: {row}")
            #         cnxn.commit()
            ###############################

                start_1 = time.time()
                try:
                    cursor.executemany(insert_query, data_list)
                    cnxn.commit()
            #         print(f'Sucessfully exectued inserted: {len(data_list)} rows')
                except pyodbc.DataError as e:
                    # Print the error message and the row causing the error
                    print(e)
                    print(e.with_traceback())
                print(f'DF insert {round(time.time() - start_1, 4)}s')
                report_str += f"\n{i} - {sheet_name} \n Import successful for sheet: {sheet_name}\n Sucessfully exectued inserted: {len(data_list)} rows \n"
                i = i +1

            #     except:
            #         report_str += f"Import failed for sheet: {sheet_name}\n"
            #         print(report_str)

            # Close database connection
            
            print(report_str)


            with open(report_path, "w") as f:
                f.write(report_str)

            print("Data upload complete!")
    cursor.close()
    cnxn.close()
    return "Done"


# In[47]:


date_add = datetime.date.today()
path = fr'Viator - {date_add}.xlsx||GYG - {date_add}.xlsx'
upload_daily_to_sql(path)


# In[39]:





# In[86]:


# df = pd.read_excel(r'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Baza Excel\Viator\Daily\VPN_Viator - 2023-05-19.xlsx', sheet_name='Athens', header=None)
# xls = pd.ExcelFile(r'G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Baza Excel\Viator\Daily\VPN_Viator - 2023-05-19.xlsx')


# In[87]:


# header = ['Tytul', 'Tytul Url', 'Cena', 'IloscOpini', 'Opinia', 'RozmiarCena', 'Data zestawienia', 'Pozycja', 'Kategoria', 'SiteUse', 'Miasto']
# df.columns = header
# df['Data zestawienia'] = df['Data zestawienia'].astype('str')
# df['IloscOpini'].fillna(0, inplace= True)
# df['Opinia'].fillna('N/A', inplace=True)
# df = df[df['Tytul'] != 'Tytul']
# df = df[df['Data zestawienia'] != 'Data zestawienia']
# df = df[df['Data zestawienia'].str.len() > 1]

