import os
import sys
import pandas as pd
import pyodbc
import re

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

class SQLTableUpload():
    def __init__(self, username, password, logger) -> None:
        self.logger = logger
        self.USERNAME = username
        self.PASSWORD = password

# Function to clean out non-UTF characters
    def clean_text(self, text):
        try:
            # Remove non-ASCII characters or replace with a safe placeholder
            cleaned_text = re.sub(r'[^\x00-\x7F]+', '', text)
            return cleaned_text
        except TypeError:
            return text  # If it's not a string, return the original value
        
    def upsert_df_to_sql_db(self, path_df_main, database_name):

        # Log start of process
        self.logger.logger_info.info(f"Starting upsert process for file {path_df_main} for database {database_name}")

        df_main = pd.read_excel(path_df_main, engine='openpyxl')
        self.logger.logger_info.info(f"Loaded {len(df_main)} rows from {path_df_main}")

        # Apply this function to all string columns in the dataframe to clean non-ASCII characters
        for column in df_main.select_dtypes(include=['object']).columns:
            df_main[column] = df_main[column].apply(lambda x: self.clean_text(x) if isinstance(x, str) else x)
        self.logger.logger_info.info(f"Cleaned text data in dataframe.")

        # Fill missing values and filter data
        df_main['Reviews'] = df_main['Reviews'].fillna(0)
        df_main['Operator'] = df_main['Operator'].fillna('Error')
        df_main['Tytul'] = df_main['Tytul'].fillna('Error')
        df_main['Reviews'] = df_main['Reviews'].astype(str)
        df_main['Operator'] = df_main['Operator'].astype(str)
        df_main['Date input'] = df_main['Date input'].astype(str)
        df_main['Date update'] = df_main['Date update'].astype(str)
        df_main = df_main[df_main['City'].str.len() >= 3]
        self.logger.logger_info.info(f"Processed missing values and filtered cities.")

        # Determine table name based on file
        if 'GYG' in path_df_main:
            table_name = 'Operators_GYG'
        elif 'Musement' in path_df_main:
            table_name = 'Operators_Musement'
        elif 'Headout' in path_df_main:
            table_name = 'Operators_Headout'
        elif 'Viator' in path_df_main:
            table_name = 'Operators_Viator'
        elif 'Tripadvisor' in path_df_main:
            table_name = 'Operators_Tripadvisor'
        df_main = df_main.drop_duplicates(subset=['uid'])
        self.logger.logger_info.info(f"Using table {table_name} for upsert operation.")



        # Database connection settings
        server = 'sqlserver-myotas.database.windows.net'
        database = database_name
        driver = '{ODBC Driver 18 for SQL Server}'

        try:
            cnxn = pyodbc.connect(f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={self.USERNAME};PWD={self.PASSWORD}')
            self.logger.logger_info.info(f"Successfully connected to database {database}.")
        except Exception as e:
            self.logger.logger_done.error(f"Failed to connect to database: {str(e)}")
            return "Couldn't connect to database"

        cursor = cnxn.cursor()
        cursor.fast_executemany = True

        # Create table if it doesn't exist
        create_table_query = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            CREATE TABLE [dbo].[{table_name}] (
                [Tytul]       NVARCHAR (MAX) NULL,
                [Link]        NVARCHAR (MAX) NULL,
                [City]        NVARCHAR (255) NULL,
                [Operator]    NVARCHAR (MAX) NULL,
                [Reviews]     NVARCHAR (255) NULL,
                [Date input]  NVARCHAR (255) NULL,
                [Date update] NVARCHAR (255) NULL,
                [uid]         NVARCHAR (255) NOT NULL PRIMARY KEY
            );
        """
        self.logger.logger_info.info(f"Ensuring table {table_name} exists.")

        try:
            cursor.execute(create_table_query)
            cnxn.commit()
            self.logger.logger_done.info(f"Table {table_name} checked/created successfully.")
        except pyodbc.Error as e:
            self.logger.logger_done.error(f"Error creating table: {str(e)}")
            return "Table creation failed"

        # Upsert query
        merge_query = f"""
            MERGE [dbo].[{table_name}] AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source ([Tytul], [Link], [City], [Operator], [Date input], [Date update], [uid], [Reviews])
            ON target.[uid] = source.[uid]
            WHEN MATCHED THEN
                UPDATE SET
                    target.[Tytul] = source.[Tytul],
                    target.[Link] = source.[Link],
                    target.[City] = source.[City],
                    target.[Operator] = source.[Operator],
                    target.[Date input] = source.[Date input],
                    target.[Date update] = source.[Date update],
                    target.[Reviews] = source.[Reviews]
            WHEN NOT MATCHED THEN
                INSERT ([Tytul], [Link], [City], [Operator], [Date input], [Date update], [uid], [Reviews])
                VALUES (source.[Tytul], source.[Link], source.[City], source.[Operator], source.[Date input], source.[Date update], source.[uid], source.[Reviews]);
        """
        data_list = [tuple(row) for row in df_main.values]
        self.logger.logger_info.info(f"Preparing to upsert {len(data_list)} rows.")

        try:
            cursor.executemany(merge_query, data_list)
            cnxn.commit()
            self.logger.logger_done.info(f"Successfully upserted {len(data_list)} rows.")
        except pyodbc.Error as e:
            self.logger.logger_done.error(f"Data upsert failed: {str(e)}")

        cnxn.close()
        self.logger.logger_done.info(f"Database connection closed.")
        return f'Successfully upserted: {len(data_list)} rows to {table_name} table'
