# OTAs/reports/historical_report_generator.py
import numpy as np
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import re
import pdfkit
from io import BytesIO
import os
import matplotlib.dates as mdates
import base64
import time
from PIL import Image
import logging
import sys
import random

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

# Optional: Configure seaborn aesthetics
sns.set(style="whitegrid")


class HistoricalReportGenerator:
    # ----------------------------- Configuration -----------------------------

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

    # Database connection settings
    SERVER = 'sqlserver-myotas.database.windows.net'
    DATABASE = 'OTAs'  # Default database name
    DRIVER = '{ODBC Driver 18 for SQL Server}'

    # Path to wkhtmltopdf executable
    WKHTMLTOPDF_PATH = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'  # Update this path if different

    def __init__(self, username, password, city=None, ota=None,logo_path='reports\logo_color.png'):
        self.USERNAME = username
        self.PASSWORD = password
        self.cnxn = None
        self.logo_path = os.path.join(project_root, logo_path)
        self.logo_base64 = self.load_logo_base64()
        # Set default currency
        self.currency = '€'
        self.ota = ota
        # Dynamic currency mapping based on city
        self.city_currency_map = {
            'london': '£',
            'las vegas': '$',
            # Add more cities and their currencies as needed
        }
        
        # Update currency based on city
        if city and isinstance(city, str):
            self.currency = self.city_currency_map.get(city.lower(), self.currency)

        self.city = city
        self.overview = []  # Store a brief summary for external use

    # ------------------------- Utility Functions -----------------------------

    def connect_to_database(self, retry_count=0, max_retries=3):
        """
        Establishes a connection to the SQL Server database.
        Implements a retry mechanism if the database might be auto-stopped or unavailable.
        """
        try:
            connection_string = (
                f'DRIVER={self.DRIVER};'
                f'SERVER=tcp:{self.SERVER};'
                f'PORT=1433;'
                f'DATABASE={self.DATABASE};'
                f'UID={self.USERNAME};'
                f'PWD={self.PASSWORD}'
            )
            self.cnxn = pyodbc.connect(connection_string, timeout=30)
            logging.info(f"Successfully connected to database '{self.DATABASE}'.")
            return self.cnxn

        except pyodbc.OperationalError as e:
            if 'Timeout' in str(e) or 'Login timeout expired' in str(e):
                if retry_count < max_retries:
                    retry_count += 1
                    logging.warning(f"Operational error: {str(e)}. Retrying in 30 seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(30)  # Wait for 30 seconds before retrying
                    return self.connect_to_database(retry_count, max_retries)
                else:
                    logging.error("Max retries reached. Could not reconnect to the database.")
                    if self.cnxn:
                        self.cnxn.close()
                    return None
            else:
                logging.error(f"Operational error occurred: {str(e)}")
                if self.cnxn:
                    self.cnxn.close()
                return None

        except Exception as e:
            if retry_count < max_retries:
                retry_count += 1
                logging.warning(f"Unexpected error: {str(e)}. Retrying in 30 seconds... (Attempt {retry_count}/{max_retries})")
                time.sleep(30)  # Wait for 30 seconds before retrying
                return self.connect_to_database(retry_count, max_retries)
            else:
                logging.error(f"Max retries reached. Could not recover from error: {str(e)}")
                raise RuntimeError(f"Failed to connect to the database after {max_retries} attempts. Last error: {str(e)}")


    def extract_table_name(self, url):
        """
        Extracts the table name from the provided URL.
        For example, from 'https://www.getyourguide.com/rome-l33/...', it extracts 'Rome'.
        """
        try:
            if "viator" in url:
                part_after_viator = url.split("viator.com/tours/")[-1]
                city_part = part_after_viator.split("/")[0]
                table_name = city_part.capitalize()
            else:
                part_after_gyg = url.split("getyourguide.com/")[-1]
                city_part = part_after_gyg.split("-")[0]
                table_name = city_part.capitalize()
            logging.info(f"Extracted table name from URL: '{table_name}'")
            return table_name
        except Exception as e:
            logging.error("Failed to extract table name from URL.")
            return None

    def check_table_exists(self, table_name):
        """
        Checks if the specified table exists in the database.
        """
        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?
        """
        try:
            df = pd.read_sql_query(query, self.cnxn, params=(table_name,))
            exists = not df.empty
            logging.info(f"Table '{table_name}' exists: {exists}")
            return exists
        except Exception as e:
            logging.error(f"Error checking table existence: {str(e)}")
            return False
    def extract_uid(self, url):

        if 'getyourguide' in url.lower():
            uid = url.lower().split('getyourguide')[-1].split('-')[-1].replace('/','')
        elif 'viator' in url.lower():
            uid = url.lower().split('viator')[-1].split('/')[-1].replace('/','')
        elif 'musement' in url.lower():
            uid = url.lower().split('musement')[-1].split('-')[-1].replace('/','')
        else:
            return None  # Return None if no pattern matches
        return uid
    
    def fetch_data(self, table_name, url, date_filter=None):
        """
        Fetches historical data for the given URL from the specified table, including Kategoria and Pozycja.
        Applies date filtering based on the provided date_filter parameter.
        """
        # Clean the table name to prevent SQL injection (basic sanitization)
        if not re.match(r'^[\w\.\[\]\-]+$', table_name):
            logging.error("Invalid table name provided.")
            return None

        # Define date filtering conditions
        date_conditions = {
            'previous_month':   "[Data zestawienia] >= DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))"
                                "AND [Data zestawienia] < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)",
            'previous_week':    "[Data zestawienia] >= DATEADD(WEEK, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) "
                                "AND [Data zestawienia] < DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)",
            'previous_quarter': "[Data zestawienia] >= DATEADD(QUARTER, -1, DATEFROMPARTS(YEAR(GETDATE()), "
                                "((MONTH(GETDATE())-1)/3)*3 + 1, 1)) "
                                "AND [Data zestawienia] < DATEFROMPARTS(YEAR(GETDATE()), ((MONTH(GETDATE())-1)/3 + 1)*3 + 1, 1)",
            'last_week': "      [Data zestawienia] >= DATEADD(DAY, -7, GETDATE())",

            'last_year_to_date': "[Data zestawienia] >= DATEADD(DAY, -365, GETDATE())",

            'last_year': "[Data zestawienia] >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1) AND [Data zestawienia] < DATEFROMPARTS(YEAR(GETDATE()), 1, 1)"

        }

        # Determine the date filter condition
        date_filter_condition = date_conditions.get(date_filter, "")
        uid = self.extract_uid(url)
        if not uid:
            raise ValueError(f"UID could not be extracted from the URL: {url}")
        # Build the query with dynamic date filtering if provided
        query = f"""
        SELECT 
            [Tytul], 
            [Tytul Url], 
            [Cena], 
            [IloscOpini], 
            [Opinia], 
            [RozmiarCena],
            [Data zestawienia],
            [Miasto],
            [Booked],
            [Kategoria],
            [Pozycja],
            [Przecena]
        FROM 
            [{table_name}]
        WHERE 
             [Tytul Url] LIKE '%{uid}%'
            {f"AND {date_filter_condition}" if date_filter_condition else ""}
        ORDER BY 
            [Data zestawienia] ASC
        """

        try:
            # Execute the query with the URL parameter
            df = pd.read_sql_query(query, self.cnxn)
            if df.empty:
                logging.warning("No data found for the provided URL.")
                return None
            return df
        except Exception as e:
            logging.error(f"Error fetching data: {str(e)}")
            return None
        
    def generate_dynamic_explanations_price_over_time(self, df_primary):
        df_primary = df_primary.reset_index()
    # Calculate basic stats for price data
        price_min = df_primary['Cena'].min()
        price_max = df_primary['Cena'].max()
        price_mean = df_primary['Cena'].mean()
        price_std = df_primary['Cena'].std()

        total_days = (df_primary['Data zestawienia'].max() - df_primary['Data zestawienia'].min()).days
        if total_days < 40:
            # Short single-month explanation
            price_over_time_desc = (
                f"This chart shows the price fluctuations for {total_days} days. "
                f"The lowest price is {self.currency}{price_min:.2f}, "
                f"the highest is {self.currency}{price_max:.2f}, "
                f"with an average of {self.currency}{price_mean:.2f}."
            )
            return price_over_time_desc
        else:
            # Identify date range for specific price changes
            first_price_date = df_primary['Data zestawienia'].iloc[0].strftime('%Y-%m-%d')
            last_price_date = df_primary['Data zestawienia'].iloc[-1].strftime('%Y-%m-%d')

            # Identify the biggest price change and when it happened
            price_diff = df_primary['Cena'].diff().abs().max()
            idx_of_largest_change = df_primary['Cena'].diff().abs().idxmax()
            price_change_date = df_primary['Data zestawienia'].iloc[idx_of_largest_change].strftime('%Y-%m-%d')

            # Calculate the largest price change as a percentage
            previous_price = df_primary['Cena'].iloc[idx_of_largest_change - 1] if idx_of_largest_change > 0 else df_primary['Cena'].iloc[0]
            price_diff_percentage = (price_diff / previous_price) * 100 if previous_price != 0 else 0

            # Check for price trends (e.g., increasing, decreasing, fluctuating)
            price_trend = np.polyfit(mdates.date2num(df_primary['Data zestawienia']), df_primary['Cena'], 1)[0]

            # Determine if the price is stable (low standard deviation) or fluctuating
            if price_std < 1:
                price_stability_desc = "The price has remained quite stable over time, suggesting steady demand and pricing policies."
            elif price_trend > 0:
                price_stability_desc = "The price has been increasing over time, which could indicate rising demand or increasing operational costs."
            elif price_trend < 0:
                price_stability_desc = "The price has been decreasing, possibly due to seasonal promotions or reduced demand."
            else:
                price_stability_desc = "The price has fluctuated significantly, possibly due to seasonal promotions or changes in market conditions."

            # Frequency of price changes
            change_frequency = df_primary['Cena'].diff().ne(0).sum()
            if change_frequency > 5:
                change_freq_desc = f"There have been frequent price adjustments, with {change_frequency} changes recorded over the observed period."
            else:
                change_freq_desc = f"There have been only {change_frequency} major price changes during the observed period, indicating long periods of price stability."

            # Magnitude of largest price change
            largest_change_desc = f"The most significant price change was {self.currency}{price_diff:.2f} ({price_diff_percentage:.2f}%) on {price_change_date}, which may reflect a major market shift or promotional event."

            # Calculate the cumulative price change over the period
            cumulative_price_change = df_primary['Cena'].iloc[-1] - df_primary['Cena'].iloc[0]
            cumulative_percentage_change = (cumulative_price_change / df_primary['Cena'].iloc[0]) * 100 if df_primary['Cena'].iloc[0] != 0 else 0
            if cumulative_price_change > 0:
                cumulative_change_desc = f"Over the entire period, there was a cumulative price increase of {self.currency}{cumulative_price_change:.2f} ({cumulative_percentage_change:.2f}%)."
            elif cumulative_price_change < 0:
                cumulative_change_desc = f"Over the entire period, there was a cumulative price decrease of {self.currency}{abs(cumulative_price_change):.2f} ({abs(cumulative_percentage_change):.2f}%)."
            else:
                cumulative_change_desc = "The price remained unchanged over the entire period."

            # Identify periods of stability
            stable_periods = (df_primary['Cena'].diff() == 0).sum()
            if stable_periods > 0:
                stability_duration_desc = f"The price remained stable for {stable_periods} days throughout the observed period."
            else:
                stability_duration_desc = "There were no long periods of stability, indicating frequent price changes."

            # Check for seasonal price patterns (if the data covers multiple seasons)
            df_primary['month'] = df_primary['Data zestawienia'].dt.month
            summer_prices = df_primary[df_primary['month'].isin([6, 7, 8])]['Cena']
            winter_prices = df_primary[df_primary['month'].isin([12, 1, 2])]['Cena']
            
            if not summer_prices.empty and not winter_prices.empty:
                if summer_prices.mean() > winter_prices.mean():
                    seasonality_desc = "Prices were generally higher during the summer months, indicating increased demand."
                elif summer_prices.mean() < winter_prices.mean():
                    seasonality_desc = "Prices were lower during the summer months, potentially reflecting off-season discounts."
                else:
                    seasonality_desc = "Prices remained consistent across both summer and winter seasons."
            else:
                seasonality_desc = "No significant seasonal price patterns were observed."

            # Identify months with highest and lowest average prices
            df_primary['month_year'] = df_primary['Data zestawienia'].dt.to_period('M')
            avg_price_per_month = df_primary.groupby('month_year')['Cena'].mean()
            month_with_highest_price = avg_price_per_month.idxmax()
            highest_avg_price = avg_price_per_month.max()
            month_with_lowest_price = avg_price_per_month.idxmin()
            lowest_avg_price = avg_price_per_month.min()

            # Include price trend description
            if price_trend > 0:
                trend_desc = "Trend analysis indicates that prices have been increasing over the period, suggesting underlying market conditions are driving prices up."
            elif price_trend < 0:
                trend_desc = "Trend analysis indicates that prices have been decreasing over the period, suggesting underlying market conditions are causing prices to fall."
            else:
                trend_desc = "Trend analysis indicates that prices have remained stable over the period, suggesting steady market conditions."

            # Determine price volatility description
            volatility_desc = f"Price volatility, measured by a standard deviation of {self.currency}{price_std:.2f}, indicates {'low' if price_std < 1 else 'high'} variability in pricing."

            # Generate dynamic description for 'Price Over Time' chart
            price_over_time_desc = (
                f"This chart illustrates how the price of the tour has evolved over time in the primary category, covering the period from {first_price_date} to {last_price_date}. "
                f"Throughout this timeframe, the price fluctuated between a low of {self.currency}{price_min:.2f} and a high of {self.currency}{price_max:.2f}, averaging {self.currency}{price_mean:.2f}. "
                f"{largest_change_desc} {cumulative_change_desc} "
                f"{trend_desc} "
                f"{volatility_desc} "
                f"An analysis of monthly averages reveals that the highest average price occurred in {month_with_highest_price.strftime('%B %Y')}, reaching {self.currency}{highest_avg_price:.2f}, while the lowest was in {month_with_lowest_price.strftime('%B %Y')}, at {self.currency}{lowest_avg_price:.2f}. "
                f"This suggests potential seasonal trends or market dynamics influencing pricing. "
                f"{price_stability_desc} {change_freq_desc} {stability_duration_desc} {seasonality_desc}"
            )

            return price_over_time_desc

    def generate_dynamic_explanations_reviews_over_time(self, df_primary):
        # Total reviews at the end of the period
        total_reviews = df_primary['IloscOpini'].iloc[-1]
        
        # Calculate the highest daily review increase and the date it occurred
        df_primary['Daily_Increase'] = df_primary['IloscOpini'].diff()
        highest_daily_increase = df_primary['Daily_Increase'].max()
        date_highest_increase = df_primary.loc[df_primary['Daily_Increase'].idxmax(), 'Data zestawienia'].date()

        # Determine the total days in the data range
        total_days = (df_primary['Data zestawienia'].max() - df_primary['Data zestawienia'].min()).days + 1

        if total_days < 40:
            # Key insights for short-term data
            reviews_over_time_desc = (
                f"This chart shows the trend in the number of reviews over a short period of <strong>{total_days} days</strong>, "
                f"highlighting recent customer engagement.\n\n"
                f"By the end of this period, the total number of reviews reached <strong>{int(total_reviews)}</strong>.\n"
                f"The highest daily increase in reviews was <strong>{int(highest_daily_increase)}</strong> on "
                f"<strong>{date_highest_increase}</strong>.\n"
            )
        else:
            # Group by month and calculate total reviews per month
            df_primary['Month'] = df_primary['Data zestawienia'].dt.to_period('M')
            monthly_reviews = df_primary.groupby('Month')['IloscOpini'].max() - df_primary.groupby('Month')['IloscOpini'].min()
            
            # Calculate days in each month
            days_in_month = df_primary['Data zestawienia'].dt.to_period('M').drop_duplicates().dt.days_in_month

            # Match the days to the monthly_reviews index
            monthly_avg_reviews = monthly_reviews / days_in_month.values
            month_highest_avg_reviews = monthly_avg_reviews.idxmax().strftime('%Y-%m')  # Convert to formatted string
            highest_avg_reviews = monthly_avg_reviews.max()
            
            # Identify periods of minimal change (stability in reviews)
            review_changes = df_primary['IloscOpini'].diff()
            stable_days = (review_changes.abs() <= 1).sum()

            # Create the dynamic explanation
            reviews_over_time_desc = (
                f"This chart illustrates the trend in the number of reviews over time, providing insights into customer engagement.\n\n"
                f"By the end of the period, the total number of reviews reached <strong>{int(total_reviews)}</strong>.\n"
                f"The highest daily increase in reviews was <strong>{int(highest_daily_increase)}</strong> on "
                f"<strong>{date_highest_increase}</strong>.\n"
                f"The month with the highest average daily reviews was <strong>{month_highest_avg_reviews}</strong>, "
                f"with an average of <strong>{highest_avg_reviews:.2f}</strong> reviews per day.\n"
                f"There were <strong>{stable_days}</strong> days with minimal changes in reviews (less than or equal to 1 review difference)."
            )
        
        return reviews_over_time_desc



    def clean_data(self, df, df_categories):
        """
        Cleans and preprocesses the data.
        Ensures deduplication based on 'Kategoria', 'Tytul Url', and 'Data zestawienia' to avoid duplicates.
        """
        # Handle 'Cena' (Price): Remove currency symbols and convert to float
        df['Cena'] = df['Cena'].replace({'€': '', '$': '', '£': '', ',': '', ' ': ''}, regex=True)
        df['Cena'] = pd.to_numeric(df['Cena'], errors='coerce')
        df = df.dropna(subset=['Cena'])
        # Check if the 'Przecena' column exists
        if 'Przecena' in df.columns:
            # Replace unwanted characters in 'Przecena' if the column is not empty
            if not df['Przecena'].empty:
                df['Przecena'] = df['Przecena'].fillna("").replace({'€': '', '$': '', '£': '', ',': '', ' ': ''}, regex=True)
                df['Przecena'] = pd.to_numeric(df['Przecena'], errors='coerce')

            else:
                logging.warning("'Przecena' column is empty.")

        # Clean Viator extreme prices
        # Use IQR to remove systematic outliers in 'Cena'

        if 'Tytul Url' in df.columns and df['Tytul Url'].str.contains("viator", na=False, case=False).any():
            logging.info("Processing data for Viator...")           
            Q1 = df['Cena'].quantile(0.25)
            Q3 = df['Cena'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.75 * IQR
            upper_bound = Q3 + 1.75 * IQR
            # Filter rows within the acceptable range
            df = df[(df['Cena'] >= lower_bound) & (df['Cena'] <= upper_bound)]
        else:
            combined = pd.concat([
                df[['Cena']].rename(columns={'Cena': 'Value'}),
                df[['Przecena']].rename(columns={'Przecena': 'Value'})
            ]).dropna()

            # Calculate quantiles based on the combined dataset
            Q1 = combined['Value'].quantile(0.25)
            Q3 = combined['Value'].quantile(0.75)
            IQR = Q3 - Q1
            # Define bounds
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            # Filter rows within the acceptable range
            df = df[(df['Cena'] >= lower_bound) & (df['Cena'] <= upper_bound)]

        df['IloscOpini'] = df['IloscOpini'].replace(',', '')

        # Handle 'IloscOpini' (Number of Reviews): Extract numeric value
        def extract_reviews(x):
            if pd.isna(x):
                return None

            # Convert input to string
            s = str(x).strip()

            # Remove thousand separators like commas
            s = s.replace(",", "")

            # Attempt float conversion
            try:
                # Convert to float first, then to int
                f = float(s)
                return int(f)
            except ValueError:
                # If it can't be converted to a float, return None
                return None
        df['IloscOpini'] = df['IloscOpini'].apply(extract_reviews)

        # Handle 'Booked': Extract numeric value from strings like 'Booked X number on Day'
        def extract_booked(x):
            if pd.isna(x):
                return None
            match = re.search(r'Booked\s+(\d+)', str(x), re.IGNORECASE)
            if match:
                return int(match.group(1))
            else:
                try:
                    return int(x)
                except:
                    return None

        df['Booked'] = df['Booked'].apply(extract_booked)

        # Ensure 'Kategoria' column exists and process it for matching purposes
        if 'Kategoria' in df.columns:
            # Convert 'Kategoria' to lowercase and strip whitespace, handling any missing values
            df['Kategoria'] = df['Kategoria'].fillna('unknown').astype(str).str.strip().str.lower()
        else:
            df['Kategoria'] = 'unknown'

        try:
            # Convert 'RawCategory' in df_categories to lowercase for case-insensitive matching
            df_categories['RawCategory'] = df_categories['RawCategory'].astype(str).str.lower()
            
            # Join df with df_categories based on 'Kategoria' and 'RawCategory'
            df = df.join(df_categories.set_index('RawCategory'), on='Kategoria')

            # Drop the original 'Kategoria' column from df and rename 'Category' to 'Kategoria'
            df = df.drop(columns=['Kategoria'], errors='ignore')
            df = df.rename(columns={'Category': 'Kategoria'})

            # Drop the 'RawCategory' column if it exists after the join
            df = df.drop(columns=['RawCategory', 'Category'], errors='ignore')
            
        except Exception as e:
            logging.error("An error occurred during the join process:", exc_info=True)

        # Handle 'Pozycja' column: Convert to numeric, handle missing values
        if 'Pozycja' in df.columns:
            df['Pozycja'] = pd.to_numeric(df['Pozycja'], errors='coerce')
        else:
            df['Pozycja'] = None
        
        # Create a flag column to mark discounted records
        df['IsDiscount'] = 0
        df['Cena_Original'] = df['Cena']

        # Wherever 'Przecena' is not null (and not empty), replace 'Cena' with 'Przecena' and set the discount flag
        mask_przecena = df['Przecena'].notna() & (df['Przecena'] != "")
        df.loc[mask_przecena, 'Cena'] = df.loc[mask_przecena, 'Przecena']
        df.loc[mask_przecena, 'IsDiscount'] = 1

        # Convert 'Data zestawienia' to datetime
        df['Data zestawienia'] = pd.to_datetime(df['Data zestawienia'], errors='coerce')

        # Drop rows with invalid dates or prices
        df = df.dropna(subset=['Data zestawienia', 'Cena'])

        # Sort by date
        df = df.sort_values('Data zestawienia')

        # Deduplicate based on 'Kategoria', 'Tytul Url', and 'Data zestawienia'
        df = df.drop_duplicates(subset=['Kategoria', 'Tytul Url', 'Data zestawienia'])

        return df
    # ------------------------- Analysis Functions ----------------------------

    def analyze_data(self, df):
        """
        Performs analysis on the DataFrame and returns summary statistics, plots, and chart explanations.
        """

        total_days_collected = (df['Data zestawienia'].max() - df['Data zestawienia'].min()).days

        limited_data = total_days_collected < 40 

        discounted_days = df.groupby('Data zestawienia')['IsDiscount'].max().sum()

        summary = {
            'Total Records': len(df),
            'Date Range': f"{df['Data zestawienia'].min().date()} to {df['Data zestawienia'].max().date()}",
            'Average Price': df['Cena'].mean(),
            'Median Price': df['Cena'].median(),
            'Average Number of Reviews': df['IloscOpini'].mean(),
            'Total Reviews': df['IloscOpini'].max(),  # Changed to sum for total reviews
            'Discounted Days': discounted_days
        }

        logging.info("\nSummary Statistics:")
        for key, value in summary.items():
            if isinstance(value, float):
                logging.info(f"{key}: {value:.2f}")
            else:
                logging.info(f"{key}: {value}")

        plots = {}

        # Determine the primary category
        if 'global' in df['Kategoria'].unique():
            primary_category = 'global'
        else:
            primary_category = df['Kategoria'].value_counts().idxmax()
        logging.info(f"\nPrimary Category for Specific Charts: {primary_category}")

        # Filter data for the primary category using .loc to avoid SettingWithCopyWarning
        df_primary = df.loc[df['Kategoria'] == primary_category].copy()
        df_primary['Review_Increase'] = df_primary['IloscOpini'].diff()

        # Find the maximum daily review increase and the corresponding date
        if not df_primary['Review_Increase'].isnull().all():
            highest_daily_review_increase = df_primary['Review_Increase'].max()
            date_highest_daily_increase = df_primary.loc[df_primary['Review_Increase'].idxmax(), 'Data zestawienia'].date()
        else:
            highest_daily_review_increase = None
            date_highest_daily_increase = None

        df_primary = df_primary.drop(columns=['Review_Increase'])
        # Ensure 'Data zestawienia' is datetime
        if df_primary['Data zestawienia'].dtype != 'datetime64[ns]':
            df_primary['Data zestawienia'] = pd.to_datetime(df_primary['Data zestawienia'], errors='coerce')

        # Drop rows with invalid dates
        df_primary = df_primary.dropna(subset=['Data zestawienia'])

        min_reviews = df_primary['IloscOpini'].min()
        max_reviews = df_primary['IloscOpini'].max()
        days_collected = (df_primary['Data zestawienia'].max() - df_primary['Data zestawienia'].min()).days

        # Calculate average review increase per day
        if days_collected > 0:
            average_review_increase_per_day = (max_reviews - min_reviews) / days_collected
        else:
            average_review_increase_per_day = None 

        # Plot Price Over Time for Primary Category
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df_primary, x='Data zestawienia', y='Cena', marker='o', color='#00AEEF')  # PRIMARY_BLUE
        plt.title('Price Over Time', color='#0073B1')  # DARK_BLUE
        plt.xlabel('Date')
        plt.ylabel(f'Price ({self.currency})')
        if limited_data:
            plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show every day
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        else:
            plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))  # Show every month
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        price_over_time_img = BytesIO()
        plt.savefig(price_over_time_img, format='PNG')
        plt.close()
        price_over_time_img.seek(0)
        plots['Price Over Time'] = Image.open(price_over_time_img)

        price_over_time_desc = self.generate_dynamic_explanations_price_over_time(df_primary)
        reviews_over_time_desc = self.generate_dynamic_explanations_reviews_over_time(df_primary)


        # Plot Discounted Price 

        df_discounted = df[df['IsDiscount'] == 1].copy()
        if not df_discounted.empty:
            # Calculate the discount difference
            df_discounted['Discount_Amount'] = df_discounted['Cena_Original'] - df_discounted['Cena']

            # Create the figure and axes for the line chart
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.lineplot(
                data=df_discounted,
                x='Data zestawienia',
                y='Discount_Amount',
                marker='o',
                color='purple',
                ax=ax
            )

            # Chart styling
            ax.set_title('Discount Amount Over Time')
            ax.set_xlabel('Date')
            ax.set_ylabel(f'Discount Amount ({self.currency})')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()

            # Save the plot as an image and store in 'plots'
            discount_plot_img = BytesIO()
            plt.savefig(discount_plot_img, format='PNG')
            plt.close(fig)
            discount_plot_img.seek(0)
            plots['Discount Amount Over Time'] = Image.open(discount_plot_img)

            # Compute summary stats about the discounts
            avg_discount = df_discounted['Discount_Amount'].mean()
            max_discount = df_discounted['Discount_Amount'].max()
            day_max_discount = df_discounted.loc[df_discounted['Discount_Amount'].idxmax(), 'Data zestawienia'].date()

            # Store them in 'summary'
            summary['Average Discount'] = avg_discount
            summary['Max Discount'] = max_discount
            summary['Date Max Discount'] = day_max_discount

        # Calculate Average Number of Reviews per Day
        reviews_daily_primary = df_primary.groupby(pd.Grouper(key='Data zestawienia', freq='D'))['IloscOpini'].mean().reset_index()
        reviews_daily_primary.rename(columns={'IloscOpini': 'Average Reviews'}, inplace=True)
        reviews_daily_primary['Data zestawienia'] = pd.to_datetime(reviews_daily_primary['Data zestawienia'])

        # Plot Number of Reviews Over Time for Primary Category
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=reviews_daily_primary, x='Data zestawienia', y='Average Reviews', marker='o', color='orange')
        plt.title('Number of Reviews Over Time')
        plt.xlabel('Date')
        plt.ylabel('Number of Reviews')

        # Improve date formatting on X-axis
        ax = plt.gca()
        if limited_data:
            plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show every day
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        else:
            plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))  # Show every month
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.xticks(rotation=45, ha='right')

        plt.tight_layout()
        reviews_over_time_img = BytesIO()
        plt.savefig(reviews_over_time_img, format='PNG')
        plt.close()
        reviews_over_time_img.seek(0)
        plots['Number of Reviews Over Time'] = Image.open(reviews_over_time_img)

        # Check if 'Booked' column has data
        if df_primary['Booked'].notna().any():
            total_days_in_period = (df_primary['Data zestawienia'].max() - df_primary['Data zestawienia'].min()).days + 1
            # Handle Booked Data Analysis
            booked_summary = {
                'Total Bookings': df_primary['Booked'].sum(),
                'Average Bookings per Day': round(df_primary['Booked'].sum() / total_days_in_period, 2),  
                'Max Bookings in a Day': df_primary['Booked'].max(),
                'Date with Max Bookings': df_primary.loc[df_primary['Booked'].idxmax(), 'Data zestawienia'].date()
            }

            logging.info("\nBooked Data Statistics:")
            for key, value in booked_summary.items():
                if isinstance(value, float):
                    logging.info(f"{key}: {value:.2f}")
                else:
                    logging.info(f"{key}: {value}")

            # Calculate Average Number of Bookings per Day
            bookings_daily_primary = df_primary.groupby(pd.Grouper(key='Data zestawienia', freq='D'))['Booked'].mean().reset_index()
            bookings_daily_primary.rename(columns={'Booked': 'Average Bookings'}, inplace=True)
            bookings_daily_primary['Data zestawienia'] = pd.to_datetime(bookings_daily_primary['Data zestawienia'])

            # Plot Number of Bookings Over Time
            plt.figure(figsize=(12, 6))
            sns.lineplot(data=bookings_daily_primary, x='Data zestawienia', y='Average Bookings', color='green')
            plt.title('Number of Bookings Over Time')
            plt.xlabel('Date')
            plt.ylabel('Number of Bookings')

            # Improve date formatting on X-axis
            ax = plt.gca()
            if limited_data:
                plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show every day
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            else:
                plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))  # Show every month
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.xticks(rotation=45, ha='right')

            plt.tight_layout()
            bookings_over_time_img = BytesIO()
            plt.savefig(bookings_over_time_img, format='PNG')
            plt.close()
            bookings_over_time_img.seek(0)
            plots['Number of Bookings Over Time'] = Image.open(bookings_over_time_img)
        else:
            booked_summary = None

        df_primary.set_index('Data zestawienia', inplace=True)
      # Analyze Reviews Increase per Month based on Primary Category
        if not limited_data:
                # Resample and calculate monthly reviews
            
            reviews_monthly_primary = df_primary['IloscOpini'].resample('ME').sum().reset_index()
            reviews_monthly_primary.rename(columns={'IloscOpini': 'Total Reviews'}, inplace=True)
            # Custom percentage change calculation to handle zero division
            reviews_monthly_primary['Review_Increase'] = reviews_monthly_primary['Total Reviews'].diff() / reviews_monthly_primary['Total Reviews'].shift(1)
            reviews_monthly_primary['Review_Increase'] = reviews_monthly_primary['Review_Increase'].fillna(0).replace([float('inf'), -float('inf')], None) * 100


            # Custom percentage change calculation to handle zero division
            reviews_monthly_primary['Review_Increase'] = (
                (reviews_monthly_primary['Total Reviews'].diff()) / reviews_monthly_primary['Total Reviews'].shift(1)
            )
            reviews_monthly_primary['Review_Increase'] = (
                reviews_monthly_primary['Review_Increase']
                .replace([float('inf'), -float('inf')], None)  # Replace inf values with None
                .fillna(0)  # Replace NaN with 0
                * 100  # Convert to percentage
            )

            # Insights for Reviews MoM
            average_mom_review_increase = reviews_monthly_primary['Review_Increase'].mean()
            highest_mom_review_increase = reviews_monthly_primary['Review_Increase'].max()

            if not reviews_monthly_primary['Review_Increase'].isnull().all():
                month_highest_mom_review_increase = reviews_monthly_primary.loc[reviews_monthly_primary['Review_Increase'].idxmax(), 'Data zestawienia'].date()
            else:
                highest_mom_review_increase = None
                month_highest_mom_review_increase = None
        else:
            average_mom_review_increase = None
            highest_mom_review_increase = None
            month_highest_mom_review_increase = None


        # Package MoM Insights into review_stats
        review_stats = {
            'Average Review Increase per Day': round(average_review_increase_per_day, 2),
            'MoM Average Review Increase (%)': average_mom_review_increase,
            'MoM Highest Review Increase (%)': highest_mom_review_increase,
            'Month with Highest MoM Review Increase': month_highest_mom_review_increase,
            'Highest Daily Review Increase': highest_daily_review_increase,
            'Date of Highest Daily Review Increase': date_highest_daily_increase
        }

        df_primary = df_primary.drop(columns=['Daily_Increase'])
        if not limited_data:
            df_primary = df_primary.drop(columns=['Month'])
        # Analyze Bookings MoM if booked_summary exists
        if booked_summary:
            # Resample and calculate monthly bookings
            bookings_monthly_primary = df_primary.resample('M').sum().reset_index()
            bookings_monthly_primary.rename(columns={'Booked': 'Total Bookings'}, inplace=True)

            # Custom percentage change calculation to handle zero division
            bookings_monthly_primary['Booking_Increase'] = (
                (bookings_monthly_primary['Total Bookings'].diff()) / bookings_monthly_primary['Total Bookings'].shift(1)
            )
            bookings_monthly_primary['Booking_Increase'] = (
                bookings_monthly_primary['Booking_Increase']
                .replace([float('inf'), -float('inf')], None)  # Replace inf values with None
                .fillna(0)  # Replace NaN with 0
                * 100  # Convert to percentage
            )

            # Insights for Bookings MoM
            if not limited_data:
                average_mom_booking_increase = bookings_monthly_primary['Booking_Increase'].mean()
                highest_mom_booking_increase = bookings_monthly_primary['Booking_Increase'].max()
                
                if not bookings_monthly_primary['Booking_Increase'].isnull().all():
                    month_highest_mom_booking_increase = bookings_monthly_primary.loc[
                        bookings_monthly_primary['Booking_Increase'].idxmax(), 'Data zestawienia'
                    ].date()
                else:
                    highest_mom_booking_increase = None
                    month_highest_mom_booking_increase = None
            else:
                highest_mom_booking_increase = None
                month_highest_mom_booking_increase = None

            # Add booking insights to booked_summary only if MoM increase is positive
            if highest_mom_booking_increase is not None and highest_mom_booking_increase > 0:
                booked_summary.update({
                    'MoM Average Booking Increase (%)': average_mom_booking_increase,
                    'MoM Highest Booking Increase (%)': highest_mom_booking_increase,
                    'Month with Highest MoM Booking Increase': month_highest_mom_booking_increase
                })
            else:
                # Add default values to avoid KeyError
                booked_summary.update({
                    'MoM Average Booking Increase (%)': None,
                    'MoM Highest Booking Increase (%)': None,
                    'Month with Highest MoM Booking Increase': None
                })
        else:
            average_mom_booking_increase = None
            highest_mom_booking_increase = None
            month_highest_mom_booking_increase = None



        # Additional Insights: Price Distribution
        bin_count = min(30, len(df_primary['Cena'].unique()))
        if bin_count > 1:
            plt.figure(figsize=(10, 6))
            sns.histplot(df_primary['Cena'], kde=True, bins=bin_count, color='blue')
            plt.title('Price Distribution')
            plt.xlabel(f'Price ({self.currency})')
            plt.ylabel('Frequency')
            plt.tight_layout()
            price_distribution_img = BytesIO()
            plt.savefig(price_distribution_img, format='PNG')
            plt.close()
            price_distribution_img.seek(0)
            plots['Price Distribution'] = Image.open(price_distribution_img)

        # Additional Insights: Correlation between Price and Number of Reviews
        plt.figure(figsize=(10, 6))
        try:
            sns.scatterplot(data=df_primary, x='Cena', y='IloscOpini', alpha=0.6, color='red')
        except:
            logging.warning(f"There was error for default type for Price Distribution: Reoslution Keep the First Occurrence of Each Index ")
            df_primary = df_primary[~df_primary.index.duplicated(keep='first')]
            sns.scatterplot(data=df_primary, x='Cena', y='IloscOpini', alpha=0.6, color='red')

        plt.title('Price vs. Reviews Correlation')
        plt.xlabel(f'Price ({self.currency})')
        plt.ylabel('Number of Reviews')
        plt.tight_layout()
        price_reviews_correlation_img = BytesIO()
        plt.savefig(price_reviews_correlation_img, format='PNG')
        plt.close()
        price_reviews_correlation_img.seek(0)
        plots['Price vs. Reviews Correlation'] = Image.open(price_reviews_correlation_img)

        if not limited_data:
            # Additional Insights: Moving Average of Reviews
            df_primary_sorted = df_primary.sort_index()
            df_primary_sorted['Reviews_MA_3'] = df_primary_sorted['IloscOpini'].rolling(window=3).mean()

            plt.figure(figsize=(12, 6))
            sns.lineplot(data=df_primary_sorted, x=df_primary_sorted.index, y='IloscOpini', marker='o', label='Number of Reviews')
            sns.lineplot(data=df_primary_sorted, x=df_primary_sorted.index, y='Reviews_MA_3', marker='x', label='3-Month Moving Average', color='red')
            plt.title('Number of Reviews with 3-Month Moving Average')
            plt.xlabel('Date')
            plt.ylabel('Number of Reviews')
            plt.legend()
            plt.tight_layout()
            reviews_moving_average_img = BytesIO()
            plt.savefig(reviews_moving_average_img, format='PNG')
            plt.close()
            reviews_moving_average_img.seek(0)
            plots['Reviews Moving Average'] = Image.open(reviews_moving_average_img)

        # Additional Analysis: Category Distribution using Matplotlib's pie
        plt.figure(figsize=(8, 8))
        category_counts = df['Kategoria'].value_counts()
        plt.pie(category_counts.values, labels=category_counts.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
        plt.title('Category Distribution')
        plt.tight_layout()
        category_distribution_img = BytesIO()
        plt.savefig(category_distribution_img, format='PNG')
        plt.close()
        category_distribution_img.seek(0)
        plots['Category Distribution'] = Image.open(category_distribution_img)

        # Additional Analysis: Position by Category
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=df, x='Kategoria', y='Pozycja')
        plt.title('Position by Category')
        plt.xlabel('Category')
        plt.xticks(rotation=45)
        plt.ylabel('Position')
        plt.tight_layout()
        position_category_img = BytesIO()
        plt.savefig(position_category_img, format='PNG')
        plt.close()
        position_category_img.seek(0)
        plots['Position by Category'] = Image.open(position_category_img)

        # Calculate Category Counts
        category_counts_dict = category_counts.to_dict()

        # Calculate Position Statistics per Category
        position_stats = df.groupby('Kategoria')['Pozycja'].agg(['mean', 'median', 'min', 'max']).reset_index()

        # Chart explanations
        chart_explanations = {
            'Price Over Time': f'{price_over_time_desc}',
            'Number of Reviews Over Time': f'{reviews_over_time_desc}',
            'Number of Bookings Over Time': 'This chart shows the trend in the number of bookings per day over time for the primary category, indicating customer purchasing behavior.',
            'Price Distribution': 'This chart shows the distribution of prices for the tour within the primary category, indicating the most common price points.',
            'Price vs. Reviews Correlation': 'This chart explores the correlation between price and the number of reviews for the primary category, suggesting how pricing may affect customer engagement.',
            'Reviews Moving Average': 'This chart shows the moving average of reviews over time for the primary category, smoothing out short-term fluctuations to reveal longer-term trends.',
            'Category Distribution': 'This pie chart displays the distribution of different categories, providing insight into the variety and prevalence of each category within the dataset.',
            'Position by Category': 'This box plot illustrates the distribution of positions within each category, highlighting any correlations or differences between categories.',
            'Discount Amount Over Time': "This chart shows the amount of discount on the original price.",
            'Original vs. Discounted Price': "This bar chart compares the original (blue) and discounted (green) prices for each date where a discount was applied. A noticeable gap indicates a larger discount."
        }

        title = df['Tytul'].iloc[0] if not df['Tytul'].isnull().all() else "No Title Available"
        title_url = df['Tytul Url'].iloc[0] if 'Tytul Url' in df.columns and not df['Tytul Url'].isnull().all() else None

        # Create title as HREF
        if title_url:
            title_href = f'<a href="{title_url}" target="_blank">{title}</a>'
        else:
            title_href = title
        # Populate the overview attribute
        self.overview = [
            f"Title: {title_href}",
            f"Total records analyzed: {len(df)}",
            f"Date range: {df['Data zestawienia'].min().strftime('%Y-%m-%d')} to {df['Data zestawienia'].max().strftime('%Y-%m-%d')}",
            f"Average price: {self.currency}{summary['Average Price']:.2f}",
            f"Highest price: {self.currency}{df['Cena'].max():.2f} on {df.loc[df['Cena'].idxmax(), 'Data zestawienia'].strftime('%Y-%m-%d')}",
            f"Number of reviews: {int(summary['Total Reviews'])}",
        ]
        if booked_summary:
            self.overview.append(f"Total bookings: {booked_summary['Total Bookings']}")

        self.overview.append(f"OTA: {self.ota}")            
        
        return summary, reviews_daily_primary, review_stats, booked_summary, plots, chart_explanations, category_counts_dict, position_stats


    def generate_insight_summary(self, summary, daily_reviews, review_stats, booked_summary=None, category_counts=None, position_stats=None):
        """
        Generates an HTML-formatted textual summary of the analysis, including category and position-based insights.
        """
        # Start of the Historical Summary with Main Findings
        insight = (
            f"<h3>Main Findings:</h3>\n"
            f"<ul>\n"
            f"  <li><strong>Total Records Analyzed:</strong> {summary['Total Records']}</li>\n"
            f"  <li><strong>Date Range:</strong> {summary['Date Range']}</li>\n"
            f"  <li><strong>Average Price:</strong> {self.currency}{summary['Average Price']:.2f}</li>\n"
            f"  <li><strong>Median Price:</strong> {self.currency}{summary['Median Price']:.2f}</li>\n"
            f"  <li><strong>Average Number of Reviews:</strong> {summary['Average Number of Reviews']:.2f}</li>\n"
            f"  <li><strong>Number of Reviews:</strong> {int(summary['Total Reviews'])}</li>\n"
            
        )

        discount_days = summary.get('Discounted Days', 0)
        if discount_days > 0:
            insight += (
                f"<li><strong>Discounted Days:</strong> There were {discount_days} days with a discounted price in "
                f"the selected time range.</li>\n"
            )

        insight += (
            f"</ul>\n\n"
        )

        # Historical Summary Table
        insight += (
            f"<table>\n"
            f"  <tr>\n"
            f"    <th>Metric</th>\n"
            f"    <th>Value</th>\n"
            f"  </tr>\n"
            f"  <tr>\n"
            f"    <td><strong>Total Records Analyzed</strong></td>\n"
            f"    <td>{summary['Total Records']}</td>\n"
            f"  </tr>\n"
            f"  <tr>\n"
            f"    <td><strong>Date Range</strong></td>\n"
            f"    <td>{summary['Date Range']}</td>\n"
            f"  </tr>\n"
            f"  <tr>\n"
            f"    <td><strong>Average Price</strong></td>\n"
            f"    <td>{self.currency}{summary['Average Price']:.2f}</td>\n"
            f"  </tr>\n"
            f"  <tr>\n"
            f"    <td><strong>Median Price</strong></td>\n"
            f"    <td>{self.currency}{summary['Median Price']:.2f}</td>\n"
            f"  </tr>\n"
            f"  <tr>\n"
            f"    <td><strong>Average Number of Reviews</strong></td>\n"
            f"    <td>{summary['Average Number of Reviews']:.2f}</td>\n"
            f"  </tr>\n"
            f"  <tr>\n"
            f"    <td><strong>Number of Reviews</strong></td>\n"
            f"    <td>{int(summary['Total Reviews'])}</td>\n"
            f"  </tr>\n"
            f"</table>\n\n"
        )

        # Reviews Analysis Section
        insight += (
            f"<h3>Reviews Analysis:</h3>\n"
            f"<table>\n"
            f"  <tr>\n"
            f"    <th>Metric</th>\n"
            f"    <th>Value</th>\n"
            f"  </tr>\n"
            f"  <tr>\n"
            f"    <td><strong>Average Review Increase per Day</strong></td>\n"
            f"    <td>{review_stats['Average Review Increase per Day']}</td>\n"
            f"  </tr>\n"
        )

        if review_stats['Highest Daily Review Increase'] is not None:
            insight += f"  <tr>\n" \
                    f"    <td><strong>Highest Daily Review Increase:</strong></td>\n" \
                    f"    <td>{review_stats['Highest Daily Review Increase']:.2f} on {review_stats['Date of Highest Daily Review Increase']}</td>\n" \
                    f"  </tr>\n"

        if review_stats['MoM Highest Review Increase (%)'] is not None:
            insight += f"  <tr>\n" \
                    f"    <td><strong>Highest MoM Review Increase (%)</strong></td>\n" \
                    f"    <td>{review_stats['MoM Highest Review Increase (%)']:.2f}% on {review_stats['Month with Highest MoM Review Increase']}</td>\n" \
                    f"  </tr>\n"
        else:
            pass
            # insight += f"  <tr>\n" \
            #         f"    <td><strong>Highest MoM Review Increase (%)</strong></td>\n" \
            #         f"    <td>Not enough data to determine.</td>\n" \
            #         f"  </tr>\n"

        insight += f"</table>\n\n"

        # Discount Price Analysis Section
        average_discount = summary.get('Average Discount', 0)
        max_discount = summary.get('Max Discount', 0)
        date_max_discount = summary.get('Date Max Discount', None)

        if discount_days > 0:  # or if 'Average Discount' is in summary
            insight += (
                f"<h3>Discount Insights:</h3>\n"
                f"<table>\n"
                f"  <tr>\n"
                f"    <th>Metric</th>\n"
                f"    <th>Value</th>\n"
                f"  </tr>\n"
                f"  <tr>\n"
                f"    <td><strong>Average Discount</strong></td>\n"
                f"    <td>{self.currency}{average_discount:.2f}</td>\n"
                f"  </tr>\n"
                f"  <tr>\n"
                f"    <td><strong>Max Discount</strong></td>\n"
                f"    <td>{self.currency}{max_discount:.2f}"
                + (f" on {date_max_discount}" if date_max_discount else "")
                + "</td>\n"
                f"  </tr>\n"
                f"</table>\n\n"
            )
        # Booked Data Analysis Section
        if booked_summary:
            insight += (
                f"<h3>Booked Data Analysis:</h3>\n"
                f"<table>\n"
                f"  <tr>\n"
                f"    <th>Metric</th>\n"
                f"    <th>Value</th>\n"
                f"  </tr>\n"
                f"  <tr>\n"
                f"    <td><strong>Total Bookings</strong></td>\n"
                f"    <td>{booked_summary['Total Bookings']}</td>\n"
                f"  </tr>\n"
                f"  <tr>\n"
                f"    <td><strong>Average Bookings per Day</strong></td>\n"
                f"    <td>{booked_summary['Average Bookings per Day']:.2f}</td>\n"
                f"  </tr>\n"
                f"  <tr>\n"
                f"    <td><strong>Maximum Bookings in a Single Day</strong></td>\n"
                f"    <td>{booked_summary['Max Bookings in a Day']} on {booked_summary['Date with Max Bookings']}</td>\n"
                f"  </tr>\n"
            )

            # Add Bookings MoM Insights if available
            if booked_summary['MoM Highest Booking Increase (%)'] is not None:
                insight += f"  <tr>\n" \
                        f"    <td><strong>Highest MoM Booking Increase (%)</strong></td>\n" \
                        f"    <td>{booked_summary['MoM Highest Booking Increase (%)']:.2f}% on {booked_summary['Month with Highest MoM Booking Increase']}</td>\n" \
                        f"  </tr>\n"
            else:
                pass
                # insight += f"  <tr>\n" \
                #         f"    <td><strong>Highest MoM Booking Increase (%)</strong></td>\n" \
                #         f"    <td>Not enough data to determine.</td>\n" \
                #         f"  </tr>\n"

            insight += f"</table>\n\n"

        # Key Insights with Highlighted Metrics and Contextual Interpretation
        insight += (
            f"<h3>Key Insights:</h3>\n"
            f"<ul>\n"
            f"  <li>The tour has an average price of <strong>{self.currency}{summary['Average Price']:.2f}</strong>, with a median price of <strong>{self.currency}{summary['Median Price']:.2f}</strong>.</li>\n"
            f"  <li>There is an average increase of <strong>{review_stats['Average Review Increase per Day']} reviews per day</strong>.</li>\n"
        )

        if review_stats['MoM Highest Review Increase (%)'] is not None:
            insight += (
                f"  <li>The highest MoM review increase was <strong>{review_stats['MoM Highest Review Increase (%)']:.2f}%</strong> in <strong>{review_stats['Month with Highest MoM Review Increase']}</strong>, indicating a significant peak in customer engagement during that period. This could be due to seasonal demand, promotional events, or improvements in service quality.</li>\n"
            )

        if booked_summary:
            insight += (
                f"  <li>A total of <strong>{booked_summary['Total Bookings']} bookings</strong> have been recorded.</li>\n"
                f"  <li>The average number of bookings per day is <strong>{booked_summary['Average Bookings per Day']:.2f}</strong>.</li>\n"
                f"  <li>The highest number of bookings in a single day is <strong>{booked_summary['Max Bookings in a Day']}</strong> on <strong>{booked_summary['Date with Max Bookings']}</strong>.</li>\n"
            )
            if booked_summary['MoM Highest Booking Increase (%)'] is not None:
                insight += (
                    f"  <li>The highest MoM booking increase was <strong>{booked_summary['MoM Highest Booking Increase (%)']:.2f}%</strong> in <strong>{booked_summary['Month with Highest MoM Booking Increase']}</strong>, indicating a significant peak in bookings during that period.</li>\n"
                )

        # Comparative Insights (Month-over-Month Performance)
        if review_stats['MoM Highest Review Increase (%)'] is not None:
            insight += (
                f"  <li><strong>Comparative Performance:</strong> The number of reviews has shown an average MoM increase of <strong>{review_stats['MoM Average Review Increase (%)']:.2f}%</strong>, with the highest increase of <strong>{review_stats['MoM Highest Review Increase (%)']:.2f}%</strong> in <strong>{review_stats['Month with Highest MoM Review Increase']}</strong>.</li>\n"
            )

        if booked_summary and booked_summary['MoM Average Booking Increase (%)'] is not None:
            insight += (
                f"  <li>The number of bookings has shown an average MoM increase of <strong>{booked_summary['MoM Average Booking Increase (%)']:.2f}%</strong>, with the highest increase of <strong>{booked_summary['MoM Highest Booking Increase (%)']:.2f}%</strong> in <strong>{booked_summary['Month with Highest MoM Booking Increase']}</strong>.</li>\n"
            )

        insight += f"</ul>\n\n"

        # Category Insights Section
        if category_counts and len(category_counts) > 1:
            most_common_category = max(category_counts, key=category_counts.get)
            insight += (
                f"<h3>Category Insights:</h3>\n"
                f"<table>\n"
                f"  <tr>\n"
                f"    <th>Metric</th>\n"
                f"    <th>Value</th>\n"
                f"  </tr>\n"
                f"  <tr>\n"
                f"    <td><strong>Total Categories</strong></td>\n"
                f"    <td>{len(category_counts)}</td>\n"
                f"  </tr>\n"
                f"  <tr>\n"
                f"    <td><strong>Most Common Category</strong></td>\n"
                f"    <td>{most_common_category.capitalize()} ({category_counts[most_common_category]} records)</td>\n"
                f"  </tr>\n"
                f"</table>\n\n"
                f"<h4>Category Distribution:</h4>\n"
                f"<table>\n"
                f"  <tr>\n"
                f"    <th>Category</th>\n"
                f"    <th>Records</th>\n"
                f"    <th>Percentage</th>\n"
                f"  </tr>\n"
            )
            for category, count in category_counts.items():
                percentage = (count / summary['Total Records']) * 100
                # Highlighting the most common category
                if category == most_common_category:
                    insight += f"  <tr>\n" \
                            f"    <td><strong>{category.capitalize()}</strong></td>\n" \
                            f"    <td><strong>{count}</strong></td>\n" \
                            f"    <td><strong>{percentage:.1f}%</strong></td>\n" \
                            f"  </tr>\n"
                else:
                    insight += f"  <tr>\n" \
                            f"    <td>{category.capitalize()}</td>\n" \
                            f"    <td>{count}</td>\n" \
                            f"    <td>{percentage:.1f}%</td>\n" \
                            f"  </tr>\n"
            insight += f"</table>\n\n"

        # Position Insights Section
        if position_stats is not None and not position_stats.empty:
            insight += (
                f"<h3>Position Insights:</h3>\n"
                f"<table>\n"
                f"  <tr>\n"
                f"    <th>Category</th>\n"
                f"    <th>Average Position</th>\n"
                f"    <th>Median Position</th>\n"
                f"    <th>Position Range</th>\n"
                f"  </tr>\n"
            )
            for _, row in position_stats.iterrows():
                category = row['Kategoria'].capitalize()
                mean_pos = row['mean']
                median_pos = row['median']
                min_pos = int(row['min']) if not pd.isna(row['min']) else 'N/A'
                max_pos = int(row['max']) if not pd.isna(row['max']) else 'N/A'
                insight += f"  <tr>\n" \
                        f"    <td>{category}</td>\n" \
                        f"    <td>{mean_pos:.2f}</td>\n" \
                        f"    <td>{median_pos:.2f}</td>\n" \
                        f"    <td>{min_pos} to {max_pos}</td>\n" \
                        f"  </tr>\n"
            insight += f"</table>\n"

        return insight



    # ------------------------- HTML Report Generation ------------------------

    def generate_html_report(
        self,
        insight_text: str,
        plots: dict,
        report_title: str,
        url: str,
        chart_explanations: dict = None,
        introduction_text: str = '',
        conclusion_text: str = '',
        logo_base64: str = ''
    ) -> str:
        """
        Generates a polished and responsive HTML report with embedded images and content.
        """
        # The insight_text is already in HTML format from generate_insight_summary
        insight_html = insight_text if insight_text else ''

        # Start building the HTML content
        html = f"""<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{report_title}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            /* Reset CSS */
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}

            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                background-color: #ffffff;
                color: #333333;
                padding: 20px;
                position: relative;
                min-height: 100vh;
                padding-bottom: 80px; /* Increased to accommodate footer */
            }}

            header {{
                text-align: center;
                padding: 20px 0;
                border-bottom: 2px solid #e0e0e0;
                margin-bottom: 40px;
                background-color: #f0f8ff; /* Light blue background */
                border-radius: 8px;
            }}

            header h1 {{
                font-size: 2.5em;
                color: #00AEEF; /* Primary Blue */
                text-decoration: none;
            }}

            header h1 a {{
                color: inherit;
                text-decoration: none;
            }}

            section {{
                margin-bottom: 40px;
                background-color: #f9f9f9; /* Muted background for sections */
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }}

            h2 {{
                font-size: 1.8em;
                color: #0073B1; /* Dark Blue */
                margin-bottom: 15px;
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 5px;
            }}

            h3 {{
                font-size: 1.4em;
                color: #0073B1; /* Dark Blue */
                margin-bottom: 10px;
                text-align: left; /* Align titles to the left */
            }}

            h4 {{
                font-size: 1.2em;
                color: #0073B1; /* Dark Blue */
                margin-bottom: 10px;
                text-align: left;
            }}

            p {{
                margin-bottom: 15px;
                font-size: 1em;
                color: #555555;
            }}

            .charts {{
                display: flex;
                flex-wrap: wrap;
                gap: 40px;
                justify-content: center;
                page-break-inside: avoid;
            }}

            .chart-item {{
                flex: 1 1 45%;
                max-width: 45%;
                page-break-inside: avoid; /* Prevent page breaks within chart items */
            }}

            .chart-item img {{
                width: 100%;
                height: auto;
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 5px;
                background-color: #f9f9f9;
            }}

            .explanation {{
                margin-top: 10px;
                font-size: 0.95em;
                color: #666666;
            }}

            footer {{
                text-align: center;
                padding: 20px;
                background-color: #f0f0f0;
                border-top: 2px solid #e0e0e0;
                position: relative;
                width: 100%;
                box-sizing: border-box;
                margin-top: 40px;
            }}

            .footer-main {{
                display: flex;
                justify-content: center;
                align-items: center;
                flex-wrap: wrap;
                gap: 10px;
            }}

            .footer-main img {{
                height: 30px;
                width: auto;
            }}

            .footer-main span {{
                font-size: 0.9em;
                color: #888888;
            }}

            .footer-date {{
                margin-top: 10px;
                font-size: 0.85em;
                color: #888888;
            }}

            /* Responsive Design */
            @media (max-width: 768px) {{
                .chart-item {{
                    flex: 1 1 100%;
                    max-width: 100%;
                }}
            }}

            /* Table of Contents */
            nav.toc {{
                margin-bottom: 40px;
                padding: 20px;
                background-color: #f4f4f4;
                border-radius: 8px;
            }}

            nav.toc ul {{
                list-style: none;
            }}

            nav.toc li {{
                margin-bottom: 10px;
            }}

            nav.toc a {{
                text-decoration: none;
                color: #2980B9;
            }}

            nav.toc a:hover {{
                text-decoration: underline;
            }}

            /* Style for HTML Lists */
            ul {{
                margin-left: 20px; /* Indent the list */
                margin-bottom: 20px; /* Space below the list */
            }}

            li {{
                margin-bottom: 10px; /* Space between list items */
            }}

            /* Style for Markdown Tables */
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}

            th, td {{
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }}

            th {{
                background-color: #00AEEF; /* Primary Blue */
                color: #ffffff; /* White text for contrast */
                font-weight: bold;
                font-size: 1em;
            }}

            tr:nth-child(even) {{
                background-color: #f2f2f2; /* Light grey for even rows */
            }}

            tr:hover {{
                background-color: #ddd; /* Darker grey on hover */
            }}

            /* Highlight Key Metrics */
            .highlight {{
                background-color: #DFF0D8; /* Light green background */
                font-weight: bold;
            }}

            /* Additional Color Coding for Sections */
            #introduction {{
                background-color: #fffbea; /* Light yellow */
            }}

            #insight-summary {{
                background-color: #e6f7ff; /* Light cyan */
            }}

            #charts-and-analysis {{
                background-color: #f9f9f9; /* Light grey */
            }}

            #conclusion-and-recommendations {{
                background-color: #fef0f0; /* Light red */
            }}

            /* Logo Styling */
            .logo {{
                height: 40px;
                width: auto;
            }}

        </style>
    </head>
    <body>

        <header>
            <img src="https://sapublicresourcesmyotas.blob.core.windows.net/resources/logo_color.png" alt="Logo" class="logo">
            <h1><a href="{url}">{report_title}</a></h1>
        </header>

        <nav class="toc">
            <h2>Table of Contents</h2>
            <ul>
    """
        # Table of Contents
        toc_sections = []
        if introduction_text:
            toc_sections.append("Introduction")
        if insight_html:
            toc_sections.append("Insight Summary")
        if plots:
            toc_sections.append("Charts and Analysis")
        # if conclusion_text:
        #     toc_sections.append("Conclusion and Recommendations")

        if toc_sections:
            for section in toc_sections:
                section_id = section.lower().replace(" ", "-")
                html += f'        <li><a href="#{section_id}">{section}</a></li>\n'
            html += """    </ul>
        </nav>
        """

        # Introduction Section
        if introduction_text:
            html += f"""    <section id="introduction">
            <h2>Introduction</h2>
            <p>{introduction_text}</p>
        </section>
        """

        # Insight Summary Section
        if insight_html:
            html += f"""    <section id="insight-summary">
            <h2>Insight Summary</h2>
            {insight_html}
        </section>
        """

        # Charts and Explanations Section
        if plots:
            html += """    <section id="charts-and-analysis">
            <h2>Charts and Analysis</h2>
            <div class="charts">
        """
            for plot_title, plot_img in plots.items():
                img_base64 = self.image_to_base64(plot_img)
                explanation = chart_explanations.get(plot_title, '') if chart_explanations else ''
                html += f"""        <div class="chart-item">
                <h3>{plot_title}</h3>
                <img src="{img_base64}" alt="{plot_title}">
                <p class="explanation">{explanation}</p>
            </div>
        """
            html += """    </div>
        </section>
        """

        # Conclusion Section
        # if conclusion_text:
        #     html += f"""    <section id="conclusion-and-recommendations">
        #     <h2>Conclusion and Recommendations</h2>
        #     <p>{conclusion_text}</p>
        # </section>
        # """

        # Footer with Updated Structure
        html += f"""<footer>
        <div class="footer-main">
            <img src="https://sapublicresourcesmyotas.blob.core.windows.net/resources/logo_color.png" alt="Logo" class="logo">
            <span>&copy; {datetime.now().year} MyOTAs. All rights reserved.</span>
        </div>
        <div class="footer-date">
            Report Date: {datetime.now().strftime('%B %d, %Y')}
        </div>
    </footer>
    </body>
    </html>
    """

        return html


    # ------------------------- PDF Report Generation -------------------------

    def generate_pdf_report(
        self,
        insight_text,
        plots,
        report_title,
        url,
        output_filename='Historical_Summary_Report.pdf',
        chart_explanations=None,
        introduction_text='',
        conclusion_text='',
        logo_base64=''
    ):
        """
        Generates a PDF report from HTML content using PDFkit.
        """
        # Generate HTML content
        html_content = self.generate_html_report(
            insight_text,
            plots,
            report_title,
            url,
            chart_explanations,
            introduction_text,
            conclusion_text,
            logo_base64
        )

        # Check if wkhtmltopdf exists at the specified path
        if not os.path.exists(self.WKHTMLTOPDF_PATH):
            logging.error(f"wkhtmltopdf not found at {self.WKHTMLTOPDF_PATH}. Please verify the path.")
            raise FileNotFoundError(f"wkhtmltopdf not found at {self.WKHTMLTOPDF_PATH}. Please verify the path.")

        # Configure PDFkit options
        config = pdfkit.configuration(wkhtmltopdf=self.WKHTMLTOPDF_PATH)

        # Define PDF options
        options = {
            'page-size': 'A4',
            'encoding': "UTF-8",
            'enable-local-file-access': None,  # Required for embedding local images
            'quiet': '',
            'footer-right': 'Page [page] of [toPage]',
            'footer-font-size': '9',
            'footer-spacing': '5',
        }

        # Generate PDF
        try:
            pdfkit.from_string(html_content, output_filename, options=options, configuration=config)
            logging.info(f"PDF report generated: {output_filename}")
            self.output_filename = output_filename
        except Exception as e:
            logging.error(f"Error generating PDF: {e}", exc_info=True)
            return None


    # ------------------------- Utility Methods ------------------------------

    def sanitize_filename(self, s):
        """
        Sanitizes the filename by removing invalid characters and limiting its length.
        """
        s = re.sub(r'[<>:"/\\|?*]', '', s)
        s = s.strip().replace(' ', '_')  # Replace spaces with underscores
        return s[:100]  # Limit filename length to 100 characters

    def image_to_base64(self, image: Image.Image) -> str:
        """
        Converts a PIL Image to a base64-encoded string.
        """
        buffered = BytesIO()
        image.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
        return f"data:image/png;base64,{img_str}"

    def load_logo_base64(self) -> str:
        """
        Loads an image from the specified path and returns its base64-encoded string.
        """
        if not os.path.exists(self.logo_path):
            print(os.getcwd())
            logging.error(f"Logo file not found at {self.logo_path}. Please provide a valid logo image.")
            return ''
        with open(self.logo_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
            return encoded_string

    # ----------------------------- Main Execution -----------------------------

    def run_report(self, url, viewer = None, date_filter=None):
        """
        Main method to run the report generation process for a given URL.
        """
        # Connect to the database
        self.connect_to_database()
        if not self.cnxn:
            return

        try:
            # Extract table name from URL
            table_name = self.extract_table_name(url)
            if not table_name:
                table_name = input("Enter the table name manually: ").strip()

            # Check if the table exists
            while not self.check_table_exists(table_name):
                logging.warning(f"Table '{table_name}' not found in database '{self.DATABASE}'.")
                if table_name != self.city and len(self.city) > 1:
                    table_name = self.city
                else:
                    table_name = input("Please enter a valid table name: ").strip()
                if table_name.lower() == 'exit':
                    logging.info("Exiting the program.")
                    self.cnxn.close()
                    return

            # Fetch data
            df = self.fetch_data(table_name, url, date_filter)
            if df is None:
                self.cnxn.close()
                return

            # Load categories DataFrame from file_management
            from file_management.file_path_manager import FilePathManager
            file_manager = FilePathManager('GYG', '')
            df_categories = pd.read_csv(file_manager.get_file_paths()['link_file'])
            df_categories = df_categories[['RawCategory', 'Category']]

            # Clean data
            df = self.clean_data(df, df_categories)
            if df.empty:
                logging.warning("No valid data to analyze after cleaning.")
                self.cnxn.close()
                return

            # Analyze data
            analysis_results = self.analyze_data(df)
            (
                summary,
                reviews_daily_primary,
                review_stats,
                booked_summary,
                plots,
                chart_explanations,
                category_counts_dict,
                position_stats
            ) = analysis_results

            # Generate textual summary
            insight_text = self.generate_insight_summary(
                summary,
                reviews_daily_primary,
                review_stats,
                booked_summary,
                category_counts=category_counts_dict,
                position_stats=position_stats
            )

            # Extract 'Tytul' for the report title
            report_title = df['Tytul'].iloc[0] if not df['Tytul'].isnull().all() else "Historical Summary Report"

            # Generate PDF file name based on 'Tytul'
            random_uuid = str(random.randint(1000, 9999))
            if viewer:
                output_filename = f"PDF_reports/" + viewer + "_" + self.sanitize_filename(report_title) + "_" + random_uuid +'.pdf'
                current_year_month = datetime.now().strftime('%Y-%m')
                self.title_report =  f"{current_year_month}_{self.sanitize_filename(report_title)}"
            else:
                output_filename = "PDF_reports/" + self.sanitize_filename(report_title) + "_" + random_uuid + '.pdf'

            # Prepare introduction text
            introduction_text = (
                f"This report provides a historical analysis of the tour '{report_title}' available at {url}.\n\n"
                "The analysis includes trends in pricing, customer reviews, bookings, categories, and positions over time."
            )

            # Prepare conclusion text
            conclusion_text = (
                "Based on the analysis, the tour has seen significant growth in customer reviews, "
                f"particularly in {reviews_daily_primary['Data zestawienia'].max().date()}, which may indicate increased popularity or successful "
                "marketing efforts during that period."
            )

            # Generate PDF report with title, introduction, insights, charts, and conclusion
            self.generate_pdf_report(
                insight_text,
                plots,
                report_title,
                url,
                output_filename,
                chart_explanations,
                introduction_text,
                conclusion_text
            )

        finally:
            # Close the database connection
            self.cnxn.close()
            logging.info("Database connection closed.")